"""Series identity and a label-based inverted index.

Every distinct label set is one series. ``cpu{host="a"}`` and ``cpu{host="b"}``
are two series that share a metric name but nothing else, so the index maps
both ways:

* name -> ID, for ingest, where the point of the whole structure is to avoid
  re-encoding the same key on every sample;
* label token -> IDs, for queries, which arrive as matchers over labels.

The index is written by ingest threads and read by query threads, so a lock
covers every access -- including reads, because ``dict.setdefault`` is a
read-modify-write and two threads racing on it will mint two IDs for one key.
"""

from __future__ import annotations

import re
import threading
from dataclasses import dataclass, field

__all__ = [
    "LABEL_RE",
    "MATCHER_RE",
    "Matcher",
    "Op",
    "SeriesIndex",
    "encode_name",
    "format_selector",
    "parse_matchers",
    "parse_selector",
]

#: A label name: ASCII letters, digits and underscore, not starting with a digit.
LABEL_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")

#: One selector element: ``name="value"``, ``name!="value"``, or ``a=~"regex"``.
#: `!=` must precede `=` in the alternation or a `!=` matcher is read as `=`.
#: No trailing space after the closing quote: it would make single-label
#: selectors like `mem{host="c"}` match nothing.
MATCHER_RE = re.compile(r'([A-Za-z_][A-Za-z0-9_]*)\s*(=~|!~|!=|=)\s*"((?:[^"\\]|\\.)*)"')

#: Label used for the metric name.
NAME_LABEL = "__name__"


@dataclass(slots=True)
class Matcher:
    """A label matcher from a selector."""

    op: str
    label: str
    value: str
    # Compiled lazily in __post_init__. Has to be a declared field: with
    # slots=True the dataclass has no __dict__, so assigning an undeclared
    # attribute raises AttributeError.
    _re: "re.Pattern[str] | None" = field(default=None, repr=False, compare=False)

    def __post_init__(self) -> None:
        if self.op not in ("=", "!=", "=~", "!~"):
            raise ValueError(f"unknown matcher op {self.op!r}")
        if self.op in ("=~", "!~"):
            try:
                self._re = re.compile(self.value)
            except re.error as exc:
                raise ValueError(f"invalid regex {self.value!r}: {exc}") from exc

    def matches(self, value: str) -> bool:
        if self.op == "=":
            return value == self.value
        if self.op == "!=":
            return value != self.value
        hit = self._re is not None and self._re.fullmatch(value) is not None
        return hit if self.op == "=~" else not hit


def parse_matchers(selector: str) -> tuple[str, list[Matcher]]:
    """Split ``name{a="1",b!="2"}`` into its name and matchers."""
    selector = selector.strip()
    if not selector:
        raise ValueError("empty selector")
    if "{" not in selector:
        if not LABEL_RE.fullmatch(selector):
            raise ValueError(f"invalid metric name {selector!r}")
        return selector, []

    name, _, rest = selector.partition("{")
    name = name.strip()
    if not LABEL_RE.fullmatch(name):
        raise ValueError(f"invalid metric name {name!r}")
    if not rest.rstrip().endswith("}"):
        raise ValueError("unterminated selector")

    matchers: list[Matcher] = []
    seen = 0
    for m in MATCHER_RE.finditer(rest):
        seen += 1
        matchers.append(Matcher(op=m.group(2), label=m.group(1), value=_unescape(m.group(3))))
    if seen == 0:
        raise ValueError(f"no label matchers in {selector!r}")
    return name, matchers


def _unescape(raw: str) -> str:
    out = []
    i = 0
    while i < len(raw):
        if raw[i] == "\\" and i + 1 < len(raw):
            out.append(raw[i + 1])
            i += 2
        else:
            out.append(raw[i])
            i += 1
    return "".join(out)


def encode_name(labels: dict[str, str]) -> str:
    """Canonical text form of a label set.

    Labels are sorted so that key order in the input cannot create two series
    for one logical series.
    """
    if not labels:
        raise ValueError("a series needs at least one label")
    if NAME_LABEL not in labels:
        raise ValueError(f"missing {NAME_LABEL} label")
    for key in labels:
        if key != NAME_LABEL and not LABEL_RE.fullmatch(key):
            raise ValueError(f"invalid label name {key!r}")
    parts = [f'{key}="{labels[key]}"' for key in sorted(labels) if key != NAME_LABEL]
    name = labels[NAME_LABEL]
    if not LABEL_RE.fullmatch(name):
        raise ValueError(f"invalid metric name {name!r}")
    return f"{name}{{{','.join(parts)}}}" if parts else name


def format_selector(name: str, labels: dict[str, str]) -> str:
    """Build a selector string from a metric name and label set."""
    return encode_name({**labels, NAME_LABEL: name})


@dataclass(slots=True)
class _Series:
    id: int
    name: str
    labels: dict[str, str]


class SeriesIndex:
    """Metric name + label set -> stable integer series ID."""

    __slots__ = ("_by_name", "_by_label", "_by_id", "_next_id", "_lock")

    def __init__(self) -> None:
        self._by_name: dict[str, int] = {}
        self._by_label: dict[str, dict[str, set[int]]] = {}
        self._by_id: dict[int, _Series] = {}
        self._next_id = 1
        self._lock = threading.RLock()

    def __len__(self) -> int:
        with self._lock:
            return len(self._by_id)

    def create(self, name: str, labels: dict[str, str] | None = None) -> tuple[int, bool]:
        """Return ``(id, created)`` for a label set, minting one if needed."""
        merged = {**(labels or {}), NAME_LABEL: name}
        key = encode_name(merged)
        with self._lock:
            existing = self._by_name.get(key)
            if existing is not None:
                return existing, False
            series_id = self._next_id
            self._next_id += 1
            self._by_name[key] = series_id
            self._by_id[series_id] = _Series(id=series_id, name=key, labels=merged)
            self._index_label(NAME_LABEL, name, series_id)
            for label, value in labels.items():
                self._index_label(label, value, series_id)
            return series_id, True

    def _index_label(self, label: str, value: str, series_id: int) -> None:
        self._by_label.setdefault(label, {}).setdefault(value, set()).add(series_id)

    def lookup(self, name: str, labels: dict[str, str] | None = None) -> int | None:
        key = encode_name({**(labels or {}), NAME_LABEL: name})
        with self._lock:
            return self._by_name.get(key)

    def register(self, series_id: int, name: str) -> bool:
        """Re-insert a series under an already-assigned ID.

        Used by WAL replay, where the ID came from the log and must not be
        re-minted. Without this, recovered series stay invisible to the
        inverted index even though their blocks are readable by ID.
        """
        with self._lock:
            if series_id in self._by_id:
                return False
            metric, labels = parse_selector(name)
            # Carry __name__ through, exactly as create() does: a series
            # restored from the WAL must be indistinguishable from one interned
            # fresh, or a matcher on the metric name behaves differently
            # depending on whether the server restarted.
            labels = {**labels, NAME_LABEL: metric}
            self._by_name[name] = series_id
            self._by_id[series_id] = _Series(id=series_id, name=name, labels=labels)
            if series_id >= self._next_id:
                self._next_id = series_id + 1
            self._index_label(NAME_LABEL, metric, series_id)
            for label, value in labels.items():
                if label != NAME_LABEL:
                    self._index_label(label, value, series_id)
            return True

    def labels(self, series_id: int) -> dict[str, str]:
        with self._lock:
            series = self._by_id.get(series_id)
            return dict(series.labels) if series else {}

    def label_name(self, series_id: int) -> str:
        with self._lock:
            series = self._by_id.get(series_id)
            return series.name if series else ""

    def match(self, name: str, matchers: list[Matcher] | None = None) -> list[int]:
        """Resolve a selector to series IDs."""
        with self._lock:
            candidates = self._by_label.get(NAME_LABEL, {}).get(name)
            if candidates is None:
                return []
            ids = sorted(candidates)
        if not matchers:
            return ids
        return [sid for sid in ids if self._matches_all(sid, matchers)]

    def _matches_all(self, series_id: int, matchers: list[Matcher]) -> bool:
        with self._lock:
            series = self._by_id.get(series_id)
            if series is None:
                return False
            labels = series.labels
        return all(m.matches(labels.get(m.label, "")) for m in matchers)

    def series_names(self) -> list[str]:
        with self._lock:
            return sorted(series.name for series in self._by_id.values())

    def all_series(self) -> list[dict[str, object]]:
        with self._lock:
            return [
                {"id": series.id, "name": series.name, "labels": dict(series.labels)}
                for series in sorted(self._by_id.values(), key=lambda s: s.id)
            ]

    def stats(self) -> dict[str, int]:
        with self._lock:
            return {
                "series": len(self._by_id),
                "label_keys": len(self._by_label),
                "label_values": sum(len(v) for v in self._by_label.values()),
            }


def parse_selector(name: str) -> tuple[str, dict[str, str]]:
    """Inverse of :func:`encode_name`.

    Only ``=`` matchers survive, which is all a stored series name can contain;
    a selector carrying ``!=`` or ``=~`` is a query, not an identity.
    """
    metric, matchers = parse_matchers(name)
    labels = {m.label: m.value for m in matchers if m.op == "="}
    return metric, labels

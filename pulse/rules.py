"""Threshold alerting on a rolling window.

A rule reduces each matching series to one number over a window (``avg`` of
the last 60s, say), compares it against a threshold, and fires. Two details
matter and both are easy to get wrong:

* ``min_count`` -- a window holding two samples is not evidence. Without a
  floor, a series that just started alerting on its first point is noise.
* edge-triggered firing -- a condition that stays true must fire *once*, not
  once per evaluation tick. ``_firing`` tracks active (rule, series) pairs, and
  a rule that stops matching resolves, which is what lets it fire again later.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Protocol

from .query import AGGREGATIONS, Query

__all__ = ["Alert", "AlertRule", "DBQuerier", "Querier", "RulesEngine", "SeriesValue"]

#: Comparison operators a rule may use.
OPERATORS = (">", "<", ">=", "<=", "==", "!=")

#: Aggregations a rule may use; ``first`` is meaningless over a rolling window.
RULE_AGGREGATIONS = AGGREGATIONS - {"first"}


@dataclass(slots=True)
class SeriesValue:
    """One series reduced to a single number over a window."""

    name: str
    labels: dict[str, str]
    value: float
    samples: int


class Querier(Protocol):
    """The slice of the engine the rules layer needs, so it is testable
    against a stub instead of a database on disk."""

    def rule_values(self, metric: str, labels: dict[str, str] | None, window_secs: int, agg: str) -> list[SeriesValue]:
        ...


@dataclass(slots=True)
class AlertRule:
    """A threshold condition evaluated on a rolling window."""

    id: str
    name: str
    metric: str
    agg: str = "avg"
    window_sec: int = 60
    op: str = ">"
    threshold: float = 0.0
    labels: dict[str, str] = field(default_factory=dict)
    min_count: int = 1
    enabled: bool = True
    created_at: int = field(default_factory=lambda: int(time.time()))

    def __post_init__(self) -> None:
        if self.op not in OPERATORS:
            raise ValueError(f"unknown operator {self.op!r}; expected one of {list(OPERATORS)}")
        if self.agg not in RULE_AGGREGATIONS:
            raise ValueError(f"unsupported rule aggregation {self.agg!r}; expected one of {sorted(RULE_AGGREGATIONS)}")
        if self.window_sec <= 0:
            raise ValueError("window_sec must be > 0")
        if self.min_count < 1:
            raise ValueError("min_count must be >= 1")

    def evaluate(self, value: float) -> bool:
        if self.op == ">":
            return value > self.threshold
        if self.op == "<":
            return value < self.threshold
        if self.op == ">=":
            return value >= self.threshold
        if self.op == "<=":
            return value <= self.threshold
        if self.op == "==":
            return value == self.threshold
        return value != self.threshold

    def as_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "metric": self.metric,
            "labels": self.labels,
            "agg": self.agg,
            "windowSec": self.window_sec,
            "op": self.op,
            "threshold": self.threshold,
            "minCount": self.min_count,
            "enabled": self.enabled,
            "createdAt": self.created_at,
        }


@dataclass(slots=True)
class Alert:
    """A rule firing."""

    rule_id: str
    rule_name: str
    metric: str
    series: str
    labels: dict[str, str]
    value: float
    threshold: float
    op: str
    fired_at: int
    message: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "ruleId": self.rule_id,
            "ruleName": self.rule_name,
            "metric": self.metric,
            "series": self.series,
            "labels": self.labels,
            "value": self.value,
            "threshold": self.threshold,
            "op": self.op,
            "firedAt": self.fired_at,
            "message": self.message,
        }


Listener = Callable[[Alert], None]


class RulesEngine:
    """Evaluates rules on an interval and keeps a ring buffer of recent alerts."""

    def __init__(self, querier: Querier, interval_sec: float = 5.0, max_keep: int = 200) -> None:
        self._querier = querier
        self._interval = interval_sec
        self._max_keep = max_keep

        self._lock = threading.RLock()
        self._rules: dict[str, AlertRule] = {}
        self._order: list[str] = []
        self._alerts: list[Alert] = []  # most recent first
        self._firing: set[tuple[str, str]] = set()

        self._listeners: list[Listener] = []
        self._listener_lock = threading.RLock()

        self._evals = 0
        self._fired_total = 0

        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    # ------------------------------------------------------------------ rules

    def add(self, rule: AlertRule) -> AlertRule:
        with self._lock:
            if rule.id:
                if rule.id in self._rules:
                    raise ValueError(f"rule {rule.id!r} already exists")
                rule_id = rule.id
            else:
                # Scan for a free slot rather than counting: an explicitly
                # added rule can occupy any id, so a running counter would
                # eventually mint a duplicate.
                rule_id = next(f"r{i}" for i in range(1, len(self._rules) + 2) if f"r{i}" not in self._rules)
                rule.id = rule_id
            self._rules[rule.id] = rule
            self._order.append(rule.id)
            return rule

    def remove(self, rule_id: str) -> bool:
        with self._lock:
            if rule_id not in self._rules:
                return False
            del self._rules[rule_id]
            self._order.remove(rule_id)
            self._firing = {key for key in self._firing if key[0] != rule_id}
            return True

    def list(self) -> list[AlertRule]:
        with self._lock:
            return [self._rules[rid] for rid in self._order if rid in self._rules]

    def clear(self) -> None:
        with self._lock:
            self._rules.clear()
            self._order.clear()
            self._firing.clear()

    # ----------------------------------------------------------------- alerts

    def alerts(self) -> list[Alert]:
        with self._lock:
            return list(self._alerts)

    def _record(self, alert: Alert) -> None:
        with self._lock:
            self._alerts.insert(0, alert)
            del self._alerts[self._max_keep :]
            self._fired_total += 1

    def on_alert(self, listener: Listener) -> None:
        with self._listener_lock:
            self._listeners.append(listener)

    def _notify(self, alert: Alert) -> None:
        with self._listener_lock:
            listeners = list(self._listeners)
        for fn in listeners:
            fn(alert)

    # ------------------------------------------------------------- evaluation

    def evaluate_once(self) -> list[Alert]:
        """Run every enabled rule once. Returns the alerts that newly fired."""
        with self._lock:
            self._evals += 1
            rules = list(self.list())

        fired: list[Alert] = []
        now = int(time.time())

        for rule in rules:
            if not rule.enabled:
                self._resolve_rule(rule.id)
                continue
            try:
                values = self._querier.rule_values(rule.metric, rule.labels or None, rule.window_sec, rule.agg)
            except Exception as exc:  # noqa: BLE001 - one bad rule must not stop the rest
                print(f"pulse: rule {rule.id} failed: {exc}")
                continue

            seen: set[str] = set()
            for sv in values:
                if sv.samples < rule.min_count:
                    continue
                seen.add(sv.name)
                key = (rule.id, sv.name)
                active = rule.evaluate(sv.value)
                with self._lock:
                    was_firing = key in self._firing
                    if active:
                        self._firing.add(key)
                    else:
                        self._firing.discard(key)
                # Edge-triggered: only a transition into firing produces an alert.
                if active and not was_firing:
                    alert = Alert(
                        rule_id=rule.id,
                        rule_name=rule.name,
                        metric=rule.metric,
                        series=sv.name,
                        labels=sv.labels,
                        value=sv.value,
                        threshold=rule.threshold,
                        op=rule.op,
                        fired_at=now,
                        message=(
                            f"{rule.name}: {rule.agg}({rule.metric}) over {rule.window_sec}s "
                            f"is {sv.value:.4g}, expected {rule.op} {rule.threshold:g}"
                        ),
                    )
                    fired.append(alert)
                    self._record(alert)
                    self._notify(alert)

            # Any series that stopped appearing resolves, so a series that
            # disappears while alerting cannot stay stuck in the firing set.
            self._resolve_missing(rule.id, seen)

        return fired

    def _resolve_rule(self, rule_id: str) -> None:
        with self._lock:
            self._firing = {key for key in self._firing if key[0] != rule_id}

    def _resolve_missing(self, rule_id: str, seen: set[str]) -> None:
        with self._lock:
            self._firing = {key for key in self._firing if key[0] != rule_id or key[1] in seen}

    # ------------------------------------------------------------------- loop

    def start(self) -> None:
        """Start the background evaluation loop."""
        if self._thread is not None:
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="pulse-rules", daemon=True)
        self._thread.start()

    def _run(self) -> None:
        while not self._stop.wait(self._interval):
            self.evaluate_once()

    def stop(self, timeout: float = 2.0) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)
            self._thread = None

    def stats(self) -> dict[str, int]:
        with self._lock:
            return {
                "rules": len(self._rules),
                "enabled": sum(1 for r in self._rules.values() if r.enabled),
                "firing": len(self._firing),
                "alerts": len(self._alerts),
                "evaluations": self._evals,
                "fired_total": self._fired_total,
            }


class DBQuerier:
    """Adapter turning a :class:`~pulse.engine.DB` into a :class:`Querier`.

    Two queries per rule evaluation, not one per series: a range query already
    returns every matching series, so the counts come back in the same round
    trip as the values.

    ``count`` is queried separately because ``min_count`` has to be honest for
    *every* aggregation -- ``avg`` of a two-sample window is just as
    untrustworthy as ``max`` of one, and the value itself carries no record of
    how many samples produced it.
    """

    __slots__ = ("_db",)

    def __init__(self, db: Any) -> None:
        self._db = db

    def rule_values(self, metric: str, labels: dict[str, str] | None, window_secs: int, agg: str) -> list[SeriesValue]:
        now = int(time.time())
        start, end = now - window_secs, now + 1

        values = self._db.query(
            Query(metric=metric, labels=labels, start=start, end=end, step=window_secs, agg=agg)
        )
        counts = {
            res.name: len(res.points)
            for res in self._db.query(
                Query(metric=metric, labels=labels, start=start, end=end, step=0, agg="avg")
            )
        }

        out: list[SeriesValue] = []
        for res in values:
            if not res.points:
                continue
            out.append(
                SeriesValue(
                    name=res.name,
                    labels=res.labels,
                    value=res.points[-1].v,
                    samples=counts.get(res.name, 0),
                )
            )
        return out

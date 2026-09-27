"""Range queries and downsampling.

A query names a metric, optionally constrains labels, and asks for a time
range. ``step`` collapses raw samples into fixed-width buckets, which is what
makes a 24h chart render: you want a few hundred pixels of data, not two
million samples, and you want the *aggregation* to be a property of the
request rather than something the caller does after fetching everything.
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Any

__all__ = ["AGGREGATIONS", "Query", "Sample", "SeriesResult", "downsample"]

#: Reductions available to a range query.
AGGREGATIONS = frozenset({"avg", "max", "min", "sum", "last", "first", "count"})


@dataclass(slots=True)
class Sample:
    """One decoded point. Kept flat because it is produced by the millions."""

    t: int
    v: float


@dataclass(slots=True)
class SeriesResult:
    """One matching series and its points."""

    name: str
    labels: dict[str, str]
    points: list[Sample]

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "labels": self.labels,
            "points": [{"t": s.t, "v": s.v} for s in self.points],
        }


@dataclass(slots=True)
class Query:
    """A range query."""

    metric: str
    start: int
    end: int
    labels: dict[str, str] | None = None
    step: int = 0  # 0 = raw samples
    agg: str = "avg"

    def validate(self) -> None:
        if self.end < self.start:
            raise ValueError("start must be <= end")
        if self.agg not in AGGREGATIONS:
            raise ValueError(f"unknown aggregation {self.agg!r}; expected one of {sorted(AGGREGATIONS)}")
        if self.step < 0:
            raise ValueError("step must be >= 0")


def downsample(samples: Sequence[Sample], step: int, agg: str) -> list[Sample]:
    """Bucket samples into ``step``-second windows and reduce each.

    Buckets are aligned to the epoch, not to the first sample, so two queries
    over overlapping ranges agree on bucket boundaries and a chart's x-axis
    does not shift when the window slides.

    Assumes ``samples`` is sorted by timestamp; ``last`` and ``first`` are
    defined by that order.
    """
    if not samples or step <= 1:
        return list(samples)

    sum_ = 0.0
    count = 0
    high = -math.inf
    low = math.inf
    first = last = 0.0

    out: list[Sample] = []
    current_key = None

    def flush(key: int) -> None:
        out.append(Sample(t=key, v=_reduce(agg, sum_, high, low, first, last, count)))

    for s in samples:
        key = s.t - s.t % step
        if key != current_key:
            if current_key is not None:
                flush(current_key)
            current_key = key
            sum_, count, first = 0.0, 0, s.v
            high, low = -math.inf, math.inf
        sum_ += s.v
        count += 1
        last = s.v
        if s.v > high:
            high = s.v
        if s.v < low:
            low = s.v

    if current_key is not None:
        flush(current_key)
    return out


def _reduce(agg: str, total: float, high: float, low: float, first: float, last: float, n: int) -> float:
    if agg == "avg":
        return total / n
    if agg == "max":
        return high
    if agg == "min":
        return low
    if agg == "sum":
        return total
    if agg == "last":
        return last
    if agg == "first":
        return first
    return float(n)


def iter_in_range(iterable: Iterable[tuple[int, float]], start: int, end: int) -> Iterable[tuple[int, float]]:
    """Filter a decoded sample stream to ``[start, end]``, inclusive."""
    for t, v in iterable:
        if start <= t <= end:
            yield t, v

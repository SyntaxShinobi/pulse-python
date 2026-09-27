"""The storage engine: a mutable head, an immutable tail, and a WAL in front.

Writes go WAL -> head block. Reads scan the head plus every sealed file that
overlaps the requested range. That head/tail split is the shape InfluxDB,
VictoriaMetrics and Prometheus all converge on, and it is the part worth being
able to explain: the head is small and mutable so appends are cheap, the tail
is immutable and compressed so reads are sequential and the compression can
assume a finished block.

Durability rules, and the reasons:

* A batch is fsynced to the WAL *before* it becomes visible, so an
  acknowledged write survives a crash.
* ``flush`` seals the head but does not truncate the WAL -- it is a routine
  operation and recovery already dedupes.
* ``compact`` seals and *then* truncates. Block sealing fsyncs first, so by the
  time the WAL is dropped every sample in it is durable elsewhere.
* Recovery drops any WAL sample at or before a series' sealed high-water mark.
  A crash between sealing and truncating leaves the same samples in both
  places; without this guard a restart silently doubles the data.
"""

from __future__ import annotations

import os
import sys
import threading
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass

from .block import BLOCK_SPAN_SECONDS, Block, align_block_base, decode_block, sealed_block_size
from .index import Matcher, SeriesIndex, parse_selector
from .query import Query, Sample, SeriesResult, downsample, iter_in_range
from .wal import RECORD_BATCH, RECORD_SERIES, WAL, WALRecord, recover_wal

__all__ = ["DB", "SealedBlock", "open_db"]

#: Callback signature for ingest subscribers (rules engine, live feed).
Listener = Callable[[int, Sequence[int], Sequence[float]], None]


@dataclass(slots=True)
class SealedBlock:
    """An immutable, already-encoded block on disk."""

    series_id: int
    name: str
    base_time: int
    min_ts: int
    max_ts: int
    count: int
    offset: int
    size: int
    data: bytes  # retained in memory; mmap past ~1M blocks


class DB:
    """The time-series store."""

    def __init__(self, directory: str | os.PathLike[str]) -> None:
        self.dir = os.fspath(directory)
        os.makedirs(self.dir, exist_ok=True)
        self.index = SeriesIndex()
        self.wal = WAL(os.path.join(self.dir, "wal.log"))

        self._lock = threading.RLock()
        self._head: dict[int, Block] = {}
        self._sealed: list[SealedBlock] = []

        self._samples_in = 0
        self._samples_out = 0
        self._bytes_on_disk = 0
        self._blocks_sealed = 0
        self._samples_recovered = 0
        self._created_at = time.time()

        self._listeners: list[Listener] = []
        self._listener_lock = threading.RLock()

        self.recover()

    # ------------------------------------------------------------------ open

    def recover(self) -> None:
        """Load sealed blocks, then replay the WAL into the head.

        Order matters, and so does the dedupe: see the module docstring.
        """
        self._load_sealed()

        sealed_max: dict[int, int] = {}
        for sb in self._sealed:
            sealed_max[sb.series_id] = max(sealed_max.get(sb.series_id, -(1 << 62)), sb.max_ts)

        def on_record(record: WALRecord) -> None:
            if record.type == RECORD_SERIES:
                # Re-intern under the recorded ID so IDs stay stable across
                # restarts; sealed blocks reference series by number.
                self.index.register(record.series_id, record.name)
            elif record.type == RECORD_BATCH:
                cutoff = sealed_max.get(record.series_id, -(1 << 62))
                for t, v in zip(record.timestamps, record.values, strict=True):
                    if t <= cutoff:
                        continue  # already durable in a sealed block
                    self._append_to_head(record.series_id, t, v)

        if recover_wal(self.wal.path, on_record):
            # A torn tail is expected after a crash or kill -9: keep the intact
            # prefix and carry on rather than refusing to start.
            print(f"pulse: WAL {self.wal.path} had a corrupt tail; recovered the intact prefix", file=sys.stderr)
            self.wal._corrupt_tail = True  # noqa: SLF001 - recovery is the only writer of this flag

        # Count what came back. Without this a restarted server reports zero
        # samples ingested while holding thousands in the head and on disk, and
        # the dashboard reads "0 samples" next to a full chart.
        recovered = sum(b.count for b in self._head.values())
        recovered += sum(sb.count for sb in self._sealed)
        self._samples_in += recovered
        self._samples_recovered = recovered

    def _load_sealed(self) -> None:
        """Read every ``*.blk`` file in the data directory."""
        for entry in sorted(os.listdir(self.dir)):
            if not entry.endswith(".blk"):
                continue
            path = os.path.join(self.dir, entry)
            if not os.path.isfile(path):
                continue
            with open(path, "rb") as fh:
                data = fh.read()

            off = 0
            while off < len(data):
                size = sealed_block_size(data[off:])
                if size <= 0 or off + size > len(data):
                    break  # truncated tail; keep what we have
                try:
                    decoded = decode_block(data[off : off + size])
                except ValueError:
                    break
                # Re-register the series from the block header. The WAL is
                # truncated after a clean flush, so sealed blocks are the only
                # surviving record of the name-to-ID mapping; without this the
                # index comes back empty and queries match nothing.
                self.index.register(decoded.series_id, decoded.name)
                self._sealed.append(
                    SealedBlock(
                        series_id=decoded.series_id,
                        name=decoded.name,
                        base_time=decoded.base_time,
                        min_ts=decoded.min_ts,
                        max_ts=decoded.max_ts,
                        count=decoded.count,
                        offset=off,
                        size=size,
                        data=bytes(data[off : off + size]),
                    )
                )
                self._bytes_on_disk += size
                off += size

        self._sealed.sort(key=lambda sb: sb.min_ts)

    # ----------------------------------------------------------------- write

    def subscribe(self, listener: Listener) -> None:
        """Register a callback fired for every accepted batch."""
        with self._listener_lock:
            self._listeners.append(listener)

    def _notify(self, series_id: int, ts: Sequence[int], values: Sequence[float]) -> None:
        with self._listener_lock:
            listeners = list(self._listeners)
        for fn in listeners:
            fn(series_id, ts, values)

    def write_one(self, name: str, timestamp: int, value: float) -> None:
        """Append a single sample. Prefer :meth:`write` for throughput."""
        self.write(name, [timestamp], [value])

    def write(self, name: str, timestamps: Sequence[int], values: Sequence[float]) -> None:
        """Append a batch to one series, durably.

        One fsync per batch, not per sample: on rotating media an fsync costs
        milliseconds, so batching is the single biggest lever on ingest rate.
        """
        if len(timestamps) != len(values):
            raise ValueError("timestamps and values length mismatch")
        if not timestamps:
            return

        series_id, created = self.index.create(*parse_selector(name))
        if created:
            self.wal.log_series(series_id, name)
        self.wal.log_batch(series_id, timestamps, values)
        self.wal.sync()

        for t, v in zip(timestamps, values, strict=True):
            self._append_to_head(series_id, t, v)
        self._samples_in += len(timestamps)
        self._notify(series_id, timestamps, values)

    def _append_to_head(self, series_id: int, timestamp: int, value: float) -> None:
        """Route a sample into the block covering its window, sealing on rollover."""
        base = align_block_base(timestamp)
        with self._lock:
            block = self._head.get(series_id)
            if block is None or block.full(timestamp):
                if block is not None and block.count > 0:
                    try:
                        self._seal(block)
                    except OSError as exc:
                        print(f"pulse: seal failed: {exc}", file=sys.stderr)
                block = Block(series_id, self.index.label_name(series_id), base)
                self._head[series_id] = block
            block.append(timestamp, value)

    def _seal(self, block: Block) -> None:
        """Encode a full block and append it to a sealed file. Caller holds the lock."""
        encoded = block.encode()
        path = os.path.join(self.dir, f"block_{block.base_time}.blk")
        fd = os.open(path, os.O_CREAT | os.O_WRONLY | os.O_APPEND, 0o644)
        try:
            offset = os.lseek(fd, 0, os.SEEK_END)
            os.write(fd, encoded)
            os.fsync(fd)
        finally:
            os.close(fd)

        min_ts, max_ts = block.range
        self._sealed.append(
            SealedBlock(
                series_id=block.series_id,
                name=block.name,
                base_time=block.base_time,
                min_ts=min_ts,
                max_ts=max_ts,
                count=block.count,
                offset=offset,
                size=len(encoded),
                data=encoded,
            )
        )
        self._sealed.sort(key=lambda sb: sb.min_ts)
        self._bytes_on_disk += len(encoded)
        self._blocks_sealed += 1

    def flush(self) -> None:
        """Seal every open head block. Deliberately does not truncate the WAL."""
        with self._lock:
            for series_id, block in list(self._head.items()):
                if block.count > 0:
                    self._seal(block)
                del self._head[series_id]

    def compact(self) -> None:
        """Seal the head, then truncate the WAL. What a clean shutdown calls."""
        self.flush()
        with self._lock:
            self.wal.truncate()

    def close(self) -> None:
        """Compact and release the WAL file."""
        self.compact()
        self.wal.close()

    def __enter__(self) -> "DB":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # ------------------------------------------------------------------ read

    def series_name(self, series_id: int) -> str:
        return self.index.label_name(series_id)

    def all_series(self) -> list[dict[str, object]]:
        return self.index.all_series()

    def query(self, q: Query) -> list[SeriesResult]:
        """Run a range query, returning one result per matching series."""
        q.validate()
        ids = self.index.match(q.metric, _matchers_for(q.labels))

        results: list[SeriesResult] = []
        for series_id in ids:
            points = self._scan(series_id, q)
            self._samples_out += len(points)
            results.append(
                SeriesResult(
                    name=self.index.label_name(series_id),
                    labels=self.index.labels(series_id),
                    points=points,
                )
            )
        return results

    def _scan(self, series_id: int, q: Query) -> list[Sample]:
        """Collect every sample for one series in range, then downsample."""
        sources: list[bytes] = []
        with self._lock:
            head = self._head.get(series_id)
            if head is not None:
                sources.append(head.encode())
            for sb in self._sealed:
                if sb.series_id != series_id:
                    continue
                if sb.max_ts < q.start or sb.min_ts > q.end:
                    continue
                sources.append(sb.data)

        raw: list[Sample] = []
        for encoded in sources:
            decoded = decode_block(encoded)
            raw.extend(Sample(t=t, v=v) for t, v in iter_in_range(decoded, q.start, q.end))

        raw.sort(key=lambda s: s.t)
        return downsample(raw, q.step, q.agg)

    def latest(self, metric: str, window_secs: int = 300) -> list[SeriesResult]:
        """Most recent value per series, for current-value tiles and rules."""
        now = int(time.time())
        return self.query(
            Query(metric=metric, start=now - window_secs, end=now + 1, step=window_secs, agg="last")
        )

    def stats(self) -> dict[str, object]:
        """Engine counters for the API and the dashboard."""
        with self._lock:
            head_samples = sum(b.count for b in self._head.values())
            head_blocks = len(self._head)
            sealed_blocks = len(self._sealed)
            sealed_samples = sum(sb.count for sb in self._sealed)
        wal = self.wal.stats()
        return {
            "series": len(self.index),
            "head_blocks": head_blocks,
            "head_samples": head_samples,
            "sealed_blocks": sealed_blocks,
            "sealed_samples": sealed_samples,
            "total_samples": self._samples_in,
            "recovered_samples": self._samples_recovered,
            "samples_read": self._samples_out,
            "bytes_on_disk": self._bytes_on_disk,
            "blocks_sealed": self._blocks_sealed,
            "wal_records": wal["records"],
            "wal_bytes": wal["bytes"],
            "wal_syncs": wal["syncs"],
            "uptime_seconds": int(time.time() - self._created_at),
            "block_span_secs": BLOCK_SPAN_SECONDS,
        }


def _matchers_for(labels: dict[str, str] | None):
    """Build equality matchers from a label dict, or None for no constraint."""
    if not labels:
        return None
    return [Matcher(op="=", label=key, value=value) for key, value in labels.items()]


def open_db(directory: str | os.PathLike[str]) -> DB:
    """Open or create a database directory."""
    return DB(directory)

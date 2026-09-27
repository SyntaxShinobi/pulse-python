"""Write-ahead log.

The WAL is what makes a crash survivable. Every batch of samples hits the log
and is fsynced before the server acknowledges it, so a process that dies
mid-block loses nothing the client was told was accepted.

Record layout::

    type        1 byte   1 = batch, 2 = series name mapping
    payload_len 4 bytes  big endian
    payload     n bytes
    crc32       4 bytes  IEEE, over type + payload_len + payload

Batch payload (columnar, matching the block encoding)::

    series_id  8 bytes
    count      4 bytes
    count x int64   seconds
    count x float64 values

Note the columns are separate: a struct format string like ``">50qd"`` does
*not* mean "50 (q,d) pairs" -- a repeat count binds to the next character only,
so that reads 50 timestamps and exactly one value.

Mapping payload::

    series_id  8 bytes
    name_len   2 bytes
    name       name_len bytes

Recovery reads records until EOF or the first truncated/corrupt one and stops
there, which is the correct response to a torn final write after a power cut:
you lose one batch, not the file.
"""

from __future__ import annotations

import os
import struct
import threading
import zlib
from dataclasses import dataclass
from typing import Callable, Iterable

__all__ = ["WAL", "WALRecord", "recover_wal", "RECORD_BATCH", "RECORD_SERIES"]

RECORD_BATCH = 1
RECORD_SERIES = 2

_HEADER = struct.Struct(">BI")
_MAX_PAYLOAD = 64 << 20


@dataclass(slots=True)
class WALRecord:
    """One recovered record."""

    type: int
    series_id: int
    name: str = ""
    timestamps: tuple[int, ...] = ()
    values: tuple[float, ...] = ()


class WAL:
    """Append-only log with buffered writes and explicit sync.

    One fsync per batch, not per sample: batching is what keeps the syscall
    count low, and it is why :meth:`log_batch` takes sequences rather than a
    single sample.
    """

    __slots__ = ("_fh", "_lock", "_path", "_records", "_bytes", "_syncs", "_corrupt_tail")

    def __init__(self, path: str | os.PathLike[str]) -> None:
        self._path = os.fspath(path)
        self._lock = threading.Lock()
        self._fh = open(self._path, "ab", buffering=0)
        self._records = 0
        self._bytes = 0
        self._syncs = 0
        self._corrupt_tail = False

    @property
    def path(self) -> str:
        return self._path

    @property
    def corrupt_tail(self) -> bool:
        """True if recovery stopped early on a bad record."""
        return self._corrupt_tail

    def log_series(self, series_id: int, name: str) -> None:
        """Record a name-to-ID mapping so recovery can rebuild the index."""
        raw = name.encode("utf-8")
        payload = struct.pack(">QH", series_id, len(raw)) + raw
        self._append(RECORD_SERIES, payload)

    def log_batch(self, series_id: int, timestamps: Iterable[int], values: Iterable[float]) -> None:
        """Record a batch of samples for one series."""
        ts = list(timestamps)
        vs = list(values)
        if len(ts) != len(vs):
            raise ValueError("timestamps and values length mismatch")
        payload = struct.pack(">QI", series_id, len(ts))
        payload += struct.pack(f">{len(ts)}q", *ts)
        payload += struct.pack(f">{len(ts)}d", *vs)
        self._append(RECORD_BATCH, payload)

    def _append(self, record_type: int, payload: bytes) -> None:
        if self._fh is None:
            raise ValueError("WAL is closed")
        header = _HEADER.pack(record_type, len(payload))
        body = header + payload
        record = body + struct.pack(">I", zlib.crc32(body) & 0xFFFFFFFF)
        with self._lock:
            self._fh.write(record)
            self._records += 1
            self._bytes += len(record)

    def sync(self) -> None:
        """Flush to stable storage. Ingest calls this once per batch."""
        with self._lock:
            if self._fh is None:
                raise ValueError("WAL is closed")
            self._fh.flush()
            os.fsync(self._fh.fileno())
            self._syncs += 1

    def stats(self) -> dict[str, int]:
        with self._lock:
            return {"records": self._records, "bytes": self._bytes, "syncs": self._syncs}

    def truncate(self) -> None:
        """Empty the log. Called after sealed blocks are fsynced to disk."""
        with self._lock:
            if self._fh is None:
                raise ValueError("WAL is closed")
            self._fh.flush()
            os.ftruncate(self._fh.fileno(), 0)
            self._fh.seek(0)
            self._records = 0
            self._bytes = 0

    def close(self) -> None:
        with self._lock:
            if self._fh is None:
                return
            self._fh.flush()
            self._fh.close()
            self._fh = None  # type: ignore[assignment]

    def __enter__(self) -> "WAL":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


def recover_wal(path: str | os.PathLike[str], on_record: Callable[[WALRecord], None]) -> bool:
    """Replay a WAL, calling ``on_record`` for each intact record.

    Returns True if it stopped early on a truncated or corrupt record, which is
    expected after a crash or ``kill -9`` and costs one batch rather than the
    file.
    """
    path = os.fspath(path)
    if not os.path.exists(path):
        return False

    corrupt = False
    with open(path, "rb") as fh:
        while True:
            header = fh.read(_HEADER.size)
            if not header:
                break  # clean end
            if len(header) < _HEADER.size:
                corrupt = True
                break
            record_type, payload_len = _HEADER.unpack(header)
            if payload_len > _MAX_PAYLOAD:
                corrupt = True
                break
            body = fh.read(payload_len + 4)
            if len(body) < payload_len + 4:
                corrupt = True  # torn payload
                break
            payload, checksum = body[:payload_len], body[payload_len:]
            expected = struct.unpack(">I", checksum)[0]
            if zlib.crc32(header + payload) & 0xFFFFFFFF != expected:
                corrupt = True  # checksum mismatch
                break
            try:
                record = _parse_record(record_type, payload)
            except (struct.error, ValueError):
                corrupt = True
                break
            on_record(record)
    return corrupt


def _parse_record(record_type: int, payload: bytes) -> WALRecord:
    if record_type == RECORD_SERIES:
        if len(payload) < 10:
            raise ValueError("short series record")
        series_id, name_len = struct.unpack_from(">QH", payload)
        if len(payload) < 10 + name_len:
            raise ValueError("truncated series name")
        name = payload[10 : 10 + name_len].decode("utf-8", errors="replace")
        return WALRecord(type=record_type, series_id=series_id, name=name)

    if record_type == RECORD_BATCH:
        if len(payload) < 12:
            raise ValueError("short batch record")
        series_id, count = struct.unpack_from(">QI", payload)
        if len(payload) < 12 + count * 16:
            raise ValueError("truncated batch payload")
        timestamps = struct.unpack_from(f">{count}q", payload, 12)
        values = struct.unpack_from(f">{count}d", payload, 12 + count * 8)
        return WALRecord(
            type=record_type,
            series_id=series_id,
            timestamps=timestamps,
            values=values,
        )

    raise ValueError(f"unknown WAL record type {record_type}")

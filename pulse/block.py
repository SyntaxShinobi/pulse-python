"""Block file format.

Samples accumulate in a 2-hour block -- the point where the Gorilla paper's
own measurements show further widening stops improving compression while
making short-range reads decode data they will throw away -- and then the
block is sealed into an immutable file.

Layout::

    magic      6 bytes  b"PULSEB"
    version    2 bytes  big endian, currently 1
    series_id  8 bytes
    name_len   2 bytes
    name       name_len bytes
    base_time  8 bytes  signed seconds
    min_ts     8 bytes
    max_ts     8 bytes
    count      4 bytes
    payload_len 4 bytes
    payload    payload_len bytes   (bit-packed, see pulse.codec)

The full series *selector* is stored in ``name``, not the bare metric name.
Sealed blocks are the durable record of the name-to-ID mapping once the WAL is
compacted, so storing only the metric here silently drops every label on the
next restart. That was a real bug in the Go original.
"""

from __future__ import annotations

import struct
import threading
from dataclasses import dataclass, field
from typing import Iterator

from .codec import Codec, CodecDecoder

__all__ = [
    "BLOCK_SPAN_SECONDS",
    "MAX_SAMPLES_PER_BLOCK",
    "Block",
    "DecodedBlock",
    "BlockFullError",
    "align_block_base",
    "sealed_block_size",
    "decode_block",
]

#: Wall-clock window a single block covers.
BLOCK_SPAN_SECONDS = 2 * 3600

#: Hard cap on block size even if a series is written faster than 1 Hz.
MAX_SAMPLES_PER_BLOCK = 8192

_MAGIC = b"PULSEB"
_VERSION = 1
_HEADER_PREFIX = 6 + 2 + 8 + 2  # magic + version + series_id + name_len
_HEADER_SUFFIX = 8 + 8 + 8 + 4 + 4
_MIN_HEADER = _HEADER_PREFIX + _HEADER_SUFFIX


class BlockFullError(RuntimeError):
    """Raised when a sample does not belong in this block's window."""


def align_block_base(timestamp: int) -> int:
    """Start of the block window containing ``timestamp``."""
    if timestamp < 0:
        return 0
    return timestamp - timestamp % BLOCK_SPAN_SECONDS


def sealed_block_size(data: bytes | bytearray | memoryview) -> int:
    """Encoded length of the block at the start of ``data``.

    Lets a caller walk a block file without decoding anything.
    """
    if len(data) < _HEADER_PREFIX:
        return len(data)
    (name_len,) = struct.unpack_from(">H", data, 16)
    payload_off = _HEADER_PREFIX + name_len + _HEADER_SUFFIX - 4
    if len(data) < payload_off + 4:
        return len(data)
    (payload_len,) = struct.unpack_from(">I", data, payload_off)
    return payload_off + 4 + payload_len


class Block:
    """One series' samples inside a fixed 2h window.

    A live head block is appended to by the ingest path while queries encode it
    to read it back, so it carries a lock. ``encode`` is not a pure read -- it
    has to pad the partial byte -- and two concurrent encodes racing on the
    codec buffer produce corrupt samples rather than an error.
    """

    __slots__ = ("series_id", "name", "base_time", "_lock", "_codec", "_count", "_min_ts", "_max_ts")

    def __init__(self, series_id: int, name: str, base_time: int) -> None:
        self.series_id = series_id
        self.name = name
        self.base_time = base_time
        self._lock = threading.RLock()
        self._codec = Codec(base_time)
        self._count = 0
        self._min_ts = -1
        self._max_ts = 0

    @property
    def count(self) -> int:
        with self._lock:
            return self._count

    @property
    def range(self) -> tuple[int, int]:
        with self._lock:
            return self._min_ts, self._max_ts

    def full(self, timestamp: int) -> bool:
        with self._lock:
            return self._full_locked(timestamp)

    def _full_locked(self, timestamp: int) -> bool:
        if self._count >= MAX_SAMPLES_PER_BLOCK:
            return True
        if timestamp < self.base_time:
            return True
        return timestamp >= self.base_time + BLOCK_SPAN_SECONDS

    def append(self, timestamp: int, value: float) -> bool:
        """Encode one sample. Returns False if the block is full for it."""
        with self._lock:
            if self._full_locked(timestamp):
                return False
            self._codec.append(timestamp, value)
            self._count += 1
            if self._min_ts < 0 or timestamp < self._min_ts:
                self._min_ts = timestamp
            if timestamp > self._max_ts:
                self._max_ts = timestamp
            return True

    def encode(self) -> bytes:
        """Serialise the block.

        Uses the codec's non-mutating snapshot, so this is safe to call on a
        block that is still being appended to.
        """
        with self._lock:
            payload = self._codec.payload()
            name = self.name.encode("utf-8")
            header = bytearray()
            header += _MAGIC
            header += struct.pack(">H", _VERSION)
            header += struct.pack(">Q", self.series_id)
            header += struct.pack(">H", len(name))
            header += name
            header += struct.pack(">qqq", self.base_time, self._min_ts, self._max_ts)
            header += struct.pack(">I", self._count)
            header += struct.pack(">I", len(payload))
            return bytes(header) + payload


@dataclass(slots=True)
class DecodedBlock:
    """Read side of a block. Samples stream out, so a query never has to
    materialise a whole block."""

    series_id: int
    name: str
    base_time: int
    min_ts: int
    max_ts: int
    count: int
    _decoder: CodecDecoder = field(repr=False)

    def __iter__(self) -> Iterator[tuple[int, float]]:
        return iter(self._decoder)

    def collect(self) -> list[tuple[int, float]]:
        return list(self._decoder)


def decode_block(data: bytes | bytearray | memoryview) -> DecodedBlock:
    """Parse an encoded block and position a streaming decoder on it."""
    if len(data) < _MIN_HEADER:
        raise ValueError("block too short")
    if bytes(data[:6]) != _MAGIC:
        raise ValueError("bad block magic")
    (version,) = struct.unpack_from(">H", data, 6)
    if version != _VERSION:
        raise ValueError(f"unsupported block version {version}")
    (series_id,) = struct.unpack_from(">Q", data, 8)
    (name_len,) = struct.unpack_from(">H", data, 16)
    if len(data) < _MIN_HEADER + name_len:
        raise ValueError("truncated block header")
    name = bytes(data[18 : 18 + name_len]).decode("utf-8", errors="replace")

    off = 18 + name_len
    base_time, min_ts, max_ts = struct.unpack_from(">qqq", data, off)
    (count,) = struct.unpack_from(">I", data, off + 24)
    (payload_len,) = struct.unpack_from(">I", data, off + 28)
    off += 32
    if len(data) < off + payload_len:
        raise ValueError("truncated block payload")

    return DecodedBlock(
        series_id=series_id,
        name=name,
        base_time=base_time,
        min_ts=min_ts,
        max_ts=max_ts,
        count=count,
        _decoder=CodecDecoder(bytes(data[off : off + payload_len]), count),
    )

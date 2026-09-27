"""Time-series compression, after Pelkonen et al., *"Gorilla: A Fast,
Scalable, In-Memory Time Series Database"* (VLDB 2015).

A sample is ``(timestamp, value)`` -- 16 bytes raw. Two properties of
monitoring data let us do much better:

1. **Timestamps arrive on a cadence.** What matters is not the delta but how
   the delta *changed*. A series sampled every 10s has deltas of 10, 10, 10,
   so the delta-of-delta is 0 and costs a single bit. The paper reports 96% of
   production timestamps in that case.

2. **Consecutive values are similar.** XORing two nearby doubles leaves a word
   that is mostly zeros, so only the meaningful middle bits are stored. The
   paper reports 59% of production values compressing to 1 bit.

Layout
------
Block header (byte aligned), written by :mod:`pulse.block`, then::

    sample 0            u64 timestamp, f64 value          (stored whole)
    sample n > 0, ts    '0'                               dod == 0
                        '10'   + zigzag(dod) in  9 bits
                        '110'  + zigzag(dod) in 12 bits
                        '1110' + zigzag(dod) in 32 bits
                        '1111' + zigzag(dod) in 64 bits
    sample n > 0, val   '0'                               xor == 0
                        '10' + 5b lead + 6b len + bits    reuse previous window
                        '11' + 5b lead + 6b len + bits    new window

Two details are easy to get wrong and both were real bugs in the Go original:

* A 6-bit length field cannot hold 64. A window spanning the whole word --
  which happens on the first XOR -- is stored as 0 and recovered on read.
* The encoder leaves its window untouched in the reuse branch, so the decoder
  must too, or the two sides drift out of sync within a few samples.
"""

from __future__ import annotations

import struct
from typing import Iterator

from .bitstream import BitReader, BitWriter, unzigzag, zigzag

__all__ = ["Codec", "CodecDecoder"]

_UINT64_MAX = (1 << 64) - 1
_INT64_MIN = -(1 << 63)


def _to_uint64(value: int) -> int:
    """Reinterpret a signed 64-bit int as unsigned, as Go/C would."""
    return value & _UINT64_MAX


def _to_int64(value: int) -> int:
    """Reinterpret the low 64 bits as a signed int."""
    value &= _UINT64_MAX
    return value - (1 << 64) if value >= 1 << 63 else value


def _leading_zeros(value: int) -> int:
    if value == 0:
        return 64
    return 64 - value.bit_length()


def _trailing_zeros(value: int) -> int:
    if value == 0:
        return 64
    return (value & -value).bit_length() - 1


def _encode_sigbits(n: int) -> int:
    """Pack a window width into the 6-bit length field.

    64 does not fit in six bits, so it is stored as 0.
    """
    return 0 if n >= 64 else n


def _decode_sigbits(raw: int) -> int:
    return 64 if raw == 0 else raw


class Codec:
    """Compresses one series into a single block."""

    __slots__ = (
        "_base_time",
        "_prev_t",
        "_prev_delta",
        "_prev_value",
        "_prev_lead",
        "_prev_trail",
        "_has_window",
        "_count",
        "_w",
    )

    def __init__(self, base_time: int) -> None:
        self._base_time = base_time
        self._prev_t = 0
        self._prev_delta = 0
        self._prev_value = 0
        self._prev_lead = 0
        self._prev_trail = 0
        self._has_window = False
        self._count = 0
        self._w = BitWriter()

    @property
    def count(self) -> int:
        return self._count

    @property
    def written_bits(self) -> int:
        return self._w.written_bits

    def append(self, timestamp: int, value: float) -> None:
        """Encode one sample. Timestamps must be non-decreasing."""
        value_bits = struct.unpack(">Q", struct.pack(">d", value))[0]

        if self._count == 0:
            # No previous state to delta against, so store the first whole.
            self._w.write_uint(_to_uint64(timestamp), 64)
            self._w.write_uint(value_bits, 64)
            self._prev_t = timestamp
            self._prev_value = value_bits
            # The delta before the first sample is defined as zero. Without
            # this, sample 1 encodes dod = t1 - t0 - 0 as a giant number.
            self._prev_delta = 0
            self._count = 1
            return

        self._encode_timestamp(timestamp)
        self._encode_value(value_bits)
        self._prev_t = timestamp
        self._prev_value = value_bits
        self._count += 1

    def _encode_timestamp(self, timestamp: int) -> None:
        delta = timestamp - self._prev_t
        dod = delta - self._prev_delta
        z = zigzag(dod)

        if dod == 0:
            self._w.write_bit(False)
        elif z <= 0xFF:
            self._w.write_uint(0b10, 2)
            self._w.write_uint(z, 9)
        elif z <= 0xFFF:
            self._w.write_uint(0b110, 3)
            self._w.write_uint(z, 12)
        elif z <= 0xFFFFFFFF:
            self._w.write_uint(0b1110, 4)
            self._w.write_uint(z, 32)
        else:
            self._w.write_uint(0b1111, 4)
            self._w.write_uint(z, 64)
        self._prev_delta = delta

    def _encode_value(self, value: int) -> None:
        xor = self._prev_value ^ value
        if xor == 0:
            self._w.write_bit(False)
            return

        lead = _leading_zeros(xor)
        trail = _trailing_zeros(xor)
        if lead > 31:  # the 5-bit header can only describe 0..31
            lead = 31
        sigbits = 64 - lead - trail

        # If the meaningful bits sit inside the previous window we reuse it
        # and save the 11-bit header. The first XOR has no window to reuse, so
        # it always takes the '11' path below.
        if self._has_window and lead >= self._prev_lead and trail >= self._prev_trail:
            reuse = 64 - self._prev_lead - self._prev_trail
            self._w.write_uint(0b10, 2)
            self._w.write_uint(self._prev_lead, 5)
            self._w.write_uint(_encode_sigbits(reuse), 6)
            self._w.write_uint(xor >> self._prev_trail, reuse)
            return

        self._w.write_uint(0b11, 2)
        self._w.write_uint(lead, 5)
        self._w.write_uint(_encode_sigbits(sigbits), 6)
        self._w.write_uint(xor >> trail, sigbits)
        self._prev_lead = lead
        self._prev_trail = trail
        self._has_window = True

    def payload(self) -> bytes:
        """Return the encoded payload without finalising the codec.

        Safe to call on a block that is still being appended to. See the note
        on :meth:`BitWriter.snapshot`.
        """
        return self._w.snapshot()

    def finalize(self) -> bytes:
        """Finalise and return the payload. No further appends after this."""
        return self._w.tobytes()


class CodecDecoder:
    """Reverses :class:`Codec`.

    The sample count lives in the enclosing block header rather than in the
    bit stream, so it is passed in -- keeping it out of the stream is part of
    what makes the stream compact.
    """

    __slots__ = (
        "_r",
        "_total",
        "_read",
        "_cur_t",
        "_cur_v",
        "_prev_t",
        "_prev_delta",
        "_prev_value",
        "_prev_lead",
        "_prev_trail",
        "_has_window",
    )

    def __init__(self, payload: bytes, total: int) -> None:
        self._r = BitReader(payload)
        self._total = total
        self._read = 0
        self._cur_t = 0
        self._cur_v = 0.0
        self._prev_t = 0
        self._prev_delta = 0
        self._prev_value = 0
        self._prev_lead = 0
        self._prev_trail = 0
        self._has_window = False

    @property
    def total(self) -> int:
        return self._total

    def at(self) -> tuple[int, float]:
        """The sample most recently produced by :meth:`__next__`."""
        return self._cur_t, self._cur_v

    def __iter__(self) -> Iterator[tuple[int, float]]:
        return self

    def __next__(self) -> tuple[int, float]:
        if self._read >= self._total:
            raise StopIteration
        if self._read == 0:
            ts = _to_int64(self._r.read_uint(64))
            value = struct.unpack(">d", struct.pack(">Q", self._r.read_uint(64)))[0]
            self._prev_t = ts
            self._prev_value = struct.unpack(">Q", struct.pack(">d", value))[0]
        else:
            ts = self._decode_timestamp()
            value_bits = self._decode_value()
            value = struct.unpack(">d", struct.pack(">Q", value_bits))[0]
            self._prev_t = ts
            self._prev_value = value_bits
        self._read += 1
        self._cur_t = ts
        self._cur_v = value
        return ts, value

    def _decode_timestamp(self) -> int:
        if not self._r.read_bit():
            return self._prev_t + self._prev_delta  # dod == 0

        if not self._r.read_bit():
            z = self._r.read_uint(9)
        elif not self._r.read_bit():
            z = self._r.read_uint(12)
        elif not self._r.read_bit():
            z = self._r.read_uint(32)
        else:
            z = self._r.read_uint(64)

        delta = self._prev_delta + unzigzag(z)
        self._prev_delta = delta
        return self._prev_t + delta

    def _decode_value(self) -> int:
        if not self._r.read_bit():
            return self._prev_value

        if not self._r.read_bit():  # '10': reuse the previous window
            if not self._has_window:
                raise ValueError("window reuse before any window was set")
            self._r.read_uint(5)  # leading zeros, informational
            sigbits = _decode_sigbits(self._r.read_uint(6))
            bits = self._r.read_uint(sigbits)
            # The encoder left its window untouched here, so we must not touch
            # ours either or the two sides drift out of sync.
            return self._prev_value ^ (bits << self._prev_trail)

        lead = self._r.read_uint(5)
        sigbits = _decode_sigbits(self._r.read_uint(6))
        bits = self._r.read_uint(sigbits)
        trail = 64 - lead - sigbits
        self._prev_lead = lead
        self._prev_trail = trail
        self._has_window = True
        return self._prev_value ^ (bits << trail)

"""Bit-level packing.

The time-series codec in :mod:`pulse.codec` encodes most samples in a handful
of bits, so working at byte granularity would throw away the whole point. This
module is the only place that knows how bits are laid out inside a byte.

Bits are packed most-significant-bit first, which matches the Gorilla paper and
makes hex dumps readable.
"""

from __future__ import annotations

__all__ = ["BitWriter", "BitReader", "zigzag", "unzigzag"]


class BitWriter:
    """An append-only bit stream.

    Call :meth:`snapshot` to read a stream that is still being written, and
    :meth:`tobytes` when you are finished with it. The distinction matters and
    is not cosmetic -- see the note on :meth:`snapshot`.
    """

    __slots__ = ("_buf", "_cur", "_n")

    def __init__(self) -> None:
        self._buf = bytearray()
        self._cur = 0  # partial byte being filled
        self._n = 0  # how many bits of _cur are valid

    def write_bit(self, bit: bool) -> None:
        """Append a single bit."""
        self._cur = (self._cur << 1) | (1 if bit else 0)
        self._n += 1
        if self._n == 8:
            self._buf.append(self._cur & 0xFF)
            self._cur = 0
            self._n = 0

    def write_uint(self, value: int, nbits: int) -> None:
        """Append the low ``nbits`` of ``value``, most significant bit first.

        ``nbits`` must be in ``[0, 64]``.
        """
        if not 0 <= nbits <= 64:
            raise ValueError(f"nbits must be in [0, 64], got {nbits}")
        for i in range(nbits - 1, -1, -1):
            self.write_bit((value >> i) & 1 == 1)

    @property
    def written_bits(self) -> int:
        """Exact number of bits written, excluding any padding."""
        return len(self._buf) * 8 + self._n

    def snapshot(self) -> bytes:
        """Return the packed stream **without mutating the writer**.

        This exists because of a bug worth knowing about. Flushing pads the
        partial byte and resets the bit counter, which is correct once at the
        end of a block and corrupts everything in the middle of one -- and a
        live block being appended to is always in the middle. A reader that
        wants to inspect such a stream must not flush it, so ``snapshot`` pads
        into a copy instead.
        """
        if self._n == 0:
            return bytes(self._buf)
        return bytes(self._buf) + bytes([(self._cur << (8 - self._n)) & 0xFF])

    def tobytes(self) -> bytes:
        """Finalise the stream and return it.

        After this the writer must not be appended to: the padding byte is now
        part of the buffer, so further writes would land after it.
        """
        if self._n:
            self._buf.append((self._cur << (8 - self._n)) & 0xFF)
            self._cur = 0
            self._n = 0
        return bytes(self._buf)

    def __len__(self) -> int:
        return len(self._buf)


class BitReader:
    """Reads bits back out of a packed byte string."""

    __slots__ = ("_data", "_pos", "_total_bits")

    def __init__(self, data: bytes | bytearray | memoryview) -> None:
        self._data = data
        self._pos = 0
        self._total_bits = len(data) * 8

    @property
    def bits_left(self) -> int:
        return self._total_bits - self._pos

    def read_bit(self) -> bool:
        if self._pos >= self._total_bits:
            raise EOFError("read past end of bit stream")
        byte = self._data[self._pos >> 3]
        bit = (byte >> (7 - (self._pos & 7))) & 1
        self._pos += 1
        return bit == 1

    def read_uint(self, nbits: int) -> int:
        if not 0 <= nbits <= 64:
            raise ValueError(f"nbits must be in [0, 64], got {nbits}")
        value = 0
        for _ in range(nbits):
            value = (value << 1) | (1 if self.read_bit() else 0)
        return value

    def align(self) -> None:
        """Skip to the next byte boundary.

        Block headers are byte-aligned but the encoded samples that follow them
        are not, so a reader needs to resynchronise once after the header.
        """
        rem = self._pos & 7
        if rem:
            self._pos += 8 - rem


def zigzag(value: int) -> int:
    """Map a signed integer onto an unsigned one.

    Small magnitudes, positive or negative, end up as small unsigned values,
    which is what makes variable-width encoding pay off for deltas that swing
    either side of zero.
    """
    return (value << 1) ^ (value >> 63)


def unzigzag(value: int) -> int:
    """Inverse of :func:`zigzag`."""
    result = value >> 1
    return result if not value & 1 else ~result

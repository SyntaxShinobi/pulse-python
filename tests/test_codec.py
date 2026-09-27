"""Tests for the compression codec.

These carry over every case that caught a real bug in the Go implementation,
plus the ones that are easy to get wrong in Python specifically (int
overflow semantics, float bit patterns, -0.0).
"""

from __future__ import annotations

import math
import random
import struct

import pytest

from pulse.bitstream import BitReader, BitWriter, unzigzag, zigzag
from pulse.codec import Codec, CodecDecoder


def roundtrip(samples: list[tuple[int, float]]) -> list[tuple[int, float]]:
    """Encode then decode, returning what came back."""
    codec = Codec(samples[0][0])
    for ts, value in samples:
        codec.append(ts, value)
    decoder = CodecDecoder(codec.payload(), len(samples))
    return list(decoder)


# --- bitstream ---------------------------------------------------------------


def test_bitwriter_packs_msb_first():
    w = BitWriter()
    w.write_uint(0b101, 3)
    w.write_bit(True)
    w.write_bit(False)
    w.write_uint(255, 8)
    w.write_uint(1, 1)
    # 101 1 0 | 111 11111 | 1 -> 0b10110111 0b11111100
    assert w.snapshot() == bytes([0b10110111, 0b11111100])
    assert w.written_bits == 14


def test_bitwriter_reader_roundtrip():
    w = BitWriter()
    values = [(0b101, 3), (255, 8), (0, 1), (1, 1), (12345, 16), ((1 << 64) - 1, 64)]
    for value, nbits in values:
        w.write_uint(value, nbits)
    reader = BitReader(w.tobytes())
    for value, nbits in values:
        assert reader.read_uint(nbits) == value


@pytest.mark.parametrize("nbits", range(1, 65))
def test_all_widths_survive_roundtrip(nbits: int):
    value = (1 << nbits) - 1
    w = BitWriter()
    w.write_uint(value, nbits)
    assert BitReader(w.tobytes()).read_uint(nbits) == value


def test_reader_raises_past_end():
    reader = BitReader(b"\xff")
    with pytest.raises(EOFError):
        reader.read_uint(9)


def test_random_roundtrip():
    rng = random.Random(7)
    fields = [(rng.getrandbits(n), n) for n in (rng.randint(1, 64) for _ in range(500))]
    w = BitWriter()
    for value, nbits in fields:
        w.write_uint(value, nbits)
    reader = BitReader(w.tobytes())
    for value, nbits in fields:
        assert reader.read_uint(nbits) == value


def test_snapshot_does_not_mutate_writer():
    """The invariant the encode-between-appends fix rests on."""
    w = BitWriter()
    w.write_uint(0b10101, 5)
    before = w.written_bits
    for _ in range(5):
        w.snapshot()
    assert w.written_bits == before
    w.write_uint(0b111, 3)
    assert BitReader(w.tobytes()).read_uint(8) == 0b10101111


@pytest.mark.parametrize(
    ("value", "expected"),
    [(0, 0), (-1, 1), (1, 2), (-2, 3), (2, 4), (-64, 127), (64, 128)],
)
def test_zigzag(value: int, expected: int):
    assert zigzag(value) == expected
    assert unzigzag(expected) == value


def test_zigzag_extremes():
    for value in (0, 1, -1, 2**40, -(2**40), 2**62, -(2**62)):
        assert unzigzag(zigzag(value)) == value


# --- codec round-trips -------------------------------------------------------


def test_regular_series_roundtrip():
    base = 1_700_000_000
    samples = [(base + i * 10, 42.5 + (i % 7) * 0.25) for i in range(1000)]
    assert roundtrip(samples) == samples


def test_constant_series_roundtrip():
    base = 1_700_000_000
    samples = [(base + i, 3.14) for i in range(500)]
    assert roundtrip(samples) == samples


def test_single_sample():
    assert roundtrip([(1_700_000_000, 7.0)]) == [(1_700_000_000, 7.0)]


def test_irregular_and_extreme_values():
    steps = [0, 1, 2, 60, 60, 61, 59, 3600, -3599, 0, 86400, 86400]
    values = [
        0.0,
        -0.0,
        1e300,
        -1e-300,
        sys_max_float(),
        sys_min_float(),
        0.1,
        0.2,
        123456789.987654321,
        -98765.4321,
        0.0,
        42.0,
    ]
    samples = []
    ts = 1_700_000_000
    for step, value in zip(steps, values, strict=True):
        ts += step
        samples.append((ts, value))

    got = roundtrip(samples)
    assert len(got) == len(samples)
    for (want_t, want_v), (got_t, got_v) in zip(samples, got, strict=True):
        assert got_t == want_t
        # Compare bit patterns so -0.0 and 0.0 are distinguished.
        assert struct.pack(">d", got_v) == struct.pack(">d", want_v)


def sys_max_float() -> float:
    return float(0x1FFFFFFFFFFFFF) * 2.0**971  # math.ldexp form of MaxFloat64


def sys_min_float() -> float:
    return 5e-324  # smallest positive subnormal double


def test_negative_zero_is_preserved():
    got = roundtrip([(1_700_000_000, 0.0), (1_700_000_001, -0.0)])
    assert struct.pack(">d", got[1][1]) == struct.pack(">d", -0.0)


def test_nan_and_inf_roundtrip():
    samples = [
        (1_700_000_000, 1.0),
        (1_700_000_001, float("nan")),
        (1_700_000_002, float("inf")),
        (1_700_000_003, float("-inf")),
        (1_700_000_004, 1.0),
    ]
    got = roundtrip(samples)
    assert math.isnan(got[1][1])
    assert got[2][1] == float("inf")
    assert got[3][1] == float("-inf")
    assert got[0][1] == got[4][1] == 1.0


def test_large_clock_jump_uses_64bit_branch():
    samples = [(0, 1.0), (1, 1.0), (1 << 40, 1.0)]
    assert [t for t, _ in roundtrip(samples)] == [0, 1, 1 << 40]


def test_timestamps_beyond_2038():
    """Second-resolution timestamps past the 32-bit epoch boundary."""
    base = 2**31 + 12345
    samples = [(base + i * 60, float(i)) for i in range(100)]
    assert roundtrip(samples) == samples


def test_random_roundtrip_many_shapes():
    """Fuzz the codec across several data shapes at once."""
    rng = random.Random(1234)
    for trial in range(20):
        n = rng.randint(1, 200)
        ts = 1_700_000_000
        samples = []
        for _ in range(n):
            ts += rng.choice([0, 1, 1, 10, 10, 60, 61, 3600, 86400])
            samples.append((ts, rng.choice([0.0, 1.0, -1.0, rng.uniform(-1e6, 1e6)])))
        assert roundtrip(samples) == samples, f"trial {trial} failed"


# --- compression ratio -------------------------------------------------------


def integer_series(n: int, seed: int = 42) -> list[tuple[int, float]]:
    """Whole-number gauge or slowly ticking counter: values repeat often."""
    rng = random.Random(seed)
    base = 1_700_000_000
    value = 200.0
    out = []
    for i in range(n):
        if rng.random() < 0.7:
            value += rng.randint(0, 4)
        out.append((base + i, value))
    return out


def drifting_series(n: int, seed: int = 42) -> list[tuple[int, float]]:
    """A float that moves every sample: high-entropy XOR."""
    rng = random.Random(seed)
    base = 1_700_000_000
    level = 30.0
    out = []
    for i in range(n):
        level += (rng.random() - 0.5) * 0.4
        level = max(0.0, min(100.0, level))
        out.append((base + i, level))
    return out


def random_series(n: int, seed: int = 42) -> list[tuple[int, float]]:
    """Adversarial: every value is a fresh full-precision double."""
    rng = random.Random(seed)
    base = 1_700_000_000
    return [(base + i, rng.getrandbits(53) / float(1 << 41)) for i in range(n)]


def encoded_size(samples: list[tuple[int, float]]) -> int:
    codec = Codec(samples[0][0])
    for ts, value in samples:
        codec.append(ts, value)
    return len(codec.finalize())


@pytest.mark.parametrize(
    ("name", "gen", "max_bytes_per_sample"),
    [
        # Counters and whole-number gauges compress hard.
        ("repeating_integers", integer_series, 3.0),
        # Timestamps still win; the value side barely pays for itself.
        ("drifting_floats", drifting_series, 12.0),
        # Full entropy: still under raw 16 B/sample, but a long way from the
        # paper's 1.37.
        ("worst_case_random", random_series, 11.0),
    ],
)
def test_compression_ratio(
    name: str, gen, max_bytes_per_sample: float, capsys
):
    n = 10_000
    samples = gen(n)
    size = encoded_size(samples)
    raw = n * 16
    per_sample = size / n
    with capsys.disabled():
        print(
            f"\n{name:<20} {n:>6} samples -> {size:>8} bytes | "
            f"{per_sample:5.2f} B/sample | {per_sample * 8:5.2f} bits/sample | "
            f"{raw / size:5.2f}x vs raw 16 B/sample"
        )
    assert per_sample <= max_bytes_per_sample, f"{name}: layout regression?"
    assert size < raw


def test_constant_series_costs_about_two_bits_per_sample():
    """1 bit for dod == 0 plus 1 bit for xor == 0."""
    base = 1_700_000_000
    codec = Codec(base)
    for i in range(101):
        codec.append(base + i * 10, 1.0)
    # 128 bits for sample 0, 12 for sample 1 (1-bit dod plus the one-off
    # 11-bit window header on the first non-zero XOR), 2 per sample after.
    assert codec.written_bits == 128 + 12 + 99 * 2


@pytest.mark.parametrize(
    ("dod", "expected_extra_bits"),
    [
        # The branch is chosen by the width of the *zigzagged* value, so
        # zigzag ~= 2*|dod| and 0xFFF is 4095, not 40000.
        #
        # Each sample costs prefix + width + 1 here (the +1 is the
        # constant-value bit) and 1 bit in the baseline, so the difference is
        # prefix + width. Written as fully parenthesised tuples:
        # `(-100, 2 + 9)` parses as `((-100, 2) + 9)`, which silently
        # duplicated the row above it.
        (0, 0),
        (100, (2 + 9) - 1),  # zigzag 200  <= 0xFF
        (-100, (2 + 9) - 1),  # zigzag 199  <= 0xFF
        (150, (3 + 12) - 1),  # zigzag 300  <= 0xFFF
        (2000, (3 + 12) - 1),  # zigzag 4000 <= 0xFFF
        (3000, (4 + 32) - 1),  # zigzag 6000 > 0xFFF
        (2_000_000_000, (4 + 32) - 1),
        (3_000_000_000, (4 + 64) - 1),  # zigzag 6e9 > 0xFFFFFFFF
        ((1 << 40), (4 + 64) - 1),
    ],
)
def test_timestamp_branch_cost(dod: int, expected_extra_bits: float):
    """Pins the exact cost of each delta-of-delta branch.

    Measured by difference against a dod == 0 baseline rather than by
    arithmetic on the layout, so a width change fails here instead of
    silently bloating block files.
    """
    if dod == 0:
        assert expected_extra_bits == 0
        return

    n = 6
    base = 1_700_000_000

    def timestamps_for(d: int) -> list[int]:
        """A constant delta-of-delta means the step grows linearly.

        step_i = 10 + (i-1)*d, so delta_i = 10 + i*d and dod_i = d for every
        sample from 1 onward. Sample 0 anchors the series.
        """
        out = [base]
        for i in range(1, n):
            out.append(out[-1] + 10 + (i - 1) * d)
        return out

    def costs(timestamps: list[int]) -> list[int]:
        """Per-sample encoded size, in bits."""
        codec = Codec(base)
        out = []
        prev = 0
        for ts in timestamps:
            codec.append(ts, 1.0)  # constant value: identical cost in both
            out.append(codec.written_bits - prev)
            prev = codec.written_bits
        return out

    zero_costs = costs(timestamps_for(0))
    dod_costs = costs(timestamps_for(dod))

    # Samples 0 and 1 are excluded: sample 0 is stored whole, and sample 1 is
    # where the one-off 11-bit window header lands on the first non-zero XOR,
    # which has nothing to do with the timestamp branch.
    diffs = [d - z for d, z in zip(dod_costs[2:], zero_costs[2:], strict=True)]
    assert len(set(diffs)) == 1, f"branch cost is not constant: {diffs}"
    assert diffs[0] == expected_extra_bits


# --- the encode-between-appends bug -----------------------------------------


def test_encode_between_appends_does_not_corrupt():
    """The nastiest bug the Go original had.

    Reading the codec used to flush it, which appends a padding byte and
    resets the bit counter. A live block is read on every query, so each read
    spliced a pad byte into the middle of the stream and every sample written
    afterwards landed in the wrong place -- sealed into immutable files as
    plausible-looking wrong values, never an error.
    """
    base = 1_700_000_000
    values = [0.5, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5]

    def encode(interleave: bool) -> list[float]:
        codec = Codec(base)
        for i, value in enumerate(values):
            codec.append(base + i, value)
            if interleave:
                CodecDecoder(codec.payload(), i + 1)  # a query landing mid-write
        return [v for _, v in CodecDecoder(codec.finalize(), len(values))]

    baseline = encode(interleave=False)
    assert baseline == values
    assert encode(interleave=True) == baseline


def test_payload_is_stable_across_repeated_reads():
    codec = Codec(1_700_000_000)
    codec.append(1_700_000_000, 1.5)
    codec.append(1_700_000_010, 2.5)
    first = codec.payload()
    for _ in range(5):
        assert codec.payload() == first
    codec.append(1_700_000_020, 3.5)
    assert list(CodecDecoder(codec.payload(), 3)) == [
        (1_700_000_000, 1.5),
        (1_700_000_010, 2.5),
        (1_700_000_020, 3.5),
    ]

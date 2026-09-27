"""Measure Pulse end to end.

    python -m pulse.cli.tsdbbench --samples 100000

Reports encode/decode throughput, compression ratio per data shape, ingest
throughput through the full write path (intern -> WAL -> fsync -> head block),
and query latency.

Every number is measured on the machine it runs on. The point of the tool is
that the figures are reproducible, not that they are impressive: Python is
interpreted and the codec is a per-sample bit loop, so this is orders of
magnitude slower than a Go or Rust equivalent, and the honest number is the
interesting one.
"""

from __future__ import annotations

import argparse
import math
import random
import shutil
import statistics
import sys
import tempfile
import time

from pulse.block import BLOCK_SPAN_SECONDS
from pulse.codec import Codec, CodecDecoder
from pulse.engine import DB
from pulse.index import parse_selector
from pulse.query import Query

__all__ = ["SHAPES", "bench_codec", "bench_ingest", "bench_query", "main"]

#: Raw baseline: an int64 timestamp plus a float64 value.
RAW_BYTES_PER_SAMPLE = 16


# ----------------------------------------------------------------- shapes


def repeating_integers(n: int, rng: random.Random) -> list[tuple[int, float]]:
    """Counters and gauges: the same whole number repeated for long runs.

    The dominant shape in real metrics, and the one the Gorilla paper's
    1.37 B/sample is measured on.
    """
    base = 1_700_000_000
    out: list[tuple[int, float]] = []
    value = float(rng.randint(0, 500))
    for i in range(n):
        if rng.random() < 0.15:
            value = float(rng.randint(0, 500))
        out.append((base + i, value))
    return out


def drifting_floats(n: int, rng: random.Random) -> list[tuple[int, float]]:
    """A bounded walk: the value changes on every sample, by a little."""
    base = 1_700_000_000
    out: list[tuple[int, float]] = []
    value = 50.0
    for i in range(n):
        value += rng.gauss(0, 0.4) + (50.0 - value) * 0.02
        out.append((base + i, value))
    return out


def worst_case_random(n: int, rng: random.Random) -> list[tuple[int, float]]:
    """Full-entropy values on irregular timestamps: the codec's floor."""
    base = 1_700_000_000
    out: list[tuple[int, float]] = []
    t = base
    for _ in range(n):
        t += rng.randint(1, 5)
        out.append((t, rng.uniform(-1e6, 1e6)))
    return out


SHAPES = {
    "repeating_integers": repeating_integers,
    "drifting_floats": drifting_floats,
    "worst_case_random": worst_case_random,
}


# -------------------------------------------------------------- benchmarks


def bench_codec(n: int, seed: int) -> None:
    print(f"\n== codec ({n:,} samples per shape) ==")
    print(
        f"{'shape':<22}{'encoded':>12}{'B/sample':>10}{'bits':>8}{'ratio':>8}"
        f"{'enc/s':>14}{'dec/s':>14}"
    )

    for name, gen in SHAPES.items():
        samples = gen(n, random.Random(seed))

        start = time.perf_counter()
        codec = Codec(samples[0][0])
        for t, v in samples:
            codec.append(t, v)
        payload = codec.finalize()
        encode_s = time.perf_counter() - start

        start = time.perf_counter()
        decoded = list(CodecDecoder(payload, len(samples)))
        decode_s = time.perf_counter() - start

        assert len(decoded) == n, f"{name}: decoded {len(decoded)} of {n}"
        for (et, ev), (dt, dv) in zip(samples, decoded, strict=True):
            assert et == dt, f"{name}: timestamp mismatch"
            assert ev == dv or (math.isnan(ev) and math.isnan(dv)), f"{name}: value mismatch"

        per_sample = len(payload) / n
        print(
            f"{name:<22}{len(payload):>12,}{per_sample:>10.2f}{per_sample * 8:>8.2f}"
            f"{RAW_BYTES_PER_SAMPLE / per_sample:>7.2f}x"
            f"{n / encode_s:>14,.0f}{n / decode_s:>14,.0f}"
        )


def bench_ingest(n: int, series: int, batches: list[int], seed: int) -> None:
    """Full write path, swept across batch sizes.

    The batch sweep is the interesting result: ingest does one ``fsync`` per
    batch, so the syscall is amortised over the whole batch. Sweeping shows the
    cost of that syscall directly, which is what justifies the "one fsync per
    batch, not per sample" design -- and it is a measured claim rather than an
    assertion in a comment.
    """
    print(f"\n== ingest ({n:,} samples, {series} series) ==")
    base = int(time.time()) - 3600

    # Pre-generate one workload so every batch size writes identical data.
    rng = random.Random(seed)
    names = [f'cpu_usage{{host="h{i}",job="bench"}}' for i in range(series)]
    per_series = max(max(batches), n // series)
    workload: list[tuple[str, list[int], list[float]]] = []
    for name in names:
        value = 50.0
        for chunk in range(0, per_series, max(batches)):
            size = min(max(batches), per_series - chunk)
            ts = [base + chunk + i for i in range(size)]
            vs: list[float] = []
            for _ in range(size):
                value += rng.gauss(0, 0.3) + (50.0 - value) * 0.02
                vs.append(value)
            workload.append((name, ts, vs))
    rng.shuffle(workload)

    print(f"  {'batch':>8}{'samples':>10}{'seconds':>10}{'samples/s':>12}{'syncs':>9}{'B/sample':>10}")
    results: dict[int, float] = {}

    for batch in sorted(batches):
        directory = tempfile.mkdtemp(prefix="pulse-bench-")
        try:
            db = DB(directory)
            written = 0
            syncs = 0
            start = time.perf_counter()
            for name, ts, vs in workload:
                for chunk in range(0, len(ts), batch):
                    db.write(name, ts[chunk : chunk + batch], vs[chunk : chunk + batch])
                    written += len(ts[chunk : chunk + batch])
                    syncs += 1
            elapsed = time.perf_counter() - start

            db.flush()
            stats = db.stats()
            per_sample = stats["bytes_on_disk"] / stats["total_samples"] if stats["total_samples"] else 0.0
            rate = written / elapsed
            results[batch] = rate
            print(
                f"  {batch:>8}{written:>10,}{elapsed:>10.3f}{rate:>12,.0f}"
                f"{stats['wal_syncs']:>9,}{per_sample:>10.2f}"
            )
            db.close()
        finally:
            shutil.rmtree(directory, ignore_errors=True)

    if len(results) > 1:
        ordered = sorted(results)
        slowest, fastest = ordered[0], ordered[-1]
        print(
            f"\n  batch {slowest} -> {fastest}: {results[slowest]:,.0f} -> {results[fastest]:,.0f} samples/s "
            f"({results[fastest] / results[slowest]:.1f}x)"
        )
        print("  That ratio is the fsync, amortised. Batching is why ingest is viable in Python.")


def _raw_of(q: Query) -> Query:
    """The same query with downsampling off, to count the samples scanned."""
    return Query(metric=q.metric, labels=q.labels, start=q.start, end=q.end, step=0, agg="avg")


def bench_query(n: int, seed: int) -> None:
    """Query latency over a sealed corpus, raw and downsampled."""
    print("\n== query ==")
    directory = tempfile.mkdtemp(prefix="pulse-bench-")
    try:
        db = DB(directory)
        base = int(time.time()) - BLOCK_SPAN_SECONDS
        rng = random.Random(seed)
        selectors = [f'cpu_usage{{host="h{i}"}}' for i in range(20)]
        for name in selectors:
            value = 50.0
            ts: list[int] = []
            vs: list[float] = []
            for i in range(n):
                value += rng.gauss(0, 0.3) + (50.0 - value) * 0.02
                ts.append(base + i)
                vs.append(value)
            db.write(name, ts, vs)
        db.flush()
        print(f"  corpus         {db.stats()['total_samples']:,} samples across {len(selectors)} series")

        cases = [
            ("all series, raw", Query(metric="cpu_usage", start=base, end=base + n, step=0, agg="avg")),
            ("all series, step=60", Query(metric="cpu_usage", start=base, end=base + n, step=60, agg="avg")),
            (
                "one series, raw",
                Query(metric="cpu_usage", start=base, end=base + n, step=0, agg="avg", labels={"host": "h0"}),
            ),
            (
                "one series, 5 min",
                Query(
                    metric="cpu_usage",
                    start=base + n - 300,
                    end=base + n,
                    step=0,
                    agg="avg",
                    labels={"host": "h0"},
                ),
            ),
        ]
        # "scanned" is the work actually done -- every sample decoded and
        # filtered -- while "returned" is what the caller receives after
        # downsampling. Reporting only the latter would make a step=60 query
        # look 60x cheaper than the same query raw, when it decodes identically.
        print(f"  {'case':<24}{'scanned':>10}{'returned':>10}{'ms':>10}{'scan/ms':>10}")
        for label, q in cases:
            runs: list[float] = []
            scanned = returned = 0
            for _ in range(5):
                start = time.perf_counter()
                results = db.query(q)
                runs.append((time.perf_counter() - start) * 1000)
                scanned = sum(len(r.points) for r in db.query(_raw_of(q)))
                returned = sum(len(r.points) for r in results)
            median = statistics.median(runs)
            print(f"  {label:<24}{scanned:>10,}{returned:>10,}{median:>10.1f}{scanned / median:>10,.0f}")
        db.close()
    finally:
        shutil.rmtree(directory, ignore_errors=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Pulse benchmark")
    parser.add_argument("--samples", type=int, default=100_000, help="samples per shape (codec)")
    parser.add_argument("--ingest-samples", type=int, default=100_000)
    parser.add_argument("--series", type=int, default=20)
    parser.add_argument("--batches", type=int, nargs="*", default=[1, 10, 100, 1000],
                        help="batch sizes to sweep for the ingest bench")
    parser.add_argument("--query-samples", type=int, default=20_000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--skip", nargs="*", default=[], choices=["codec", "ingest", "query"])
    args = parser.parse_args(argv)

    print(f"Pulse benchmark — Python {sys.version.split()[0]}")
    print(f"raw baseline is {RAW_BYTES_PER_SAMPLE} B/sample (int64 ts + float64 value)")

    if "codec" not in args.skip:
        bench_codec(args.samples, args.seed)
    if "ingest" not in args.skip:
        bench_ingest(args.ingest_samples, args.series, args.batches, args.seed)
    if "query" not in args.skip:
        bench_query(args.query_samples, args.seed)

    print("\nReproduce with the same flags; numbers move with CPU and disk.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

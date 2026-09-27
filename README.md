# Pulse

A self-hosted time-series metrics engine in Python: ingest, compressed storage,
range queries, threshold alerting, and a live dashboard.

It implements the compression scheme from Facebook's **Gorilla** paper
(Pelkonen et al., VLDB 2015) — delta-of-delta timestamps and XOR-encoded floats,
bit-packed — behind a write-ahead log, an immutable block layout, a label index,
and an HTTP API that speaks the Prometheus exposition format.

This is not a wrapper around an existing database. The bit-level codec, the WAL
framing, the block format, the inverted label index, the range-query planner and
the alerting engine are all implemented here, and all of them are tested.

---

## Quick start

```bash
pip install -e ".[dev]"

# 1. start the server
python -m pulse.cli.server --data ./pulse-data --port 8080

# 2. in another terminal, generate load
python -m pulse.cli.loadgen --url http://localhost:8080 --hosts 8 --interval 1
```

Open <http://localhost:8080/> for the dashboard, or
<http://localhost:8080/api/docs> for the interactive OpenAPI schema.

Run the tests:

```bash
python -m pytest            # 163 tests
```

Measure it:

```bash
python -m pulse.cli.tsdbbench
```

---

## What it does

**Ingest.** `POST /api/write` accepts the Prometheus text exposition format.
Samples are grouped by series, then written as one WAL record per series and
fsynced *before* the request is acknowledged. An acknowledged write survives
`kill -9`.

**Storage.** Samples accumulate in an in-memory head block covering a 2-hour
window. When the window rolls over — or the process shuts down cleanly — the
block is encoded, appended to an immutable `block_<base>.blk` file, and fsynced.
The head is small and mutable so appends are cheap; the tail is immutable and
compressed so reads are sequential.

**Index.** Every distinct label set is one series with a stable integer ID. The
index maps both directions: name → ID for ingest, and label value → IDs for
queries, so `cpu{host=~"web.*"}` resolves without a scan.

**Query.** `GET /api/query?metric=cpu_usage&start=&end=&step=60&agg=avg` scans
the head plus every sealed block overlapping the range, filters to the window,
and reduces each epoch-aligned bucket.

**Alerting.** Threshold rules on a rolling window, evaluated on an interval,
edge-triggered so a condition that stays true fires once rather than every tick.

**Live feed.** `WS /api/live` (or `GET /api/stream` for SSE) pushes samples and
alerts to the dashboard as they land.

---

## Compression

A sample is an `int64` timestamp and a `float64` value: **16 bytes raw**. Gorilla
encodes the *difference* between consecutive samples instead of the samples
themselves:

- **Timestamps** — the delta-of-delta. Series are scraped on a fixed interval,
  so the delta is nearly constant and the delta-of-delta is nearly always zero,
  costing 1 bit. The paper reports this holds for 96% of production samples.
- **Values** — the XOR against the previous value. Similar values share most of
  their bits, so the XOR is mostly zeros; only the meaningful window is stored.
  The paper reports 59% of production values XOR to exactly zero, costing 1 bit.

Measured with `python -m pulse.cli.tsdbbench` (100,000 samples per shape,
Python 3.13):

| Data shape            | Encoded     | B/sample | vs 16 B raw |
| --------------------- | ----------- | -------- | ----------- |
| `repeating_integers`  | 83,406 B    | **0.83** | **19.2x**   |
| `drifting_floats`     | 824,881 B   | **8.25** | 1.9x        |
| `worst_case_random`   | 1,075,207 B | **10.75** | 1.5x       |

**The ratio is a property of the data, not of the codec.** Counters and gauges —
the dominant shape in real metrics — collapse to well under a byte per sample.
Full-entropy random data barely compresses at all, because there is no
redundancy to remove; 1.5x there is the honest floor, not a failure.

The Gorilla paper's headline **1.37 bytes/sample** is measured on production
metrics dominated by repeating whole numbers. It is not a number this
implementation claims to reproduce on arbitrary data, and the benchmark
deliberately includes a shape that fails to compress so the contrast is visible.

### Why not numpy?

I measured it. Vectorising the aggregation with `np.add.reduceat` is ~13x faster
than the scalar bucketing loop — but 89% of that win is spent copying decoded
Python objects into arrays, because the decoder yields tuples. Net: 1.5x on the
whole query path, and floating-point summation order changes the result. That is
not worth a hard dependency, so `numpy` is not one.

---

## Performance

`python -m pulse.cli.tsdbbench`, Python 3.13, single core, container filesystem.
Reproduce with the same flags; these move with CPU and disk.

**Codec** (100,000 samples per shape):

| Shape                | Encode      | Decode     |
| -------------------- | ----------- | ---------- |
| `repeating_integers` | 514k samp/s | 406k samp/s |
| `drifting_floats`    | 69k samp/s  | 52k samp/s  |
| `worst_case_random`  | 60k samp/s  | 43k samp/s  |

**Ingest** (100,000 samples, 20 series) — swept across batch sizes, because one
`fsync` happens per batch:

| Batch size | Throughput  | fsyncs   |
| ---------- | ----------- | -------- |
| 1          | 30,192/s    | 100,000  |
| 10         | 48,761/s    | 10,000   |
| 100        | 62,741/s    | 1,000    |
| 1000       | 55,831/s    | 100      |

Batching is worth ~2x, and the ratio *is* the fsync being amortised. This is why
`POST /api/write` groups a scrape payload into one record per series rather than
writing sample by sample.

**Query** (400,000-sample corpus, 20 series):

| Case                | Scanned | Returned | Latency |
| ------------------- | ------- | -------- | ------- |
| All series, raw     | 400,000 | 400,000  | 8,288 ms |
| All series, step=60 | 400,000 | 6,700    | 7,818 ms |
| One series, raw     | 20,000  | 20,000   | 357 ms  |
| One series, 5 min   | 300     | 300      | 46 ms   |

Scan rate is ~50k samples/s regardless of downsampling, because aggregation
still has to decode everything in range. The dashboard queries short windows,
which is why it feels instant; a 400k-point full-range scan does not.

**Python is slow here, and that is the honest finding.** The codec is a
per-sample bit loop in an interpreted language. A Go implementation of the same
design encoded ~3.4M samples/s — roughly 50x faster. The compression ratio and
the durability guarantees are identical; only the throughput differs. If
throughput were the requirement, this would be written in Go or Rust, or the
codec would be a C extension.

---

## Design notes

Full architectural rationale and internal invariants are documented in docs/DESIGN.md.

The short version:

**Durability.** A batch is fsynced to the WAL before it is acknowledged.
`flush()` seals the head but leaves the WAL alone; `compact()` seals *and then*
truncates, and it is safe because sealing fsyncs first. Recovery drops any WAL
sample at or before a series' sealed high-water mark — without that guard, a
crash between sealing and truncating silently doubles the data on restart.

**Recovery counts what it restored.** A restarted server that reports zero
samples while drawing a full chart is a bug that looks like a rendering problem
and isn't.

**The live feed drops slow clients.** Each WebSocket gets a bounded queue; a
client that falls behind is disconnected and counted, rather than allowed to
grow server memory without limit.

**A read that mutates is a trap.** `Codec.payload()` returns a non-mutating
snapshot. The obvious implementation — a `bytes()` that finalises the bit
buffer as a side effect — corrupts every sample written after the first read,
because finalising pads the partial byte. That bug occurred in an earlier prototype, and the regression test for it is enforced in tests/test_codec.py.

---

## Layout

```
pulse/
  bitstream.py   MSB-first bit reader/writer, zigzag
  codec.py       Gorilla encoder/decoder
  block.py       2h block container + on-disk format
  wal.py         framed, CRC32-checked write-ahead log
  index.py       series interning + inverted label index
  query.py       range queries, downsampling, aggregations
  engine.py      head/tail storage engine, recovery
  rules.py       threshold alerting
  api.py         FastAPI routes, WebSocket/SSE hub
  static/        dashboard
  cli/           server, loadgen, tsdbbench
tests/           163 tests
docs/            DESIGN.md
```

## API

| Method   | Path                  | Purpose                                   |
| -------- | --------------------- | ----------------------------------------- |
| `GET`    | `/healthz`            | Liveness                                  |
| `GET`    | `/api/stats`          | Engine, WAL, rules and live-feed counters |
| `GET`    | `/api/series`         | Every known series with its labels        |
| `POST`   | `/api/write`          | Ingest, Prometheus exposition format      |
| `GET`    | `/api/query`          | Range query (`metric`, `start`, `end`, `step`, `agg`, `label_*`) |
| `GET`    | `/api/rules`          | List alert rules                          |
| `POST`   | `/api/rules`          | Create an alert rule                      |
| `DELETE` | `/api/rules/{id}`     | Delete an alert rule                      |
| `POST`   | `/api/rules/check`    | Evaluate all rules now                    |
| `GET`    | `/api/alerts`         | Recent firings                            |
| `WS`     | `/api/live`           | Live samples and alerts                   |
| `GET`    | `/api/stream`         | The same feed over SSE                    |

Aggregations: `avg`, `max`, `min`, `sum`, `last`, `first`, `count`.

Set `PULSE_API_KEY` to require a key (via `X-Api-Key` or `Authorization: Bearer`)
on writes and rule changes. Reads stay open.

```bash
curl -X POST localhost:8080/api/write \
  -d 'cpu_usage{host="web-1",job="api"} 73.5'

curl 'localhost:8080/api/query?metric=cpu_usage&step=60&agg=avg&label_host=web-1'
```

## Limitations

Deliberate, and worth being able to state plainly:

- **Single node.** No replication, no clustering, no consensus.
- **Whole blocks in memory.** Sealed blocks are read into RAM; past ~1M blocks
  this needs mmap.
- **Second-resolution timestamps.** Sub-second sampling is not supported.
- **No downsampling tiers.** Raw blocks only; there is no 1s → 1m → 1h rollup.
- **Queries scan every block in range.** There is no per-block min/max skipping
  beyond the timestamp overlap check.

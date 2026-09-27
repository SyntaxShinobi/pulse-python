# Design

Why Pulse is built the way it is. Each section states the decision, the
alternative it was chosen over, and what would have to change to revisit it.

---

## 1. Compression: delta-of-delta and XOR

### The problem

A metric sample is an `int64` timestamp and a `float64` value — 16 bytes. A
single host reporting 20 metrics once a second produces 27 MB/day of raw
samples, and almost all of it is redundant: the timestamps advance by a constant
interval and the values barely move.

### The scheme

Gorilla (Pelkonen et al., VLDB 2015) encodes the *difference* between consecutive
samples rather than the samples.

**Timestamps — delta-of-delta.** Store `dod = (t_i - t_{i-1}) - (t_{i-1} - t_{i-2})`.
A series scraped every 10s has a constant delta, so `dod` is 0. The encoding is
a variable-width prefix:

| `zigzag(dod)` | Prefix  | Bits stored | Total |
| ------------- | ------- | ----------- | ----- |
| `= 0`         | `0`     | 0           | 1     |
| `≤ 0xFF`      | `10`    | 9           | 11    |
| `≤ 0xFFF`     | `110`   | 12          | 15    |
| `≤ 0xFFFFFFFF`| `1110`  | 32          | 36    |
| otherwise     | `1111`  | 64          | 68    |

Values are zigzag-encoded first so small negatives cost the same as small
positives. The thresholds are on the *zigzagged* value, so `0xFFF` is 4095 — not
40000, which is what the raw delta would suggest.

**Values — XOR.** Store `xor = bits(v_i) ^ bits(v_{i-1})`. Similar floats share
their sign, exponent and high mantissa bits, so the XOR is mostly zeros. If it
is entirely zero, one bit says so. Otherwise a 5-bit leading-zero count and a
6-bit length describe the meaningful window, followed by the window itself.

The window is *reused* when the new XOR's significant bits fit inside the
previous window — the common case for a slowly drifting value. That reuse branch
is where a subtle bug hides: the encoder and decoder must agree on the window,
and a decoder that recomputes leading zeros instead of reading the stored count
silently mis-decodes. See `TALKING_POINTS.md`.

### Why 2-hour blocks

Blocks are 2 hours wide and capped at 8192 samples. Gorilla's own measurements
show compression gains flattening past roughly two hours, while read amplification
keeps growing: to serve a 5-minute query you must decode the whole block. Two
hours is the point where those curves cross.

At second resolution the 8192-sample cap can never actually bind — a 2h window
holds 7200 distinct seconds — so it is a backstop for pathological writers, not
the operative limit. `tests/test_engine.py::test_sample_cap_is_a_backstop_behind_the_window`
pins that relationship so it cannot silently invert.

### What it achieves

Measured on 100,000 samples per shape (`python -m pulse.cli.tsdbbench`):

| Shape               | B/sample | Ratio |
| ------------------- | -------- | ----- |
| `repeating_integers`| 0.83     | 19.2x |
| `drifting_floats`   | 8.25     | 1.9x  |
| `worst_case_random` | 10.75    | 1.5x  |

The spread is the point. **Compression ratio is a property of the data.** The
paper's 1.37 B/sample comes from production metrics dominated by repeating whole
numbers; random data has no redundancy to remove and 1.5x is the honest floor.
Any benchmark that reports only the good shape is measuring its own generator.

---

## 2. Storage: mutable head, immutable tail

```
POST /api/write
      |
      v
  intern series ----> SeriesIndex  (name -> int ID, label -> IDs)
      |
      v
  WAL append + fsync            <- acknowledged after this
      |
      v
  head Block (2h window)        <- in memory, bit-packed
      |  window rolls over / clean shutdown
      v
  block_<base>.blk              <- immutable, fsynced, concatenated blocks
```

Writes go WAL → head. Reads scan the head plus every sealed block whose
`[min_ts, max_ts]` overlaps the requested range.

This head/tail split is the shape InfluxDB, VictoriaMetrics and Prometheus all
converge on, for the same reason: the head is small and mutable so appends are
cheap, and the tail is immutable so reads are sequential and the encoder can
assume a finished block.

### Why the WAL is written before the head

An acknowledged write must survive a crash. Logging after updating the head
would leave a window where the server has said "accepted" and the sample exists
only in RAM.

### Why one fsync per batch

`fsync` is the expensive part of ingest — it waits for the device. The benchmark
sweeps batch size against identical data:

| Batch | Throughput | fsyncs  |
| ----- | ---------- | ------- |
| 1     | 30,192/s   | 100,000 |
| 10    | 48,761/s   | 10,000  |
| 100   | 62,741/s   | 1,000   |
| 1000  | 55,831/s   | 100     |

Roughly 2x from batching, and the compression ratio is identical throughout —
batching buys durability economics, it does not change the encoding. Note that
batch=1000 is *slower* than batch=100 here: past a point the cost of building
large payloads and holding locks outweighs the syscalls saved. The optimum is
workload- and device-dependent, which is exactly why the API accepts batches and
lets the caller choose.

### WAL record layout

```
type        1 byte   1 = batch, 2 = series mapping
payload_len 4 bytes  big endian
payload     n bytes
crc32       4 bytes  IEEE, over type + payload_len + payload
```

CRC over the header as well as the payload matters: a corrupted length field
would otherwise be interpreted as a valid record boundary and desynchronise the
rest of the file.

Batch payloads are **columnar** — all timestamps, then all values:

```
series_id  8 bytes
count      4 bytes
count x int64    seconds
count x float64  values
```

This is not an aesthetic choice. A struct format like `">50qd"` does not mean
"50 (q,d) pairs" — in Python's `struct`, a repeat count binds to the next
*character* only, so that reads 50 timestamps and exactly one value. Writing it
interleaved and parsing it with a repeat count silently loses data. Columnar
sidesteps the trap and matches the block encoding.

### Recovery

On startup: load sealed blocks, then replay the WAL.

Two things are easy to get wrong, and both shipped as bugs in the Go original:

**Dedupe against the sealed high-water mark.** A clean shutdown seals the head
and then truncates the WAL. A crash *between* those steps leaves the same
samples in both places. Recovery drops any WAL sample at or before the series'
sealed `max_ts`. Without it, a restart silently doubles the data.

**Re-register series from the block headers.** After a clean flush the WAL is
empty, so sealed blocks are the only surviving record of the name→ID mapping.
Loading blocks without re-registering them leaves the index empty and every
query matches nothing — while the data is sitting right there on disk.

A torn final record is expected after `kill -9` and costs one batch, not the
file: recovery keeps the intact prefix and reports the truncation.

---

## 3. Series identity and the label index

A series is a metric name plus a label set. `cpu{host="a"}` and `cpu{host="b"}`
are different series that share a name and nothing else.

Interning the label set to an integer ID means ingest does an O(1) dict lookup
per batch instead of re-encoding a string key per sample. Labels are sorted
before hashing so key order in the input cannot mint a second series for one
logical series.

The index maps both directions:

- **name → ID** for ingest;
- **label value → IDs** for queries, which arrive as matchers (`=`, `!=`, `=~`,
  `!~`). Resolution starts from the metric name's posting set and filters, so a
  selector never scans every series.

`__name__` is stored in the label map alongside the others. A series restored
from the WAL must be indistinguishable from one interned fresh, or a matcher on
the metric name behaves differently depending on whether the server restarted.

The index is read by query threads and written by ingest threads, so a lock
covers every access — including reads, because `dict.setdefault` is a
read-modify-write and two threads racing on it will mint two IDs for one key.

---

## 4. Queries

A query names a metric, optionally constrains labels, and gives a range plus an
optional `step` and aggregation.

**Blocks are filtered by timestamp overlap before decoding.** A sealed block
outside the range is never read; that is the only index the storage layer has.

**Downsampling buckets are epoch-aligned**, not aligned to the first sample.
Two queries over overlapping ranges then agree on bucket boundaries, and a
chart's x-axis does not shift as the window scrolls.

**Aggregations:** `avg`, `max`, `min`, `sum`, `last`, `first`, `count`. `first`
and `last` are defined by timestamp order, so the scan sorts before reducing.

### Measured query cost

| Case                | Scanned | Returned | Latency  |
| ------------------- | ------- | -------- | -------- |
| All series, raw     | 400,000 | 400,000  | 8,288 ms |
| All series, step=60 | 400,000 | 6,700    | 7,818 ms |
| One series, raw     | 20,000  | 20,000   | 357 ms   |
| One series, 5 min   | 300     | 300      | 46 ms    |

Downsampling does **not** make a query cheaper — it still decodes everything in
range and then throws most of it away. Reporting only the returned point count
would make `step=60` look 60x faster than raw when the work is identical. Scan
rate is ~50k samples/s.

There is no per-block min/max skipping, no downsampling tier, and no column
pruning. Adding rollup blocks (1s → 1m → 1h) is the obvious next step and would
change the long-range numbers by orders of magnitude.

---

## 5. Alerting

A rule reduces each matching series to one number over a window and compares it
to a threshold.

**Edge-triggered.** A condition that stays true fires once, not once per
evaluation tick. An active `(rule, series)` set tracks this, and a rule that
stops matching resolves — which is what lets it fire again later. Without
resolution, a flapping threshold either spams or goes permanently silent.

**`min_count`.** A window holding two samples is not evidence. Without a floor,
a series that just started reports an alert on its first point.

**Two queries per rule, not one per series.** The value and the sample count
come back as separate range queries, each of which already returns every
matching series. Counting separately keeps `min_count` honest for *every*
aggregation — the average of a two-sample window is just as untrustworthy as
the max of one, and the value itself carries no record of how many samples
produced it.

One bad rule cannot stop the others: evaluation failures are caught and logged
per rule.

---

## 6. HTTP and the live feed

FastAPI for routing and validation, Starlette's WebSocket support for the live
feed. The Go original hand-wrote RFC 6455 framing, masking and close codes —
instructive, and not something to run in production.

**The hub bounds every client queue.** A dashboard tab backgrounded on a laptop
stops reading its socket. An unbounded queue turns that into a memory leak in
the server; a bounded one drops the slow client and counts the drop, which is
the right trade for a live chart.

**Publishing crosses a thread boundary.** Ingest runs in a worker thread, not
the event loop, so handoff goes through `loop.call_soon_threadsafe`. Touching an
`asyncio.Queue` from another thread directly is a race, not a shortcut.

**SSE mirrors the WebSocket feed** for clients that cannot upgrade.

**Writes are authenticated, reads are not**, so the dashboard works without
credentials while ingest and rule changes require a key.

---

## 7. Deliberate limitations

Stated plainly because knowing where the edges are is part of the design:

- **Single node.** No replication, no clustering, no consensus. A real deployment
  needs a replica set and a story for split brain.
- **Sealed blocks are held in RAM.** Past ~1M blocks this needs mmap.
- **Second-resolution timestamps only.**
- **No rollup tiers.** Raw blocks only.
- **No block-level predicate pushdown** beyond the timestamp overlap check.
- **Python throughput.** ~50k samples/s decode. A Go implementation of this same
  design measured ~3.4M samples/s encode. Same compression, same durability —
  50x the throughput. If throughput were the requirement, this would be Go or
  Rust, or the codec would be a C extension.

---

## 8. What I would do next

In rough order of value:

1. **Rollup blocks** — precompute 1m and 1h aggregates at seal time. Long-range
   queries stop scanning raw data; this is the single biggest win available.
2. **mmap sealed blocks** — removes the RAM ceiling and lets the OS page cache
   do its job.
3. **Per-block value ranges in a sidecar index** — skip blocks whose min/max
   cannot match a predicate.
4. **Remote write and read endpoints** — Prometheus compatibility, so Pulse can
   sit behind an existing scrape setup.
5. **A C extension or Cython codec** — the bit loop is the bottleneck and the
   only part that would genuinely benefit.

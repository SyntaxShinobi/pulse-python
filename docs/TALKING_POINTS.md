# Talking points

For an interview. Each item is something you can actually defend, with the
number or the failure that backs it up.

---

## The 60-second version

> Pulse is a self-hosted time-series metrics engine I wrote in Python. It
> implements Facebook's Gorilla compression — delta-of-delta timestamps and
> XOR-encoded floats, bit-packed — behind a write-ahead log, an immutable block
> layout, an inverted label index, range queries with downsampling, threshold
> alerting, and a live WebSocket dashboard. It ingests the Prometheus exposition
> format directly.
>
> The interesting part isn't the feature list, it's the durability model and the
> bugs it exposed. I found five, and each one taught me something different
> about verifying a system that claims not to lose data.

Then stop talking and let them pick a thread.

---

## If they ask about compression

**"How does it work?"**

A sample is an int64 timestamp and a float64 value, 16 bytes raw. Gorilla
encodes the difference between consecutive samples instead of the samples.

For timestamps, the delta-of-delta. A series scraped every 10 seconds has a
constant delta, so the delta-of-delta is zero, and zero costs one bit. The
paper reports that holds for 96% of production samples.

For values, the XOR against the previous value. Similar floats share their sign,
exponent and high mantissa bits, so the XOR is mostly zeros. If it's entirely
zero, one bit says so — 59% of production values. Otherwise I store a 5-bit
leading-zero count, a 6-bit length, and the meaningful window. When the new
XOR's significant bits fit inside the previous window, the window is reused,
which is the common case for a slowly drifting value.

**"What ratio do you get?"**

It depends entirely on the data, and I'd rather show the spread than quote one
number. On 100k samples per shape:

- repeating integers — counters, queue depths, status codes — **0.83 bytes per
  sample, 19x**
- drifting floats, a bounded random walk — **8.25 bytes, 1.9x**
- full-entropy random values on irregular timestamps — **10.75 bytes, 1.5x**

**"The Gorilla paper says 1.37 bytes per sample."**

Yes, and that's measured on production metrics dominated by repeating whole
numbers. It's not a number I can reproduce on arbitrary data and I don't claim
to. That's precisely why my benchmark includes a worst-case shape that barely
compresses — a benchmark that only reports its best shape is measuring its own
generator, not the codec.

**Follow-up they might try: "Why 2-hour blocks?"**

Gorilla's own measurements show compression flattening past roughly two hours,
while read amplification keeps growing — to answer a 5-minute query you decode
the whole block. Two hours is where those cross.

---

## If they ask about durability

This is the strongest thread. Lead with the recovery rule, because it's the
non-obvious one.

**"What happens if the process is killed mid-write?"**

The batch is fsynced to the WAL *before* the request is acknowledged, so an
acknowledged write survives `kill -9`. If the kill lands mid-record, the final
record is torn: recovery validates a CRC32 over the header *and* payload, keeps
the intact prefix, drops the torn tail, and starts. You lose one batch, not the
file.

The CRC covers the length field deliberately. A corrupted length would otherwise
be read as a valid record boundary and desynchronise everything after it.

**"Walk me through restart."**

Load the sealed block files, then replay the WAL into the head.

The subtle part is the dedupe. A clean shutdown seals the head and *then*
truncates the WAL. A crash between those two steps leaves the same samples in
both places. So recovery drops any WAL sample at or before that series' sealed
high-water mark. Without that guard, a restart silently doubles your data — no
error, no log line, just twice the points on the chart.

**"Why not truncate the WAL inside flush?"**

Because flush is routine — it can run periodically — and truncating there would
make a crash after a flush lose data that was never sealed. `compact()` is the
explicit "the WAL has served its purpose" operation: it seals, and only then
truncates. It's safe because sealing fsyncs each block file before returning, so
by the time the WAL is dropped every sample in it is durable elsewhere.

**"Why one fsync per batch instead of per sample?"**

Because fsync is the whole cost. I measured it: same data, swept across batch
sizes — 30k samples/s at batch=1, 63k at batch=100. About 2x, and that ratio
*is* the syscall being amortised. Compression is identical throughout, so
batching buys durability economics without changing the encoding.

Worth volunteering: batch=1000 was *slower* than batch=100 in my run. Past a
point, building large payloads and holding locks costs more than the syscalls
you save. The optimum is device-dependent, which is why the API takes batches
and lets the caller pick.

---

## The bugs — the best material

Five real bugs. Each has a different lesson, and saying what the lesson was
matters more than the bug.

### 1. Restart doubled the data

**Symptom:** after a restart, every series had exactly twice as many points.

**Cause:** recovery replayed the WAL on top of blocks that were already sealed.
The crash window between sealing and truncating.

**Fix:** dedupe against each series' sealed `max_ts`.

**Lesson:** the interesting failures live in the transitions between states, not
in the states. I tested "clean shutdown" and I tested "crash", but not "crash
*here* specifically". Now there's a test that flushes without truncating and
asserts the count stays the same.

### 2. Queries matched nothing after a clean restart

**Symptom:** data on disk, index empty, every query returns zero series.

**Cause:** after a clean flush the WAL is empty, so the sealed block headers are
the only surviving record of the name→ID mapping. I was loading the blocks
without re-registering the series.

**Lesson:** when you compact away a log, ask what else that log was the only
copy of.

### 3. Labels vanished after restart

**Symptom:** `cpu{host="a"}` came back as bare `cpu`.

**Cause:** I stored the metric name in the block header instead of the full
selector.

**Lesson:** persist the identity, not a display string. The header looked fine
in every test that didn't restart.

### 4. Silent corruption — the expensive one

**Symptom:** samples written *after* the first read came back as garbage. The
first read was always fine.

**Cause:** my `bytes()`-style getter finalised the bit buffer as a side effect.
Finalising pads the partial byte to a byte boundary — which moves the write
cursor. So the "read" mutated the encoder, and every subsequent append started
at the wrong bit offset.

**Why it cost so long:** I assumed the encoder was broken and went reading
compression logic. The encoder was fine.

**What actually found it:** two trivial experiments I should have run first —
remove the reader and see if the corruption stops, and dump the bytes at the
moment of sealing. Both took a minute.

**Fix:** the read path returns a non-mutating snapshot; only `finalize()` pads,
and it's called once, at seal.

**Lesson, and the one I'd say out loud:** *a getter that mutates is a bug
waiting for a caller.* And: when something is corrupt, bisect by *removing*
components before you start reading them. I lost time to a plausible theory
instead of a two-line experiment.

There's a regression test for it: interleave a decode after every append and
assert the output is byte-identical to the non-interleaved baseline.

### 5. The dashboard said "0 samples"

**Symptom:** after a restart, a full chart next to a counter reading zero.

**Cause:** recovery never added the replayed samples to the ingest counter.

**Lesson:** it presented as a frontend bug and wasn't. When a display is wrong,
check the number at the source before touching the renderer. Recovery now counts
what it restored and exposes it separately as `recovered_samples`.

---

## If they ask about performance

Be straight about this. It's more credible than spinning it.

- Codec: ~500k samples/s encode and ~400k/s decode on repeating integers;
  ~60-70k/s on data that doesn't compress, because the encoder does more work
  per sample when it can't take the cheap branches.
- Ingest through the full path — intern, WAL, fsync, head block: ~63k samples/s
  at batch=100.
- Query: ~50k samples/s scan rate.

**"That's slow."**

It is, and it's Python. The codec is a per-sample bit loop in an interpreted
language. A Go implementation of this same design measured about 3.4M samples/s
encode — roughly 50x. The compression ratio and the durability guarantees are
identical; only the throughput differs. If throughput were the requirement I'd
write it in Go or Rust, or make the codec a C extension. I chose Python because
I wanted the design to be readable and every line to be something I could
explain.

**"Did you try numpy?"**

Yes, and I measured it rather than assuming. Vectorising the aggregation with
`np.add.reduceat` is about 13x faster than my scalar bucketing loop. But 89% of
that win is spent copying decoded Python objects into arrays, because the
decoder yields tuples. Net effect on the query path: 1.5x, and the
floating-point summation order changes the result. That's not worth a hard
dependency, so numpy isn't one.

The real fix would be for the decoder to produce arrays directly — then the
vectorisation would pay. That's a design change, not a library swap.

**"What's the actual bottleneck?"**

Query latency is dominated by decode plus per-sample Python object creation.
Downsampling doesn't help — I measured a step=60 query taking the same 7.8s as
the raw one over 400k samples, because it decodes everything and then throws
most of it away. The honest fix is rollup blocks: precompute 1-minute and 1-hour
aggregates at seal time so long-range queries never touch raw data.

---

## If they ask about concurrency

- **The index is locked for reads too.** `dict.setdefault` is a
  read-modify-write; two ingest threads racing on it will mint two IDs for one
  key. There's a test with 8 threads × 500 samples asserting no series loses a
  sample.
- **The head block has its own lock** because a query encodes it while ingest
  appends to it. That's also why the read path must not mutate — bug 4 — since
  a concurrent encode during an append corrupts the buffer.
- **The live-feed hub hands off via `call_soon_threadsafe`.** Ingest runs in a
  worker thread, not the event loop, and touching an `asyncio.Queue` from
  another thread is a race, not a shortcut.
- **Slow clients are dropped, not buffered.** Each WebSocket gets a bounded
  queue. A backgrounded browser tab stops reading; an unbounded queue turns that
  into a memory leak in my server.

---

## If they ask what you'd do next

In order of value:

1. **Rollup blocks** — precompute 1m and 1h aggregates at seal time. Biggest win
   available; long-range queries stop scanning raw data.
2. **mmap sealed blocks** — they're held in RAM now, which caps the dataset.
3. **Per-block min/max sidecar** — skip blocks that can't match a predicate.
4. **Prometheus remote-write/read** — drop-in compatibility with existing scrape
   setups.
5. **A C extension codec** — the bit loop is the bottleneck and the only part
   that would genuinely benefit.

And the honest one: **it's single-node.** No replication, no consensus. That's
the biggest gap between this and something you'd run in production, and I'd want
to design the replica story before adding features.

---

## Questions worth asking back

- How do you decide what's worth measuring before writing the code?
- What does your on-call rotation look like for the storage layer?
- Where's the boundary between the metrics system and the alerting system here?

---

## Things not to claim

Say these plainly if they come up. Overclaiming is the fastest way to lose the
room.

- It is **single-node**. No replication, no clustering.
- Sealed blocks are **held in memory**; past ~1M blocks it needs mmap.
- **Second-resolution timestamps** only.
- **No rollup tiers** — raw blocks only, so long-range queries are slow.
- No per-block predicate pushdown beyond the timestamp overlap check.
- The 1.37 B/sample figure is **the paper's**, on production data. My measured
  range is 0.83–10.75 depending on shape.
- The Go throughput numbers are **not** this implementation's. Python is ~50x
  slower and I measured it rather than guessing.

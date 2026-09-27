"""Storage-layer tests: block format, WAL framing, the index, and the engine.

The restart tests here are the reason this file exists. Every one of them is a
regression guard for a bug that actually shipped in the Go original:

* WAL replay duplicating samples that were also in a sealed block;
* sealed series never re-registered in the index, so queries matched nothing;
* labels lost because bare metric names were persisted instead of selectors;
* recovery not counting what it restored, so ``/api/stats`` reported zero
  samples while the dashboard drew a full chart.
"""

from __future__ import annotations

import os
import time

import pytest

from pulse.block import BLOCK_SPAN_SECONDS, MAX_SAMPLES_PER_BLOCK, Block, align_block_base, decode_block, sealed_block_size
from pulse.engine import DB
from pulse.index import LABEL_RE, Matcher, SeriesIndex, encode_name, parse_matchers, parse_selector
from pulse.query import Query, Sample, downsample
from pulse.wal import WAL, recover_wal

BASE = 1_700_000_000


@pytest.fixture
def data_dir(tmp_path):
    return tmp_path / "data"


# --------------------------------------------------------------------- blocks


def test_align_block_base_floors_to_the_window():
    assert align_block_base(0) == 0
    assert align_block_base(BLOCK_SPAN_SECONDS - 1) == 0
    assert align_block_base(BLOCK_SPAN_SECONDS) == BLOCK_SPAN_SECONDS
    assert align_block_base(BLOCK_SPAN_SECONDS * 3 + 17) == BLOCK_SPAN_SECONDS * 3


def test_block_round_trip_preserves_every_sample():
    block = Block(series_id=7, name='cpu{host="a"}', base_time=BASE)
    expected = [(BASE + i * 3, float(i) * 1.5) for i in range(500)]
    for t, v in expected:
        assert block.append(t, v)
    assert block.count == 500
    assert block.range == (BASE, BASE + 499 * 3)

    decoded = decode_block(block.encode())
    assert decoded.series_id == 7
    assert decoded.name == 'cpu{host="a"}'
    assert decoded.base_time == BASE
    assert decoded.min_ts == BASE
    assert decoded.max_ts == BASE + 499 * 3
    assert decoded.count == 500
    assert list(decoded) == expected


def test_block_rejects_samples_outside_its_window():
    block = Block(series_id=1, name="cpu", base_time=BASE)
    assert not block.full(BASE + BLOCK_SPAN_SECONDS - 1)
    assert block.full(BASE + BLOCK_SPAN_SECONDS)
    assert block.full(BASE - 1)
    assert not block.append(BASE + BLOCK_SPAN_SECONDS, 1.0)
    assert block.count == 0


def test_sample_cap_is_a_backstop_behind_the_window():
    """At 1 Hz the 2h window fills first, so the cap only binds for
    writers that emit several samples per second."""
    assert MAX_SAMPLES_PER_BLOCK > BLOCK_SPAN_SECONDS
    block = Block(series_id=1, name="cpu", base_time=0)
    appended = sum(1 for i in range(MAX_SAMPLES_PER_BLOCK) if block.append(i, 1.0))
    assert appended == BLOCK_SPAN_SECONDS  # one sample per distinct second
    assert block.count == BLOCK_SPAN_SECONDS


def test_sealed_block_size_matches_the_encoded_length():
    """Lets a reader walk a file of concatenated blocks without decoding."""
    encoded = b"".join(
        Block(series_id=i, name=f"m{i}{{a=\"{i}\"}}", base_time=BASE + i).encode()
        for i in range(1, 4)
    )
    off, walked = 0, 0
    while off < len(encoded):
        size = sealed_block_size(encoded[off:])
        assert size > 0
        decoded = decode_block(encoded[off : off + size])
        walked += 1
        assert decoded.series_id == walked
        off += size
    assert off == len(encoded)
    assert walked == 3


def test_decode_rejects_corrupt_blocks():
    good = Block(series_id=1, name="cpu", base_time=BASE)
    good.append(BASE, 1.0)
    raw = bytearray(good.encode())

    with pytest.raises(ValueError, match="too short"):
        decode_block(b"PULS")
    bad_magic = bytearray(raw)
    bad_magic[0] = ord("X")
    with pytest.raises(ValueError, match="bad block magic"):
        decode_block(bytes(bad_magic))
    with pytest.raises(ValueError, match="truncated"):
        decode_block(bytes(raw[: len(raw) - 5]))


# ------------------------------------------------------------------------ WAL


def test_wal_round_trips_both_record_types(tmp_path):
    path = tmp_path / "wal.log"
    with WAL(path) as wal:
        wal.log_series(4, 'cpu{host="a"}')
        wal.log_batch(4, [BASE, BASE + 1], [1.5, -2.5])
        wal.sync()

    records: list = []
    assert recover_wal(path, records.append) is False
    assert len(records) == 2
    assert (records[0].type, records[0].series_id, records[0].name) == (2, 4, 'cpu{host="a"}')
    assert records[1].timestamps == (BASE, BASE + 1)
    assert records[1].values == (1.5, -2.5)


def test_wal_survives_a_torn_final_record(tmp_path):
    """A crash mid-write must cost one batch, not the file."""
    path = tmp_path / "wal.log"
    with WAL(path) as wal:
        for i in range(5):
            wal.log_batch(1, [BASE + i], [float(i)])
        wal.sync()

    intact = path.read_bytes()
    path.write_bytes(intact[:-6])  # chop through the last record's checksum

    records: list = []
    assert recover_wal(path, records.append) is True  # reported as corrupt
    assert len(records) == 4  # intact prefix survived


def test_wal_detects_bit_rot(tmp_path):
    """CRC32 over the header and payload catches a flipped byte."""
    path = tmp_path / "wal.log"
    with WAL(path) as wal:
        wal.log_series(1, "cpu")
        wal.log_batch(1, [BASE], [1.0])
        wal.sync()

    raw = bytearray(path.read_bytes())
    raw[20] ^= 0x40
    path.write_bytes(bytes(raw))

    records: list = []
    assert recover_wal(path, records.append) is True


def test_wal_truncate_empties_the_log(tmp_path):
    path = tmp_path / "wal.log"
    with WAL(path) as wal:
        wal.log_batch(1, [BASE], [1.0])
        wal.sync()
        assert wal.stats()["records"] == 1
        wal.truncate()
        assert wal.stats() == {"records": 0, "bytes": 0, "syncs": 1}
    assert path.stat().st_size == 0


def test_wal_rejects_mismatched_batch(tmp_path):
    with WAL(tmp_path / "wal.log") as wal:
        with pytest.raises(ValueError, match="length mismatch"):
            wal.log_batch(1, [BASE, BASE + 1], [1.0])


# ---------------------------------------------------------------------- index


def test_index_interns_one_id_per_label_set():
    index = SeriesIndex()
    a, created_a = index.create("cpu", {"host": "a"})
    b, _ = index.create("cpu", {"host": "b"})
    a_again, created_again = index.create("cpu", {"host": "a"})
    assert (created_a, created_again) == (True, False)
    assert a == a_again
    assert a != b
    assert index.labels(a) == {"host": "a", "__name__": "cpu"}


def test_index_is_insensitive_to_label_order():
    """Key order in the input must not mint a second series."""
    index = SeriesIndex()
    first, _ = index.create("cpu", {"host": "a", "job": "x"})
    second, created = index.create("cpu", {"job": "x", "host": "a"})
    assert first == second
    assert created is False


def test_index_resolves_matchers():
    index = SeriesIndex()
    a, _ = index.create("cpu", {"host": "a", "job": "web"})
    b, _ = index.create("cpu", {"host": "b", "job": "db"})
    index.create("mem", {"host": "a"})

    assert index.match("cpu") == [a, b]
    assert index.match("mem") == [3]
    assert index.match("cpu", [Matcher("=", "host", "a")]) == [a]
    assert index.match("cpu", [Matcher("!=", "host", "a")]) == [b]
    assert index.match("cpu", [Matcher("=~", "host", "a|b")]) == [a, b]
    assert index.match("cpu", [Matcher("!~", "job", "web")]) == [b]
    assert index.match("cpu", [Matcher("=", "job", "")]) == []
    assert index.match("missing") == []


def test_index_register_keeps_ids_stable_across_restarts():
    """WAL replay supplies the ID; it must not be re-minted."""
    index = SeriesIndex()
    index.create("cpu", {"host": "a"})
    assert index.register(99, 'mem{host="c"}') is True
    assert index.register(99, 'mem{host="c"}') is False  # idempotent
    assert index.match("mem") == [99]
    # A restored series must look exactly like an interned one, including
    # __name__: otherwise a metric-name matcher behaves differently depending
    # on whether the server has restarted.
    assert index.labels(99) == {"host": "c", "__name__": "mem"}
    fresh_id, _ = index.create("mem", {"host": "c"})
    assert fresh_id == 99  # the same label set, not a new series
    assert index.labels(fresh_id) == index.labels(99)
    # A later create must not collide with the restored ID.
    new_id, _ = index.create("net", {})
    assert new_id != 99


def test_index_rejects_invalid_input():
    index = SeriesIndex()
    with pytest.raises(ValueError, match="__name__"):
        encode_name({"host": "a"})
    with pytest.raises(ValueError, match="at least one label"):
        encode_name({})
    with pytest.raises(ValueError, match="invalid label name"):
        encode_name({"__name__": "cpu", "1bad": "x"})
    with pytest.raises(ValueError, match="invalid metric name"):
        index.create("9cpu")
    with pytest.raises(ValueError, match="unknown matcher op"):
        Matcher("~~", "a", "b")
    with pytest.raises(ValueError, match="invalid regex"):
        Matcher("=~", "a", "(")


def test_parse_matchers():
    assert parse_matchers("cpu") == ("cpu", [])
    name, matchers = parse_matchers('cpu{host="a",job!="b",env=~"p.*"}')
    assert name == "cpu"
    assert [(m.op, m.label, m.value) for m in matchers] == [
        ("=", "host", "a"),
        ("!=", "job", "b"),
        ("=~", "env", "p.*"),
    ]
    assert parse_selector('cpu{host="a"}') == ("cpu", {"host": "a"})
    assert parse_selector("cpu") == ("cpu", {})

    with pytest.raises(ValueError, match="unterminated"):
        parse_matchers('cpu{host="a"')
    with pytest.raises(ValueError, match="no label matchers"):
        parse_matchers("cpu{}")


def test_single_label_selector_matches():
    """A trailing space in the matcher regex made `mem{host="c"}` match nothing."""
    assert parse_selector('mem{host="c"}') == ("mem", {"host": "c"})
    index = SeriesIndex()
    series_id, _ = index.create("mem", {"host": "c"})
    assert index.match("mem", [Matcher("=", "host", "c")]) == [series_id]


def test_label_regex():
    assert LABEL_RE.fullmatch("_private")
    assert LABEL_RE.fullmatch("a1")
    assert not LABEL_RE.fullmatch("1a")
    assert not LABEL_RE.fullmatch("a-b")


# ----------------------------------------------------------------- downsampling


def test_downsample_buckets_are_epoch_aligned():
    """Buckets align to the epoch, not to the first sample, so overlapping
    queries agree on boundaries and a chart does not shift as it scrolls."""
    offset = BASE % 60
    assert offset == 20, "this test depends on BASE not being a multiple of 60"
    samples = [Sample(t=BASE + i, v=float(i)) for i in range(120)]
    out = downsample(samples, 60, "avg")

    first_key = BASE - offset
    assert [s.t for s in out] == [first_key, first_key + 60, first_key + 120]
    # The first bucket is partial: it only sees samples from BASE onward.
    assert out[0].v == pytest.approx(sum(range(40)) / 40)
    assert out[1].v == pytest.approx(sum(range(40, 100)) / 60)
    assert out[2].v == pytest.approx(sum(range(100, 120)) / 20)


def test_downsample_aggregations():
    samples = [Sample(t=i, v=v) for i, v in enumerate([1.0, 5.0, 3.0, 9.0])]
    assert [s.v for s in downsample(samples, 2, "avg")] == [3.0, 6.0]
    assert [s.v for s in downsample(samples, 2, "max")] == [5.0, 9.0]
    assert [s.v for s in downsample(samples, 2, "min")] == [1.0, 3.0]
    assert [s.v for s in downsample(samples, 2, "sum")] == [6.0, 12.0]
    assert [s.v for s in downsample(samples, 2, "first")] == [1.0, 3.0]
    assert [s.v for s in downsample(samples, 2, "last")] == [5.0, 9.0]
    assert [s.v for s in downsample(samples, 2, "count")] == [2.0, 2.0]


def test_downsample_step_one_is_the_identity():
    samples = [Sample(t=i, v=float(i)) for i in range(10)]
    assert downsample(samples, 1, "avg") == samples
    assert downsample([], 60, "avg") == []


# ---------------------------------------------------------------------- engine


def _series(db: DB, metric: str, **kw) -> list:
    return db.query(Query(metric=metric, start=0, end=2**40, **kw))


def test_write_then_query_returns_exact_values(data_dir):
    with DB(data_dir) as db:
        timestamps = [BASE + i for i in range(300)]
        values = [50.0 + (i % 7) for i in range(300)]
        db.write('cpu{host="a"}', timestamps, values)

        result = _series(db, "cpu")[0]
        assert [p.v for p in result.points] == values
        assert [p.t for p in result.points] == timestamps


def test_query_isolates_series_by_label(data_dir):
    with DB(data_dir) as db:
        ts = [BASE + i for i in range(10)]
        db.write('cpu{host="a"}', ts, [1.0] * 10)
        db.write('cpu{host="b"}', ts, [2.0] * 10)

        assert len(_series(db, "cpu")) == 2
        only_b = _series(db, "cpu", labels={"host": "b"})
        assert len(only_b) == 1
        assert only_b[0].labels == {"host": "b", "__name__": "cpu"}
        assert {p.v for p in only_b[0].points} == {2.0}


def test_query_rejects_bad_input(data_dir):
    with DB(data_dir) as db:
        db.write("cpu", [BASE], [1.0])
        with pytest.raises(ValueError, match="start must be"):
            db.query(Query(metric="cpu", start=10, end=5))
        with pytest.raises(ValueError, match="unknown aggregation"):
            db.query(Query(metric="cpu", start=0, end=10, agg="median"))
        with pytest.raises(ValueError, match="length mismatch"):
            db.write("cpu", [BASE, BASE + 1], [1.0])


def test_rollover_seals_and_keeps_data_readable(data_dir):
    """Crossing a 2h boundary must seal, not drop or duplicate."""
    with DB(data_dir) as db:
        ts = [BASE + i * 60 for i in range(250)]  # 250 min > 2h
        db.write("cpu", ts, [float(i) for i in range(250)])
        assert db.stats()["blocks_sealed"] >= 1
        assert [p.t for p in _series(db, "cpu")[0].points] == ts


def test_restart_after_clean_close_keeps_data_and_reports_it(data_dir):
    db = DB(data_dir)
    db.write('cpu{host="a"}', [BASE + i for i in range(100)], [float(i) for i in range(100)])
    db.close()

    with DB(data_dir) as db:
        stats = db.stats()
        points = _series(db, "cpu")[0].points
        assert len(points) == 100, "clean restart must not duplicate sealed samples"
        assert stats["total_samples"] == 100, "recovered samples must be counted"
        assert stats["recovered_samples"] == 100
        assert stats["sealed_samples"] == 100
        assert stats["head_samples"] == 0
        assert db.wal.stats()["bytes"] == 0, "clean close truncates the WAL"


def test_restart_after_crash_dedupes_wal_against_sealed_blocks(data_dir):
    """The crash window between sealing and truncating must not double data."""
    db = DB(data_dir)
    values = [float(i % 5) for i in range(50)]
    db.write('mem{host="a"}', [BASE + i for i in range(50)], values)
    db.flush()  # seals and fsyncs blocks, but leaves the WAL intact
    assert os.path.getsize(db.wal.path) > 0
    db.wal.close()  # abrupt exit: no compact, no truncate

    with DB(data_dir) as db:
        points = _series(db, "mem")[0].points
        assert len(points) == 50, "WAL samples already sealed must be dropped"
        assert [p.v for p in points] == values
        assert db.stats()["total_samples"] == 50


def test_restart_from_wal_alone_preserves_labels(data_dir):
    """Sealed blocks are absent here, so the WAL is the only mapping record."""
    db = DB(data_dir)
    db.write('disk{host="a",mount="/"}', [BASE], [9.0])
    db.wal.close()  # crash before anything was sealed

    with DB(data_dir) as db:
        series = db.all_series()
        assert len(series) == 1
        assert series[0]["labels"] == {"host": "a", "mount": "/", "__name__": "disk"}
        assert _series(db, "disk")[0].points[0].v == 9.0


def test_restart_after_torn_wal_starts_with_the_intact_prefix(data_dir):
    db = DB(data_dir)
    db.write('net{host="a"}', [BASE + i for i in range(20)], [1.0] * 20)
    db.write('net{host="a"}', [BASE + 100 + i for i in range(20)], [2.0] * 20)
    intact = open(db.wal.path, "rb").read()
    db.wal.close()
    with open(db.wal.path, "wb") as fh:
        fh.write(intact[:-7])

    with DB(data_dir) as db:
        assert db.wal.corrupt_tail is True
        points = _series(db, "net")[0].points
        assert len(points) == 20, "first batch survived, torn second batch dropped"
        assert {p.v for p in points} == {1.0}


def test_flush_does_not_truncate_but_compact_does(data_dir):
    with DB(data_dir) as db:
        db.write("cpu", [BASE], [1.0])
        db.flush()
        assert db.wal.stats()["bytes"] > 0
        assert db.stats()["head_blocks"] == 0
        db.compact()
        assert db.wal.stats()["bytes"] == 0
        # Still readable, now served entirely from sealed blocks.
        assert _series(db, "cpu")[0].points[0].v == 1.0


def test_stats_tracks_reads_and_disk_usage(data_dir):
    with DB(data_dir) as db:
        db.write("cpu", [BASE + i for i in range(100)], [1.0] * 100)
        db.flush()
        before = db.stats()
        assert before["bytes_on_disk"] > 0
        assert before["samples_read"] == 0

        _series(db, "cpu")
        after = db.stats()
        assert after["samples_read"] == 100
        assert after["sealed_blocks"] == 1
        assert after["block_span_secs"] == BLOCK_SPAN_SECONDS


def test_concurrent_writers_do_not_lose_or_duplicate_samples(data_dir):
    import threading

    with DB(data_dir) as db:
        errors: list[BaseException] = []

        def worker(host: str) -> None:
            try:
                for batch in range(10):
                    ts = [BASE + batch * 50 + i for i in range(50)]
                    db.write(f'cpu{{host="{host}"}}', ts, [float(batch)] * 50)
            except BaseException as exc:  # noqa: BLE001 - surfaced to the test
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(f"h{i}",)) for i in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == []
        stats = db.stats()
        assert stats["series"] == 8
        assert stats["total_samples"] == 8 * 500
        for i in range(8):
            points = _series(db, "cpu", labels={"host": f"h{i}"})[0].points
            assert len(points) == 500, f"series h{i} lost samples"


def test_subscribe_sees_every_accepted_batch(data_dir):
    with DB(data_dir) as db:
        seen: list[tuple[int, tuple[int, ...]]] = []
        db.subscribe(lambda sid, ts, vs: seen.append((sid, tuple(ts))))
        db.write("cpu", [BASE, BASE + 1], [1.0, 2.0])
        db.write("cpu", [BASE + 2], [3.0])
        assert [s[1] for s in seen] == [(BASE, BASE + 1), (BASE + 2,)]
        assert seen[0][0] == seen[1][0], "same series keeps one ID"


def test_samples_written_just_now_are_readable(data_dir):
    """The head block is read live, not only once sealed."""
    with DB(data_dir) as db:
        now = int(time.time())
        db.write("cpu", [now - 2, now - 1, now], [1.0, 2.0, 3.0])
        assert db.stats()["head_blocks"] == 1
        assert db.stats()["sealed_blocks"] == 0
        latest = db.latest("cpu", window_secs=60)
        assert len(latest) == 1
        assert latest[0].points[-1].v == 3.0

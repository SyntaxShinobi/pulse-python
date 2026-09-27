"""API tests.

Exercised through FastAPI's ``TestClient``, which runs the real ASGI app, so
routing, validation and the WebSocket path are covered the way a client hits
them. The write path is checked against the Prometheus exposition format
because that is the contract external scrapers depend on.
"""

from __future__ import annotations

import time

import pytest
from fastapi.testclient import TestClient

from pulse.api import Hub, create_app, parse_exposition
from pulse.engine import DB
from pulse.query import Query
from pulse.rules import AlertRule, DBQuerier, RulesEngine


@pytest.fixture
def stack(tmp_path):
    db = DB(tmp_path / "data")
    rules = RulesEngine(DBQuerier(db), interval_sec=3600)  # manual evaluation only
    app = create_app(db, rules)
    with TestClient(app) as client:
        yield client, db, rules
    db.close()


@pytest.fixture
def secured_stack(tmp_path):
    db = DB(tmp_path / "data")
    rules = RulesEngine(DBQuerier(db), interval_sec=3600)
    app = create_app(db, rules, api_key="s3cret")
    with TestClient(app) as client:
        yield client, db, rules
    db.close()


# ---------------------------------------------------------- exposition parser


def test_parse_exposition_handles_the_prometheus_shape():
    body = "\n".join(
        [
            "# HELP cpu_usage docs",
            "# TYPE cpu_usage gauge",
            "",
            'cpu_usage{host="a"} 42.5',
            'cpu_usage{host="b"} 7 1700000000000',
            "plain_metric 1",
        ]
    )
    batches, errors, accepted = parse_exposition(body, default_ts=123)
    assert accepted == 3
    assert errors == []
    assert batches['cpu_usage{host="a"}'] == ([123], [42.5])
    # Exposition timestamps are milliseconds; the engine stores seconds.
    assert batches['cpu_usage{host="b"}'] == ([1_700_000_000], [7.0])
    assert batches["plain_metric"] == ([123], [1.0])


def test_parse_exposition_reports_bad_lines_without_dropping_the_batch():
    body = "\n".join(
        [
            "good 1",
            "novalue",
            "bad notanumber",
            "badts 3 notats",
            "negts 3 -5",
            "also_good 2",
        ]
    )
    batches, errors, accepted = parse_exposition(body, default_ts=1)
    assert accepted == 2
    assert set(batches) == {"good", "also_good"}
    assert len(errors) == 4
    assert "bad value" in errors[1]
    assert "bad timestamp" in errors[2]
    assert "positive" in errors[3]


# ------------------------------------------------------------------- endpoints


def test_healthz(stack):
    client, _, _ = stack
    assert client.get("/healthz").json() == {"status": "ok"}


def test_write_then_query_round_trip(stack):
    client, db, _ = stack
    now = int(time.time())
    lines = [f'cpu_usage{{host="a"}} {40 + i}' for i in range(10)]
    resp = client.post("/api/write", content="\n".join(lines))
    assert resp.status_code == 200
    assert resp.json()["accepted"] == 10
    assert resp.json()["series"] == 1

    got = db.query(Query(metric="cpu_usage", start=now - 5, end=now + 5))[0]
    assert [p.v for p in got.points] == [float(40 + i) for i in range(10)]

    resp = client.get("/api/query", params={"metric": "cpu_usage", "start": now - 5, "end": now + 5})
    assert resp.status_code == 200
    body = resp.json()
    assert body["metric"] == "cpu_usage"
    assert len(body["result"]) == 1
    assert len(body["result"][0]["points"]) == 10


def test_query_requires_a_metric(stack):
    client, _, _ = stack
    assert client.get("/api/query").status_code == 422


def test_query_rejects_an_unknown_aggregation(stack):
    client, _, _ = stack
    client.post("/api/write", content="cpu 1")
    resp = client.get("/api/query", params={"metric": "cpu", "agg": "median"})
    assert resp.status_code == 400
    assert "unknown aggregation" in resp.json()["error"]


def test_query_rejects_an_inverted_range(stack):
    client, _, _ = stack
    resp = client.get("/api/query", params={"metric": "cpu", "start": 100, "end": 50})
    assert resp.status_code == 400


def test_query_applies_label_filters(stack):
    client, _, _ = stack
    client.post("/api/write", content='cpu{host="a"} 1\ncpu{host="b"} 2')
    resp = client.get("/api/query", params={"metric": "cpu", "label_host": "b"})
    body = resp.json()
    assert len(body["result"]) == 1
    assert body["result"][0]["labels"]["host"] == "b"


def test_query_range_alias_works(stack):
    client, _, _ = stack
    client.post("/api/write", content="cpu 1")
    assert client.get("/api/query_range", params={"metric": "cpu"}).status_code == 200


def test_write_rejects_an_invalid_series_name(stack):
    client, _, _ = stack
    resp = client.post("/api/write", content="9bad_metric 1")
    assert resp.status_code == 400
    assert "invalid metric name" in resp.json()["error"]


def test_series_endpoint_lists_selectors(stack):
    client, _, _ = stack
    client.post("/api/write", content='cpu{host="a"} 1\nmem{host="a"} 2')
    series = client.get("/api/series").json()["series"]
    assert {s["name"] for s in series} == {'cpu{host="a"}', 'mem{host="a"}'}
    assert all(s["id"] > 0 for s in series)


def test_stats_counts_writes_and_samples(stack):
    client, _, _ = stack
    client.post("/api/write", content="cpu 1\ncpu 2\ncpu 3")
    stats = client.get("/api/stats").json()
    assert stats["writes"] == 1
    assert stats["samples_received"] == 3
    assert stats["total_samples"] == 3
    assert stats["series"] == 1
    assert stats["block_span_secs"] == 7200


def test_stats_exposes_rule_counters(stack):
    client, _, _ = stack
    stats = client.get("/api/stats").json()
    assert stats["rules_rules"] == 0
    assert stats["rules_fired_total"] == 0


# ------------------------------------------------------------------- alerting


def test_rule_lifecycle(stack):
    client, _, _ = stack
    client.post("/api/write", content="cpu 90")

    resp = client.post(
        "/api/rules",
        json={"name": "cpu high", "metric": "cpu", "agg": "avg", "windowSec": 300, "op": ">", "threshold": 50, "minCount": 1},
    )
    assert resp.status_code == 201
    rule = resp.json()
    assert rule["id"] == "r1"
    assert rule["enabled"] is True

    assert len(client.get("/api/rules").json()["rules"]) == 1

    check = client.post("/api/rules/check")
    assert check.status_code == 200
    assert check.json()["fired"] == 1

    alerts = client.get("/api/alerts").json()["alerts"]
    assert len(alerts) == 1
    assert alerts[0]["ruleId"] == "r1"
    assert "cpu high" in alerts[0]["message"]

    # Edge-triggered: an unchanged condition does not fire again.
    assert client.post("/api/rules/check").json()["fired"] == 0

    assert client.delete(f"/api/rules/{rule['id']}").status_code == 204
    assert client.get("/api/rules").json()["rules"] == []
    assert client.delete(f"/api/rules/{rule['id']}").status_code == 404


def test_rule_validation_is_rejected_not_stored(stack):
    client, _, _ = stack
    assert client.post("/api/rules", json={"name": "x"}).status_code == 400
    assert client.post("/api/rules", json={"metric": "cpu"}).status_code == 400
    resp = client.post("/api/rules", json={"name": "x", "metric": "cpu", "op": "~="})
    assert resp.status_code == 400
    assert "unknown operator" in resp.json()["error"]
    resp = client.post("/api/rules", json={"name": "x", "metric": "cpu", "agg": "median"})
    assert resp.status_code == 400
    assert client.get("/api/rules").json()["rules"] == []


def test_duplicate_rule_id_conflicts(stack):
    client, _, _ = stack
    payload = {"id": "fixed", "name": "x", "metric": "cpu"}
    assert client.post("/api/rules", json=payload).status_code == 201
    assert client.post("/api/rules", json=payload).status_code == 409


def test_invalid_json_body_is_a_400(stack):
    client, _, _ = stack
    resp = client.post("/api/rules", content="{not json", headers={"Content-Type": "application/json"})
    assert resp.status_code == 400
    assert "invalid JSON" in resp.json()["error"]


# --------------------------------------------------------------------- auth


def test_writes_require_the_key_when_one_is_configured(secured_stack):
    client, db, _ = secured_stack
    assert client.post("/api/write", content="cpu 1").status_code == 401
    assert client.post("/api/write", content="cpu 1", headers={"X-Api-Key": "wrong"}).status_code == 401

    assert client.post("/api/write", content="cpu 1", headers={"X-Api-Key": "s3cret"}).status_code == 200
    assert client.post("/api/write", content="cpu 2", headers={"Authorization": "Bearer s3cret"}).status_code == 200
    assert db.stats()["total_samples"] == 2


def test_reads_stay_open_when_a_key_is_configured(secured_stack):
    client, _, _ = secured_stack
    client.post("/api/write", content="cpu 1", headers={"X-Api-Key": "s3cret"})
    assert client.get("/api/stats").status_code == 200
    assert client.get("/api/query", params={"metric": "cpu"}).status_code == 200
    assert client.get("/api/series").status_code == 200


def test_rule_mutation_requires_the_key(secured_stack):
    client, _, _ = secured_stack
    payload = {"name": "x", "metric": "cpu"}
    assert client.post("/api/rules", json=payload).status_code == 401
    assert client.post("/api/rules", json=payload, headers={"X-Api-Key": "s3cret"}).status_code == 201
    assert client.delete("/api/rules/r1").status_code == 401
    assert client.delete("/api/rules/r1", headers={"X-Api-Key": "s3cret"}).status_code == 204
    assert client.post("/api/rules/check").status_code == 401


# ------------------------------------------------------------------ live feed


def test_websocket_receives_the_hello_and_live_samples(stack):
    client, _, _ = stack
    with client.websocket_connect("/api/live") as ws:
        hello = ws.receive_json()
        assert hello["type"] == "hello"
        assert hello["clients"] == 1

        client.post("/api/write", content='cpu{host="a"} 5')
        event = ws.receive_json()
        assert event["type"] == "samples"
        assert event["name"] == 'cpu{host="a"}'
        assert event["v"] == 5.0
        assert event["count"] == 1


def test_websocket_pushes_alerts(stack):
    client, _, _ = stack
    client.post("/api/write", content="cpu 90")
    client.post("/api/rules", json={"name": "hot", "metric": "cpu", "op": ">", "threshold": 50, "windowSec": 300})

    with client.websocket_connect("/api/live") as ws:
        ws.receive_json()  # hello
        client.post("/api/rules/check")
        event = ws.receive_json()
        assert event["type"] == "alert"
        assert event["ruleName"] == "hot"


@pytest.mark.integration
def test_sse_stream_over_a_real_server(tmp_path):
    """SSE against a live uvicorn, not TestClient.

    ``TestClient`` buffers a streaming response body before returning, so an
    endpoint whose generator blocks on a queue looks like a hang under it. That
    is a limitation of the test transport, not of the endpoint -- so this one
    runs the real ASGI server on a spare port and reads the stream with httpx,
    which is how a browser actually consumes it.
    """
    import contextlib
    import socket
    import threading

    import httpx
    import uvicorn

    db = DB(tmp_path / "data")
    rules = RulesEngine(DBQuerier(db), interval_sec=3600)
    app = create_app(db, rules)

    with contextlib.closing(socket.socket()) as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]

    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()

    try:
        base = f"http://127.0.0.1:{port}"
        deadline = time.time() + 10
        while time.time() < deadline:
            try:
                if httpx.get(f"{base}/healthz", timeout=1).status_code == 200:
                    break
            except httpx.HTTPError:
                time.sleep(0.1)
        else:
            raise AssertionError("server did not become ready")

        with httpx.stream("GET", f"{base}/api/stream", timeout=10) as resp:
            assert resp.status_code == 200
            assert resp.headers["content-type"].startswith("text/event-stream")

            events: list[str] = []
            lines = resp.iter_lines()
            first = next(lines)
            assert "hello" in first

            # Subscribe first, then write: the feed carries events published
            # after the client attached.
            assert httpx.post(f"{base}/api/write", content='cpu{host="a"} 7', timeout=5).status_code == 200

            for line in lines:
                if line.startswith("data:"):
                    events.append(line)
                if events:
                    break
            assert "samples" in events[0]
            assert "42" not in events[0]  # the value we wrote was 7, not 42
            assert "7.0" in events[0]
    finally:
        server.should_exit = True
        thread.join(timeout=10)
        rules.stop()
        db.close()


def test_hub_drops_a_client_that_cannot_keep_up():
    """An unbounded queue would turn one stalled browser into a memory leak."""
    hub = Hub(queue_size=4)
    hub.bind_loop(_LoopStub())
    _, queue = hub.subscribe()

    for i in range(10):
        hub._publish_now({"type": "samples", "i": i})  # publish directly: no loop in tests

    assert hub.dropped == 1
    assert hub.client_count == 0
    assert queue.qsize() == 4  # the first four made it


class _LoopStub:
    """Stands in for an event loop; ``_publish_now`` is called synchronously."""


def test_hub_publish_is_a_noop_before_a_loop_is_bound():
    hub = Hub()
    hub.publish({"type": "samples"})
    assert hub.published == 0


# ------------------------------------------------------------------ dashboard


def test_dashboard_is_served(stack):
    client, _, _ = stack
    resp = client.get("/")
    assert resp.status_code == 200
    assert "Pulse" in resp.text
    assert "/api/live" in resp.text


def test_openapi_is_exposed(stack):
    client, _, _ = stack
    resp = client.get("/api/openapi.json")
    assert resp.status_code == 200
    paths = resp.json()["paths"]
    assert "/api/write" in paths
    assert "/api/query" in paths

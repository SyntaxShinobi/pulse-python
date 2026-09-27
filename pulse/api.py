"""HTTP API.

FastAPI for routing and validation, Starlette's WebSocket support for the live
feed -- which replaces a hand-written RFC 6455 implementation in the Go
original. Writing framing, masking and close codes by hand is a good way to
learn the protocol and a bad way to run a server.

The live feed has a bounded queue per client and counts drops. A dashboard tab
backgrounded on a laptop stops reading its socket, and an unbounded queue turns
that into a memory leak in the server. Dropping frames for one slow client is
the right trade for a live chart.
"""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import os

from fastapi import APIRouter, FastAPI, Header, HTTPException, Query, Request, Response, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from .engine import DB
from .query import Query as TSQuery
from .rules import AlertRule, RulesEngine

__all__ = ["Hub", "create_app", "parse_exposition"]

#: Body limit for an ingest POST, matching Prometheus scrape payloads.
MAX_WRITE_BODY = 8 << 20

#: Per-client live-feed buffer. Beyond this the client is dropped.
LIVE_QUEUE_SIZE = 256


def parse_exposition(body: str, default_ts: int) -> tuple[dict[str, tuple[list[int], list[float]]], list[str], int]:
    """Parse the Prometheus text exposition format.

    ``metric{label="value"} 12.5`` or with a trailing millisecond timestamp.
    Comment and blank lines are ignored; unparsable lines are collected as
    errors rather than aborting the batch, so one bad scrape line does not
    discard a thousand good ones.

    Returns ``(batches_by_series, errors, accepted)``.
    """
    batches: dict[str, tuple[list[int], list[float]]] = {}
    errors: list[str] = []
    accepted = 0

    for lineno, raw in enumerate(body.splitlines(), start=1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        fields = line.split()
        if len(fields) < 2:
            errors.append(f"line {lineno}: expected metric and value")
            continue
        try:
            value = float(fields[1])
        except ValueError:
            errors.append(f"line {lineno}: bad value {fields[1]!r}")
            continue
        timestamp = default_ts
        if len(fields) >= 3:
            try:
                millis = int(fields[2])
            except ValueError:
                errors.append(f"line {lineno}: bad timestamp {fields[2]!r}")
                continue
            if millis <= 0:
                errors.append(f"line {lineno}: timestamp must be positive")
                continue
            # Exposition timestamps are milliseconds; the engine stores seconds.
            timestamp = millis // 1000

        name = fields[0]
        timestamps, values = batches.setdefault(name, ([], []))
        timestamps.append(timestamp)
        values.append(value)
        accepted += 1

    return batches, errors, accepted


class Hub:
    """Fan-out of ingest events and alerts to live-feed clients.

    Publishing happens on the ingest thread, which is not the event loop
    thread, so every handoff goes through ``call_soon_threadsafe``. Touching an
    ``asyncio.Queue`` from another thread directly is a race, not a shortcut.
    """

    def __init__(self, queue_size: int = LIVE_QUEUE_SIZE) -> None:
        self._queue_size = queue_size
        self._clients: dict[int, asyncio.Queue[dict[str, Any]]] = {}
        self._loop: asyncio.AbstractEventLoop | None = None
        self._next_id = 0
        self._dropped = 0
        self._published = 0

    def bind_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop

    @property
    def client_count(self) -> int:
        return len(self._clients)

    @property
    def dropped(self) -> int:
        return self._dropped

    @property
    def published(self) -> int:
        return self._published

    def publish(self, event: dict[str, Any]) -> None:
        """Push an event to every client. Thread-safe."""
        if self._loop is None or not self._clients:
            return
        try:
            self._loop.call_soon_threadsafe(self._publish_now, event)
        except RuntimeError:
            return  # loop already closed during shutdown

    def _publish_now(self, event: dict[str, Any]) -> None:
        self._published += 1
        for client_id, queue in list(self._clients.items()):
            try:
                queue.put_nowait(event)
            except asyncio.QueueFull:
                # A client that cannot keep up is dropped rather than allowed
                # to grow the queue without bound.
                self._dropped += 1
                self._clients.pop(client_id, None)

    def subscribe(self) -> tuple[int, asyncio.Queue[dict[str, Any]]]:
        self._next_id += 1
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=self._queue_size)
        self._clients[self._next_id] = queue
        return self._next_id, queue

    def unsubscribe(self, client_id: int) -> None:
        self._clients.pop(client_id, None)


def create_app(db: DB, rules: RulesEngine, api_key: str | None = None) -> FastAPI:
    """Build the ASGI application."""
    hub = Hub()
    counters = {"requests": 0, "writes": 0, "samples_rx": 0, "queries": 0}
    started = time.time()

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        # The hub publishes from ingest threads, so it needs the running loop
        # to hand events over safely. Captured here, at startup.
        hub.bind_loop(asyncio.get_running_loop())
        yield

    app = FastAPI(
        title="Pulse",
        version="0.1.0",
        docs_url="/api/docs",
        openapi_url="/api/openapi.json",
        lifespan=lifespan,
    )
    app.state.db = db
    app.state.rules = rules
    app.state.hub = hub

    @app.middleware("http")
    async def _count(request: Request, call_next):
        counters["requests"] += 1
        return await call_next(request)

    def authorize(authorization: str | None, x_api_key: str | None) -> None:
        """Reject mutating requests when a key is configured."""
        if api_key is None:
            return
        supplied = x_api_key
        if supplied is None and authorization and authorization.startswith("Bearer "):
            supplied = authorization.removeprefix("Bearer ").strip()
        if supplied != api_key:
            raise HTTPException(status_code=401, detail="invalid or missing api key")

    # ------------------------------------------------------------- plumbing

    def json_error(status: int, detail: str) -> JSONResponse:
        return JSONResponse(status_code=status, content={"error": detail})

    router = APIRouter()

    @router.get("/healthz")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @router.get("/api/stats")
    def stats() -> dict[str, Any]:
        out = dict(db.stats())
        out.update(
            {
                "live_clients": hub.client_count,
                "clients_dropped": hub.dropped,
                "events_published": hub.published,
                "http_requests": counters["requests"],
                "writes": counters["writes"],
                "samples_received": counters["samples_rx"],
                "queries": counters["queries"],
                "server_uptime_seconds": int(time.time() - started),
            }
        )
        out.update({f"rules_{k}": v for k, v in rules.stats().items()})
        return out

    @router.get("/api/series")
    def series() -> dict[str, Any]:
        return {"series": db.all_series()}

    # ---------------------------------------------------------------- ingest

    @router.post("/api/write")
    async def write(
        request: Request,
        authorization: str | None = Header(default=None),
        x_api_key: str | None = Header(default=None),
    ) -> Any:
        authorize(authorization, x_api_key)
        body = (await request.body())[:MAX_WRITE_BODY]
        try:
            text = body.decode("utf-8")
        except UnicodeDecodeError as exc:
            return json_error(400, f"body is not valid UTF-8: {exc}")

        batches, errors, accepted = parse_exposition(text, int(time.time()))

        # One WAL fsync per series rather than per sample. The fsync is the
        # expensive part of ingest, so batching is the whole point.
        written = 0
        for name, (timestamps, values) in batches.items():
            try:
                db.write(name, timestamps, values)
            except ValueError as exc:
                return json_error(400, f"{name}: {exc}")
            written += len(timestamps)

        # The live feed is driven by db.subscribe, so every accepted batch --
        # from this handler or any other caller -- reaches clients exactly once.
        counters["writes"] += 1
        counters["samples_rx"] += accepted

        payload: dict[str, Any] = {"accepted": accepted, "skipped": len(errors), "series": len(batches)}
        if errors:
            payload["errors"] = errors[:20]
        return payload

    # ----------------------------------------------------------------- query

    @router.get("/api/query")
    @router.get("/api/query_range")
    def query_range(
        request: Request,
        metric: str = Query(..., description="Bare metric name, e.g. cpu_usage"),
        start: int | None = None,
        end: int | None = None,
        step: int = 0,
        agg: str = "avg",
    ) -> Any:
        now = int(time.time())
        labels = {
            key.removeprefix("label_"): value
            for key, value in request.query_params.items()
            if key.startswith("label_")
        }
        try:
            results = db.query(
                TSQuery(
                    metric=metric,
                    labels=labels,
                    start=start if start is not None else now - 3600,
                    end=end if end is not None else now,
                    step=step,
                    agg=agg,
                )
            )
        except ValueError as exc:
            return json_error(400, str(exc))

        counters["queries"] += 1
        return {
            "metric": metric,
            "agg": agg,
            "step": step,
            "start": start if start is not None else now - 3600,
            "end": end if end is not None else now,
            "result": [r.as_dict() for r in results],
        }

    # ----------------------------------------------------------------- rules

    @router.get("/api/rules")
    def list_rules() -> dict[str, Any]:
        return {"rules": [r.as_dict() for r in rules.list()]}

    @router.post("/api/rules", status_code=201)
    async def add_rule(
        request: Request,
        authorization: str | None = Header(default=None),
        x_api_key: str | None = Header(default=None),
    ) -> Any:
        authorize(authorization, x_api_key)
        try:
            payload = await request.json()
        except json.JSONDecodeError as exc:
            return json_error(400, f"invalid JSON: {exc}")
        if not isinstance(payload, dict):
            return json_error(400, "body must be a JSON object")
        if not payload.get("name") or not payload.get("metric"):
            return json_error(400, "name and metric are required")
        try:
            rule = AlertRule(
                id=str(payload.get("id") or ""),
                name=str(payload["name"]),
                metric=str(payload["metric"]),
                labels=payload.get("labels") or {},
                agg=payload.get("agg", "avg"),
                window_sec=int(payload.get("windowSec", 60)),
                op=payload.get("op", ">"),
                threshold=float(payload.get("threshold", 0.0)),
                min_count=int(payload.get("minCount", 1)),
                enabled=True,
            )
        except (ValueError, TypeError) as exc:
            return json_error(400, str(exc))
        try:
            rules.add(rule)
        except ValueError as exc:
            return json_error(409, str(exc))
        return rule.as_dict()

    @router.delete("/api/rules/{rule_id}", status_code=204)
    def delete_rule(
        rule_id: str,
        authorization: str | None = Header(default=None),
        x_api_key: str | None = Header(default=None),
    ) -> Response:
        # The annotation has to be Response, not Any: FastAPI infers a response
        # model from the return type, and a 204 is not allowed to have one --
        # the route fails at app-construction time, not at request time.
        authorize(authorization, x_api_key)
        if not rules.remove(rule_id):
            return json_error(404, "no such rule")
        return Response(status_code=204)

    @router.get("/api/alerts")
    def alerts() -> dict[str, Any]:
        return {"alerts": [a.as_dict() for a in rules.alerts()]}

    @router.post("/api/rules/check")
    def check_now(
        authorization: str | None = Header(default=None),
        x_api_key: str | None = Header(default=None),
    ) -> dict[str, Any]:
        authorize(authorization, x_api_key)
        fired = rules.evaluate_once()
        for alert in fired:
            hub.publish({"type": "alert", **alert.as_dict()})
        return {"ok": True, "fired": len(fired), "stats": rules.stats()}

    # ------------------------------------------------------------- live feed

    @app.websocket("/api/live")
    async def live(websocket: WebSocket) -> None:
        await websocket.accept()
        client_id, queue = hub.subscribe()
        try:
            await websocket.send_text(json.dumps({"type": "hello", "clients": hub.client_count}))
            while True:
                event = await queue.get()
                await websocket.send_text(json.dumps(event, default=str))
        except (WebSocketDisconnect, RuntimeError):
            pass  # client went away; unsubscribe in finally
        finally:
            hub.unsubscribe(client_id)

    @router.get("/api/stream")
    async def stream() -> StreamingResponse:
        """Server-sent events: the same feed, for clients without WebSocket."""

        async def events() -> AsyncIterator[str]:
            client_id, queue = hub.subscribe()
            yield f"data: {json.dumps({'type': 'hello', 'clients': hub.client_count})}\n\n"
            try:
                while True:
                    event = await queue.get()
                    yield f"data: {json.dumps(event, default=str)}\n\n"
            except asyncio.CancelledError:
                pass
            finally:
                hub.unsubscribe(client_id)

        return StreamingResponse(
            events(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    app.include_router(router)

    # Dashboard last, so /api/* is not shadowed by the static mount.
    static_dir = os.path.join(os.path.dirname(__file__), "static")
    if os.path.isdir(static_dir):
        app.mount("/static", StaticFiles(directory=static_dir), name="static")

        @app.get("/", include_in_schema=False)
        def dashboard() -> FileResponse:
            return FileResponse(os.path.join(static_dir, "index.html"))

    # Feed ingest and alerts into the hub.
    def on_batch(series_id: int, timestamps, values) -> None:
        hub.publish(
            {
                "type": "samples",
                "series": series_id,
                "name": db.series_name(series_id),
                "count": len(timestamps),
                "t": timestamps[-1],
                "v": values[-1],
            }
        )

    db.subscribe(on_batch)
    rules.on_alert(lambda alert: hub.publish({"type": "alert", **alert.as_dict()}))

    return app

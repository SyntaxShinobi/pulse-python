"""Generate realistic load against a running Pulse server.

    python -m pulse.cli.loadgen --url http://localhost:8080 --hosts 8

Five metrics per host, chosen because compression is a property of the *data*
and not of the codec, so a generator that only emits sine waves measures
nothing interesting:

``cpu_usage``, ``load_1m``, ``temp_c``
    Bounded walks that hover, so the XOR deltas stay narrow.

``mem_used_bytes``
    A large integer drifting slowly -- big absolute value, tiny change, which
    is where the float encoding earns its keep.

``requests_total``
    A monotonic counter that repeats the same whole number most ticks. This is
    the shape behind the Gorilla paper's 1.37 B/sample, and it compresses far
    better than a drifting float.
"""

from __future__ import annotations

import argparse
import math
import os
import random
import sys
import time
import urllib.error
import urllib.request

__all__ = ["HostState", "build_batch", "main", "post"]

#: Metrics emitted per host per tick.
METRICS_PER_HOST = 5


class HostState:
    """Per-host walk state, so a series looks continuous across batches."""

    def __init__(self, host: str, rng: random.Random) -> None:
        self.host = host
        self.cpu = rng.uniform(20, 60)
        self.mem = rng.uniform(1.5e9, 3.0e9)
        self.requests = rng.randint(1000, 5000)
        self._rng = rng

    def step(self, t: float) -> dict[str, float]:
        # Mean-reverting so the walk cannot wander off scale over a long run.
        self.cpu += self._rng.gauss(0, 2.5) + (45 - self.cpu) * 0.05
        self.cpu = max(0.0, min(100.0, self.cpu))

        self.mem += self._rng.gauss(0, 4e6) + (2.2e9 - self.mem) * 0.001
        self.mem = max(1e9, min(8e9, self.mem))

        # Counter: idle on most ticks, so the value repeats exactly.
        if self._rng.random() < 0.3:
            self.requests += self._rng.randint(1, 40)

        return {
            "cpu_usage": round(self.cpu, 4),
            "load_1m": round(max(0.0, self.cpu / 25 + self._rng.gauss(0, 0.2)), 3),
            "temp_c": round(38 + 12 * math.sin(t / 600) + self._rng.gauss(0, 0.4), 2),
            "mem_used_bytes": float(int(self.mem)),
            "requests_total": float(self.requests),
        }


def build_batch(hosts: list[HostState], now: int, stamp: bool = False) -> str:
    """Render one payload in the Prometheus text exposition format."""
    lines: list[str] = ["# pulse loadgen", f"# generated at {now}"]
    for state in hosts:
        values = state.step(now)
        labels = f'{{host="{state.host}",job="pulse-loadgen",region="ap-south-1"}}'
        for metric, value in values.items():
            line = f"{metric}{labels} {value!r}"
            if stamp:
                line += f" {now * 1000}"
            lines.append(line)
    return "\n".join(lines) + "\n"


def post(url: str, body: str, api_key: str | None, timeout: float) -> tuple[int, str]:
    """POST one batch. Returns ``(status, body)``; status 0 means no response."""
    req = urllib.request.Request(
        url,
        data=body.encode("utf-8"),
        headers={"Content-Type": "text/plain; version=0.0.4"},
        method="POST",
    )
    if api_key:
        req.add_header("X-Api-Key", api_key)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode("utf-8", "replace")
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        return 0, str(exc)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Pulse load generator")
    parser.add_argument("--url", default="http://localhost:8080", help="server base URL")
    parser.add_argument("--hosts", type=int, default=8, help="simulated hosts")
    parser.add_argument("--interval", type=float, default=1.0, help="seconds between batches")
    parser.add_argument("--batches", type=int, default=0, help="stop after N batches (0 = forever)")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--api-key", default=os.environ.get("PULSE_API_KEY"))
    parser.add_argument("--timestamp", action="store_true", help="include explicit timestamps")
    parser.add_argument("--timeout", type=float, default=10.0)
    args = parser.parse_args(argv)

    rng = random.Random(args.seed)
    hosts = [HostState(f"host-{i:02d}", rng) for i in range(args.hosts)]
    endpoint = args.url.rstrip("/") + "/api/write"
    per_batch = args.hosts * METRICS_PER_HOST

    print(
        f"loadgen: {args.hosts} hosts x {METRICS_PER_HOST} metrics = {per_batch} samples/batch "
        f"-> {endpoint} every {args.interval}s",
        flush=True,
    )

    batches = sent = failed = 0
    started = time.time()
    next_tick = started

    try:
        while args.batches == 0 or batches < args.batches:
            now = int(time.time())
            status, text = post(endpoint, build_batch(hosts, now, args.timestamp), args.api_key, args.timeout)
            batches += 1
            if status == 200:
                sent += per_batch
            else:
                failed += 1
                print(f"loadgen: batch {batches} -> HTTP {status}: {text[:200]}", flush=True)

            if batches == 1 or batches % 10 == 0:
                elapsed = time.time() - started
                rate = sent / elapsed if elapsed > 0 else 0.0
                print(f"loadgen: batches={batches} sent={sent} failed={failed} {rate:,.0f} samples/s", flush=True)

            next_tick += args.interval
            delay = next_tick - time.time()
            if delay > 0:
                time.sleep(delay)
            else:
                # Falling behind: skip the missed ticks instead of bursting.
                next_tick = time.time()
    except KeyboardInterrupt:
        pass

    elapsed = time.time() - started
    rate = sent / elapsed if elapsed > 0 else 0.0
    print(
        f"\nloadgen: done. {batches} batches, {sent} samples accepted, {failed} failed, "
        f"{rate:,.0f} samples/s over {elapsed:.1f}s",
        flush=True,
    )
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())

"""Run the Pulse server.

    python -m pulse.cli.server --data ./pulse-data --port 8080

Set ``PULSE_API_KEY`` (or pass ``--api-key``) to require a key on mutating
endpoints -- ``POST /api/write`` and the rule routes. Reads stay open so the
dashboard works without credentials.
"""

from __future__ import annotations

import argparse
import os
import signal
import sys

from pulse.api import create_app
from pulse.engine import DB
from pulse.rules import DBQuerier, RulesEngine

__all__ = ["build", "main"]


def build(data_dir: str, api_key: str | None = None, rule_interval: float = 5.0):
    """Wire engine, rules and API together. Returns ``(app, db, rules)``."""
    db = DB(data_dir)
    rules = RulesEngine(DBQuerier(db), interval_sec=rule_interval)
    rules.start()
    app = create_app(db, rules, api_key=api_key)
    return app, db, rules


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Pulse time-series server")
    parser.add_argument("--data", default=os.environ.get("PULSE_DATA", "./pulse-data"), help="data directory")
    parser.add_argument("--host", default=os.environ.get("PULSE_HOST", "0.0.0.0"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("PULSE_PORT", "8080")))
    parser.add_argument("--api-key", default=os.environ.get("PULSE_API_KEY"), help="require this key for writes")
    parser.add_argument("--rule-interval", type=float, default=5.0, help="seconds between rule evaluations")
    args = parser.parse_args(argv)

    import uvicorn

    app, db, rules = build(args.data, args.api_key, args.rule_interval)

    def shutdown(signum: int, _frame) -> None:
        # Compact before exiting: seal the head and truncate the WAL, so the
        # next start does not replay samples that are already durable.
        print(f"\npulse: caught signal {signum}; compacting and closing", flush=True)
        rules.stop()
        db.close()
        os._exit(0)

    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, shutdown)

    stats = db.stats()
    print(
        f"pulse: data={args.data} series={stats['series']} "
        f"recovered={stats['recovered_samples']} samples, {stats['sealed_blocks']} sealed blocks",
        flush=True,
    )
    print(f"pulse: listening on http://{args.host}:{args.port} (dashboard at /)", flush=True)

    uvicorn.run(app, host=args.host, port=args.port, log_level="info")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

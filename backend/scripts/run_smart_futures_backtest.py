#!/usr/bin/env python3
"""
Run Smart Futures backtest for a date range (IST scan times per day).

Examples:
  PYTHONPATH=. python backend/scripts/run_smart_futures_backtest.py \\
    --from-date 2026-02-12 --to-date 2026-03-06 --times 09:30,10:30

Does not start the web server; uses DB + Upstox like the live picker (read-only on sentiment tables).
Session dates must be >= 2026-02-01. For 2026-02-01..2026-03-31, April-2026 expiry FUTs are taken from
nse_instruments.json; from 2026-04-01 onward, currmth_future_* from arbitrage_master.
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import date

# Project root on sys.path when run as `python backend/scripts/...` from repo root
_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

os.environ.setdefault("PYTHONPATH", _ROOT)


def main() -> int:
    import backend.env_bootstrap  # noqa: F401

    from backend.database import SessionLocal
    from backend.services.smart_futures_backtest.engine import (
        BACKTEST_MIN_SESSION_DATE,
        run_backtest_date_range,
        validate_backtest_date_bounds,
    )

    p = argparse.ArgumentParser(description="Smart Futures backtest runner")
    p.add_argument(
        "--from-date",
        dest="from_date",
        required=True,
        help=f"YYYY-MM-DD (inclusive), >= {BACKTEST_MIN_SESSION_DATE.isoformat()}",
    )
    p.add_argument(
        "--to-date",
        dest="to_date",
        required=True,
        help=f"YYYY-MM-DD (inclusive), >= {BACKTEST_MIN_SESSION_DATE.isoformat()}",
    )
    p.add_argument(
        "--times",
        default="09:30,10:30",
        help="Comma-separated IST clock labels, e.g. 09:30,10:30",
    )
    p.add_argument("--throttle", type=float, default=0.04, help="Seconds between symbols (default 0.04)")
    args = p.parse_args()

    d0 = date.fromisoformat(args.from_date.strip())
    d1 = date.fromisoformat(args.to_date.strip())
    times = tuple(t.strip() for t in args.times.split(",") if t.strip())

    ve = validate_backtest_date_bounds(d0, d1)
    if ve:
        print("ERROR:", ve, file=sys.stderr)
        return 2

    db = SessionLocal()
    try:
        out = run_backtest_date_range(db, d0, d1, times, throttle_sec=args.throttle)
        if out.get("error"):
            print("ERROR:", out["error"], file=sys.stderr)
            return 1
        print("ok_slots", out.get("ok_slots"), "total_slots", out.get("total_slots"))
        for r in out.get("results") or []:
            print(r)
        return 0
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())

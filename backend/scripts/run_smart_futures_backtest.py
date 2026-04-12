#!/usr/bin/env python3
"""
Run Smart Futures backtest for a date range (IST scan times per day).

Examples:
  PYTHONPATH=. python backend/scripts/run_smart_futures_backtest.py \\
    --from-date 2026-02-12 --to-date 2026-03-06 --times 09:30,10:30

Does not start the web server; uses DB + Upstox like the live picker (read-only on sentiment tables).
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
    from backend.services.smart_futures_backtest.engine import run_backtest_date_range

    p = argparse.ArgumentParser(description="Smart Futures backtest runner")
    p.add_argument("--from-date", dest="from_date", required=True, help="YYYY-MM-DD (inclusive)")
    p.add_argument("--to-date", dest="to_date", required=True, help="YYYY-MM-DD (inclusive)")
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

    db = SessionLocal()
    try:
        out = run_backtest_date_range(db, d0, d1, times, throttle_sec=args.throttle)
        print("ok_slots", out.get("ok_slots"), "total_slots", out.get("total_slots"))
        for r in out.get("results") or []:
            print(r)
        return 0
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())

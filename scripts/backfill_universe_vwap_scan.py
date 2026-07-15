#!/usr/bin/env python3
"""Backfill kavach_universe_vwap_scan for recent sessions (Upstox historical 5m).

Usage (on paperclip / local with DB + Upstox token):
  python3 scripts/backfill_universe_vwap_scan.py --dates 2026-07-13,2026-07-14,2026-07-15

Research-only. Does not touch live gates. ~200 symbols × 1 fetch/day paced by --pace.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--dates",
        default="2026-07-13,2026-07-14,2026-07-15",
        help="Comma-separated YYYY-MM-DD session dates",
    )
    p.add_argument(
        "--pace",
        type=float,
        default=0.25,
        help="Seconds between Upstox historical fetches (default 0.25)",
    )
    args = p.parse_args()
    dates = [d.strip() for d in args.dates.split(",") if d.strip()]
    from backend.services.kavach_universe_vwap_scan import backfill_universe_vwap_scan

    out = backfill_universe_vwap_scan(dates, pace_sec=args.pace)
    print(json.dumps(out, indent=2, default=str))
    return 0 if out.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())

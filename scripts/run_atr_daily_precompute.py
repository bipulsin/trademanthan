#!/usr/bin/env python3
"""CLI: run ATR(14)% nightly precompute or backfill.

Examples:
  PYTHONPATH=. python scripts/run_atr_daily_precompute.py --as-of 2026-07-25
  PYTHONPATH=. python scripts/run_atr_daily_precompute.py --backfill 2026-07-20 2026-07-24
  PYTHONPATH=. python scripts/run_atr_daily_precompute.py --status 2026-07-25
"""
from __future__ import annotations

import argparse
import json
import sys


def main() -> int:
    ap = argparse.ArgumentParser(description="ATR daily precompute job / backfill")
    ap.add_argument("--as-of", help="as_of_date YYYY-MM-DD (default: next NSE session)")
    ap.add_argument(
        "--backfill",
        nargs=2,
        metavar=("START", "END"),
        help="Backfill inclusive date range and patch rs_scanner_history zeros",
    )
    ap.add_argument("--status", metavar="DATE", help="Print nightly status for as_of_date")
    ap.add_argument(
        "--no-patch-history",
        action="store_true",
        help="With --backfill, skip rs_scanner_history zero patch",
    )
    args = ap.parse_args()

    from backend.services.atr_daily_precompute import (
        ensure_atr_daily_precompute_tables,
        get_nightly_status,
        run_atr_daily_precompute_backfill,
        run_atr_daily_precompute_job,
    )

    ensure_atr_daily_precompute_tables()

    if args.status:
        print(json.dumps(get_nightly_status(args.status), indent=2, default=str))
        return 0
    if args.backfill:
        out = run_atr_daily_precompute_backfill(
            args.backfill[0],
            args.backfill[1],
            trigger="cli_backfill",
            patch_rs_scanner_history=not args.no_patch_history,
        )
        print(json.dumps(out, indent=2, default=str))
        return 0 if out.get("ok") else 1

    out = run_atr_daily_precompute_job(as_of_date=args.as_of, trigger="cli")
    print(json.dumps(out, indent=2, default=str))
    return 0 if out.get("ok") else 1


if __name__ == "__main__":
    sys.exit(main())

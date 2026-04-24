#!/usr/bin/env python3
"""
One-time: Re-point daily_futures_screening and open trades (bought) to next-month FUT
from arbitrage_master for a given trade_date. Run after code switches Daily Futures
to use nextmth columns.

  cd /path/to/trademanthan && source venv/bin/activate
  python3 backend/scripts/migrate_daily_futures_next_month.py
  python3 backend/scripts/migrate_daily_futures_next_month.py --date 2026-04-24
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))
os.chdir(project_root)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--date",
        metavar="YYYY-MM-DD",
        help="IST trade date (default: today in Asia/Kolkata)",
    )
    args = p.parse_args()
    if args.date:
        y, m, d = (int(x) for x in args.date.split("-"))
        td = date(y, m, d)
    else:
        from backend.services.daily_futures_service import ist_today

        td = ist_today()

    from backend.services.daily_futures_service import retarget_daily_futures_to_next_month_for_date

    out = retarget_daily_futures_to_next_month_for_date(td)
    print(json.dumps(out, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""
One-off: set alert_time to 13:15 IST for specific today's CE rows; delete SUZLON 50 CE.

Usage (repo root):
  PYTHONPATH=. python3 backend/scripts/adjust_scan_alert_times_and_remove_suzlon.py --dry-run
  PYTHONPATH=. python3 backend/scripts/adjust_scan_alert_times_and_remove_suzlon.py --apply

Optional:
  --date YYYY-MM-DD   (default: today IST)
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pytz

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import and_

from backend.database import SessionLocal
from backend.models.trading import IntradayStockOption

IST = pytz.timezone("Asia/Kolkata")

# (stock_name_upper, strike)
UPDATE_SPECS = [
    ("ADANIGREEN", 1100.0),
    ("CUMMINSIND", 5200.0),
    ("FEDERALBNK", 295.0),
    ("TITAN", 4600.0),
    ("TVSMOTOR", 3900.0),
]

DELETE_SPEC = ("SUZLON", 50.0)


def _day_bounds_ist(day_str: str | None):
    ist = IST
    if day_str:
        d = datetime.strptime(day_str, "%Y-%m-%d")
        today = ist.localize(d.replace(hour=0, minute=0, second=0, microsecond=0))
    else:
        today = datetime.now(ist).replace(hour=0, minute=0, second=0, microsecond=0)
    end = today + timedelta(days=1)
    alert_1315 = today.replace(hour=13, minute=15, second=0, microsecond=0)
    return today, end, alert_1315


def _strike_match(row: IntradayStockOption, strike: float) -> bool:
    if row.option_strike is None:
        return False
    try:
        return abs(float(row.option_strike) - float(strike)) < 0.02
    except (TypeError, ValueError):
        return False


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None, help="Trading date YYYY-MM-DD (IST), default today")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    if args.apply and args.dry_run:
        print("Use only one of --apply or --dry-run")
        return 1

    today, end, alert_1315 = _day_bounds_ist(args.date)

    db = SessionLocal()
    try:
        rows = (
            db.query(IntradayStockOption)
            .filter(
                and_(
                    IntradayStockOption.trade_date >= today,
                    IntradayStockOption.trade_date < end,
                )
            )
            .all()
        )

        print(f"IST day window: {today.isoformat()} .. {end.isoformat()}")
        print(f"Target alert_time: {alert_1315.isoformat()}")

        updated = 0
        for sym, strike in UPDATE_SPECS:
            matches = [
                r
                for r in rows
                if (r.stock_name or "").strip().upper() == sym
                and (r.option_type or "").upper() == "CE"
                and _strike_match(r, strike)
            ]
            if not matches:
                print(f"⚠️ No row: {sym} {strike} CE")
                continue
            for r in matches:
                old = r.alert_time
                print(
                    f"  UPDATE id={r.id} {sym} strike={r.option_strike} "
                    f"alert_time {old} -> {alert_1315}"
                )
                if args.apply:
                    r.alert_time = alert_1315
                    updated += 1

        # Delete SUZLON 50 CE
        del_rows = [
            r
            for r in rows
            if (r.stock_name or "").strip().upper() == DELETE_SPEC[0]
            and (r.option_type or "").upper() == "CE"
            and _strike_match(r, DELETE_SPEC[1])
        ]
        for r in del_rows:
            print(f"  DELETE id={r.id} SUZLON 50 CE (alert was {r.alert_time})")
            if args.apply:
                db.delete(r)

        if args.apply:
            db.commit()
            print(f"✅ Committed: {updated} alert_time updates, {len(del_rows)} deleted")
        elif not args.dry_run:
            print("Pass --apply to commit or --dry-run to preview without DB writes")
        else:
            print("Dry-run: no changes written")

        return 0
    except Exception as e:
        print(f"❌ {e}")
        db.rollback()
        return 1
    finally:
        db.close()


if __name__ == "__main__":
    sys.exit(main())

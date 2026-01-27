#!/usr/bin/env python3
"""
Query intraday_stock_options for exit_reason (and related fields) for a given stock/contract.
Usage:
  # From repo root (or with PYTHONPATH including project root):
  python backend/scripts/check_exit_reason.py [STOCK_NAME]
  python backend/scripts/check_exit_reason.py MARUTI
  python backend/scripts/check_exit_reason.py MARUTI 15000 PE   # narrow to contract
"""
import sys
from datetime import datetime
import pytz

def main():
    stock_name = (sys.argv[1] if len(sys.argv) > 1 else "MARUTI").strip().upper()
    strike_filter = sys.argv[2] if len(sys.argv) > 2 else None   # e.g. 15000
    opt_type_filter = (sys.argv[3] if len(sys.argv) > 3 else "").strip().upper()  # CE or PE

    # Use project's DB
    try:
        from backend.database import SessionLocal
        from backend.models.trading import IntradayStockOption
    except Exception as e:
        print("Run from repo root or set PYTHONPATH to project root.")
        print("Example: cd /path/to/TradeManthan && python backend/scripts/check_exit_reason.py MARUTI")
        raise SystemExit(1) from e

    ist = pytz.timezone("Asia/Kolkata")
    today = datetime.now(ist).replace(hour=0, minute=0, second=0, microsecond=0)

    db = SessionLocal()
    try:
        q = db.query(IntradayStockOption).filter(
            IntradayStockOption.stock_name == stock_name,
            IntradayStockOption.trade_date >= today,
        ).order_by(IntradayStockOption.alert_time.desc())

        rows = q.all()
        if strike_filter:
            rows = [r for r in rows if r.option_contract and strike_filter in str(r.option_contract)]
        if opt_type_filter:
            rows = [r for r in rows if (r.option_type or "") == opt_type_filter or (opt_type_filter in (r.option_contract or ""))]

        if not rows:
            print(f"No rows for {stock_name}" + (f" strike ~{strike_filter} {opt_type_filter}" if strike_filter or opt_type_filter else ""))
            return

        print(f"Found {len(rows)} row(s) for {stock_name}")
        print("-" * 80)
        for r in rows:
            sell_ts = r.sell_time.strftime("%Y-%m-%d %H:%M IST") if r.sell_time else None
            alert_ts = r.alert_time.strftime("%Y-%m-%d %H:%M IST") if r.alert_time else None
            print(f"  option_contract : {r.option_contract}")
            print(f"  alert_time     : {alert_ts}")
            print(f"  exit_reason    : {r.exit_reason}")
            print(f"  sell_time      : {sell_ts}")
            print(f"  status         : {r.status}")
            print(f"  buy_price      : {r.buy_price}  sell_price: {r.sell_price}  pnl: {r.pnl}")
            print("-" * 80)
    finally:
        db.close()

if __name__ == "__main__":
    main()

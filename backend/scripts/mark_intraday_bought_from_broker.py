#!/usr/bin/env python3
"""
Mark an intraday_stock_options row as bought using today's Upstox BUY fill (order book / order id).

Usage (repo root, server has DATABASE_URL + Upstox token):

  python backend/scripts/mark_intraday_bought_from_broker.py --stock BOSCHLTD --strike 28000 --type PE --dry-run
  python backend/scripts/mark_intraday_bought_from_broker.py --stock BOSCHLTD --strike 28000 --type PE --apply

Optional: --date YYYY-MM-DD (IST trade_date), default today.
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta

import pytz


def _matches(row, stock: str, strike: float, opt_type: str) -> bool:
    if (row.stock_name or "").strip().upper() != stock.strip().upper():
        return False
    if (row.option_type or "").upper() != opt_type.upper():
        return False
    if row.option_strike is not None and float(row.option_strike) > 1.0:
        return abs(float(row.option_strike) - strike) < 0.02
    oc = (row.option_contract or "").replace(" ", "").upper()
    strike_s = str(int(strike)) if strike == int(strike) else str(strike).rstrip("0").rstrip(".")
    return strike_s in oc


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--stock", required=True, help="e.g. BOSCHLTD")
    parser.add_argument("--strike", type=float, required=True)
    parser.add_argument("--type", default="PE", choices=("PE", "CE"))
    parser.add_argument("--date", default=None, help="Trade date YYYY-MM-DD (IST)")
    parser.add_argument(
        "--expiry",
        default="2026-04-28",
        help="Option expiry YYYY-MM-DD if instrument_key is empty (builds NSE_FO key)",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    if args.apply and args.dry_run:
        print("Use either --apply or --dry-run")
        return 1

    from pathlib import Path

    project_root = Path(__file__).resolve().parent.parent.parent
    sys.path.insert(0, str(project_root))

    from backend.database import SessionLocal
    from backend.models.trading import IntradayStockOption
    from backend.services import live_trading
    from sqlalchemy import and_

    ist = pytz.timezone("Asia/Kolkata")
    if args.date:
        day = datetime.strptime(args.date, "%Y-%m-%d")
        today = ist.localize(day.replace(hour=0, minute=0, second=0, microsecond=0))
    else:
        now = datetime.now(ist)
        today = now.replace(hour=0, minute=0, second=0, microsecond=0)

    end = today + timedelta(days=1)

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
            .order_by(IntradayStockOption.id.desc())
            .all()
        )
        matches = [r for r in rows if _matches(r, args.stock, args.strike, args.type)]
        if not matches:
            print(f"No row for {args.stock} {args.strike} {args.type} on {today.date()}")
            return 1
        row = matches[0]
        print(
            f"Match id={row.id} status={row.status!r} instrument_key={row.instrument_key!r} "
            f"contract={row.option_contract!r} qty={row.qty} buy_order_id={row.buy_order_id!r}"
        )
        if not (row.instrument_key or "").strip() and upstox_service:
            exp_dt = datetime.strptime(args.expiry, "%Y-%m-%d")
            ik = upstox_service.get_option_instrument_key(
                args.stock.strip().upper(), exp_dt, args.strike, args.type
            )
            if ik:
                row.instrument_key = ik
                print(f"Filled missing instrument_key → {ik}")

        if not args.apply and not args.dry_run:
            print("Pass --apply to write DB, or --dry-run to test broker match (no commit).")
            return 0

        if args.dry_run:
            ok = live_trading.apply_broker_buy_fill_to_intraday_trade(db, row)
            print(f"apply_broker_buy_fill_to_intraday_trade → {ok} (rolled back, no DB write)")
            db.rollback()
            return 0

        ok = live_trading.apply_broker_buy_fill_to_intraday_trade(db, row)
        if not ok:
            print("apply_broker_buy_fill_to_intraday_trade returned False (no matching BUY fill?)")
            db.rollback()
            return 1
        db.commit()
        print(f"✅ Updated id={row.id}: status=bought buy_price={row.buy_price} qty={row.qty} buy_order_id={row.buy_order_id}")
        return 0
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())

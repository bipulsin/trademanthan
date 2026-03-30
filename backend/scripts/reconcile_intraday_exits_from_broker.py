#!/usr/bin/env python3
"""
Reconcile intraday_stock_options rows with Upstox completed SELL (after DB was wrongly set to bought).

Usage (repo root, server has DATABASE_URL + Upstox token):

  python backend/scripts/reconcile_intraday_exits_from_broker.py --apply \\
    ADANIENT:1680:PE:stock_vwap_cross BIOCON:340:PE:stop_loss

Format: STOCK:STRIKE:PE|CE:exit_reason (exit_reason optional, default stock_vwap_cross)
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pytz


def _parse_spec(s: str):
    parts = s.split(":")
    if len(parts) < 3:
        raise SystemExit(f"Bad spec {s!r}; use STOCK:STRIKE:PE|CE[:exit_reason]")
    stock = parts[0].strip().upper()
    strike = float(parts[1])
    opt = parts[2].strip().upper()
    er = (parts[3].strip() if len(parts) > 3 else None) or "stock_vwap_cross"
    return stock, strike, opt, er


def _matches(row, stock: str, strike: float, opt_type: str) -> bool:
    if (row.stock_name or "").strip().upper() != stock:
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
    parser.add_argument(
        "specs",
        nargs="+",
        metavar="STOCK:STRIKE:PE[:exit_reason]",
        help="One or more targets for today (IST)",
    )
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--date", default=None, help="Trade date YYYY-MM-DD (IST)")
    args = parser.parse_args()
    if args.apply and args.dry_run:
        print("Use either --apply or --dry-run")
        return 1

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
        for spec in args.specs:
            stock, strike, opt, er = _parse_spec(spec)
            matches = [r for r in rows if _matches(r, stock, strike, opt)]
            if not matches:
                print(f"No row for {stock} {strike} {opt} on {today.date()}")
                continue
            row = matches[0]
            print(
                f"--- {stock} id={row.id} status={row.status!r} exit_reason={row.exit_reason!r} "
                f"sell={row.sell_price} buy={row.buy_price} qty={row.qty}"
            )
            if not args.apply:
                if args.dry_run:
                    ok = live_trading.reconcile_intraday_exit_from_broker(
                        db, row, default_exit_reason=er
                    )
                    print(f"reconcile → {ok} (rolled back)")
                    db.rollback()
                else:
                    print("Pass --apply to commit or --dry-run to test")
                continue
            ok = live_trading.reconcile_intraday_exit_from_broker(
                db, row, default_exit_reason=er
            )
            if ok:
                db.commit()
                print(f"✅ Updated id={row.id} status={row.status} sell={row.sell_price} pnl={row.pnl}")
            else:
                db.rollback()
                print(f"⚠️ No change for id={row.id} (broker SELL not found or already aligned)")
        return 0
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())

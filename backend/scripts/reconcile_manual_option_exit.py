#!/usr/bin/env python3
"""
Reconcile intraday_stock_options after you manually square off at the broker.

Updates matching rows to status=sold, exit_reason=manual, sell_time, sell_price, pnl.

Usage (from repo root on app server or locally with DATABASE_URL):

  # Preview rows (no DB writes)
  python backend/scripts/reconcile_manual_option_exit.py --dry-run

  # Apply with actual average SELL fill prices (₹ per unit) — required for --apply
  python backend/scripts/reconcile_manual_option_exit.py --apply \\
      --sell INDHOTEL=12.50 NYKAA=8.75

  # Optional: trading date (IST), default is today
  python backend/scripts/reconcile_manual_option_exit.py --dry-run --date 2026-03-30

  # Try to set sell_price from Upstox quote (instrument_key); may fail after hours
  python backend/scripts/reconcile_manual_option_exit.py --apply --fetch-ltp

Targets default to Bearish PE: INDHOTEL strike 550, NYKAA strike 230 (same as scan alerts).
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import pytz


DEFAULT_TARGETS: List[Tuple[str, float, str]] = [
    ("INDHOTEL", 550.0, "PE"),
    ("NYKAA", 230.0, "PE"),
]


def _row_matches_contract(row, stock_name: str, strike: float, opt_type: str) -> bool:
    """Match by option_strike when set; else by option_contract text (strike is often 0 in DB)."""
    if (row.option_type or "").upper() != opt_type.upper():
        return False
    if row.option_strike is not None and float(row.option_strike) > 1.0:
        return abs(float(row.option_strike) - strike) < 0.02
    oc = (row.option_contract or "").replace(" ", "")
    # e.g. "INDHOTEL550PE28APR26" or "INDHOTEL 550 PE 28 APR 26"
    strike_s = str(int(strike)) if strike == int(strike) else str(strike).rstrip("0").rstrip(".")
    return strike_s in oc or str(int(strike)) in oc


def _parse_sell_map(items: List[str]) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for item in items:
        if "=" not in item:
            raise SystemExit(f"Invalid --sell {item!r}; use STOCK=price e.g. INDHOTEL=12.5")
        k, v = item.split("=", 1)
        k = k.strip().upper()
        out[k] = float(v.strip())
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Reconcile DB after manual option exit at broker")
    parser.add_argument("--dry-run", action="store_true", help="List matches only; no updates")
    parser.add_argument("--apply", action="store_true", help="Write updates (requires --sell or --fetch-ltp)")
    parser.add_argument("--date", type=str, default=None, help="Trade date YYYY-MM-DD (IST); default today")
    parser.add_argument(
        "--sell",
        nargs="*",
        default=[],
        metavar="STOCK=PRICE",
        help="Sell fill price per unit, e.g. INDHOTEL=12.5 NYKAA=8.0",
    )
    parser.add_argument(
        "--fetch-ltp",
        action="store_true",
        help="Use Upstox quote last_price on instrument_key as sell_price (best effort)",
    )
    parser.add_argument(
        "--also-sold-vwap",
        action="store_true",
        help="Update rows already status=sold with stock_vwap_cross (fix bad prior exit rows)",
    )
    parser.add_argument(
        "--use-sell-column",
        action="store_true",
        help="Use existing sell_price on the row (mark-to-market LTP) when --sell not given; less accurate than broker fill",
    )
    args = parser.parse_args()

    try:
        from backend.database import SessionLocal
        from backend.models.trading import IntradayStockOption
        from sqlalchemy.orm.attributes import flag_modified
    except Exception as e:
        print("Run from repo root: cd TradeManthan && python backend/scripts/reconcile_manual_option_exit.py ...")
        raise SystemExit(1) from e

    ist = pytz.timezone("Asia/Kolkata")
    if args.date:
        day = datetime.strptime(args.date, "%Y-%m-%d")
        day = ist.localize(day.replace(hour=0, minute=0, second=0, microsecond=0))
    else:
        day = datetime.now(ist).replace(hour=0, minute=0, second=0, microsecond=0)
    day_end = day + timedelta(days=1)

    sell_map = _parse_sell_map(args.sell) if args.sell else {}

    upstox = None
    if args.fetch_ltp:
        try:
            from backend.services.upstox_service import upstox_service

            upstox = upstox_service
        except Exception as e:
            print(f"Cannot load Upstox: {e}", file=sys.stderr)
            raise SystemExit(1) from e

    db = SessionLocal()
    now = datetime.now(ist)
    updated = 0

    try:
        for stock_name, strike, opt_type in DEFAULT_TARGETS:
            q = db.query(IntradayStockOption).filter(
                IntradayStockOption.stock_name == stock_name,
                IntradayStockOption.option_type == opt_type,
                IntradayStockOption.trade_date >= day,
                IntradayStockOption.trade_date < day_end,
            )
            rows = [r for r in q.all() if _row_matches_contract(r, stock_name, strike, opt_type)]
            if not rows:
                print(f"[{stock_name} {strike} {opt_type}] No row for {day.date()}")
                continue
            # Most recent alert for that contract
            row = max(rows, key=lambda x: x.alert_time or x.created_date_time)

            status_ok = row.status == "bought" and row.exit_reason is None
            sold_vwap = (
                row.status == "sold" and (row.exit_reason or "") in ("stock_vwap_cross", "Exit-VWAP Cross")
            )
            if not status_ok and not (args.also_sold_vwap and sold_vwap):
                print(
                    f"[{stock_name}] id={row.id} skip: status={row.status!r} exit_reason={row.exit_reason!r} "
                    f"(use --also-sold-vwap to rewrite sold/vwap rows)"
                )
                continue

            sell_price: Optional[float] = None
            if stock_name in sell_map:
                sell_price = sell_map[stock_name]
            elif args.use_sell_column and row.sell_price and float(row.sell_price) > 0:
                sell_price = float(row.sell_price)
                print(f"[{stock_name}] Using row sell_price column: ₹{sell_price:.2f}")
            elif args.fetch_ltp and row.instrument_key and upstox:
                qd = upstox.get_market_quote_by_key(row.instrument_key)
                if qd and qd.get("last_price"):
                    sell_price = float(qd["last_price"])
                    print(f"[{stock_name}] LTP from quote: ₹{sell_price:.2f}")
                else:
                    print(f"[{stock_name}] Quote failed for {row.instrument_key}", file=sys.stderr)
                    continue
            elif args.apply:
                print(f"[{stock_name}] Missing sell price", file=sys.stderr)
                continue

            buy = float(row.buy_price or 0)
            qty = int(row.qty or 0)
            pnl = (sell_price - buy) * qty if sell_price is not None and buy and qty else None

            print(
                f"[{stock_name} {strike}{opt_type}] id={row.id} contract={row.option_contract!r} "
                f"buy=₹{buy:.2f} sell=₹{sell_price:.2f} qty={qty} pnl=₹{pnl:.2f}"
                if sell_price is not None and pnl is not None
                else f"[{stock_name}] id={row.id} preview"
            )

            if args.dry_run or not args.apply:
                continue

            row.status = "sold"
            row.exit_reason = "manual"
            row.sell_time = now
            row.sell_price = round(sell_price, 2) if sell_price is not None else row.sell_price
            if pnl is not None:
                row.pnl = round(pnl, 2)
            flag_modified(row, "status")
            flag_modified(row, "exit_reason")
            flag_modified(row, "sell_time")
            flag_modified(row, "sell_price")
            flag_modified(row, "pnl")
            updated += 1
            print(f"  -> updated id={row.id}")

        if args.apply and updated:
            db.commit()
            print(f"Committed {updated} row(s).")
        elif args.apply:
            db.rollback()
            print("No rows updated.")
        else:
            print("(dry-run: no writes)")
    finally:
        db.close()


if __name__ == "__main__":
    main()

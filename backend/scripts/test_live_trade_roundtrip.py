#!/usr/bin/env python3
"""
End-to-end test: resolve an option → place_live_upstox_gtt_entry → place_live_upstox_exit
(same code paths as scan algo live trading).

Usage (from project root, PYTHONPATH=.):
  python3 backend/scripts/test_live_trade_roundtrip.py --dry-run --symbol NIFTY --expiry 2026-03-27 --strike 25000 --opt CE --qty 65

  python3 backend/scripts/test_live_trade_roundtrip.py --execute --i-know-risk \\
      --symbol NIFTY --expiry 2026-03-27 --strike 25000 --opt CE --qty 65

After market hours: Upstox may reject or leave GTT OPEN; exit path cancels OPEN orders.

Requires: trading_live.json = YES on server, valid Upstox token, margin for qty.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

# Project root on server or local
ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import backend.env_bootstrap  # noqa: F401 — load .env

from backend.services import live_trading
from backend.services.upstox_service import upstox_service


def _parse_expiry(s: str) -> datetime:
    return datetime.strptime(s.strip(), "%Y-%m-%d")


def main() -> int:
    p = argparse.ArgumentParser(description="Test live GTT entry + exit (same as scan algo)")
    p.add_argument("--dry-run", action="store_true", help="Only print what would be sent (no orders)")
    p.add_argument("--execute", action="store_true", help="Place real orders (requires --i-know-risk)")
    p.add_argument(
        "--i-know-risk",
        action="store_true",
        help="Confirm real money / live orders",
    )
    p.add_argument("--symbol", default="NIFTY", help="Underlying (e.g. NIFTY, RELIANCE)")
    p.add_argument("--expiry", required=True, help="Option expiry YYYY-MM-DD")
    p.add_argument("--strike", type=float, required=True, help="Strike")
    p.add_argument("--opt", choices=("CE", "PE"), default="CE")
    p.add_argument("--qty", type=int, default=65, help="Quantity (NIFTY index lot often 65)")
    p.add_argument(
        "--instrument-key",
        default=None,
        help="Override NSE_FO|... (skips symbol/expiry/strike build)",
    )
    p.add_argument("--buy-price", type=float, default=None, help="Override entry; default from option LTP")
    p.add_argument("--stop-loss", type=float, default=None, help="Override SL; default buy - 2 ticks")
    args = p.parse_args()

    if args.execute and not args.i_know_risk:
        print("Refusing --execute without --i-know-risk", file=sys.stderr)
        return 2
    if args.execute and args.dry_run:
        print("Use either --dry-run or --execute, not both", file=sys.stderr)
        return 2

    if not upstox_service:
        print("Upstox service not available", file=sys.stderr)
        return 1

    expiry_dt = _parse_expiry(args.expiry)
    if args.instrument_key:
        ikey = args.instrument_key.strip()
    else:
        ikey = upstox_service.get_option_instrument_key(
            args.symbol.upper(), expiry_dt, args.strike, args.opt
        )
    if not ikey:
        print("Could not build instrument_key", file=sys.stderr)
        return 1

    label = f"{args.symbol} {args.strike}{args.opt} exp {args.expiry}"
    buy_price = args.buy_price
    if buy_price is None:
        buy_price = upstox_service.get_option_ltp(
            args.symbol.upper(), expiry_dt, args.strike, args.opt
        )
    if not buy_price or buy_price <= 0:
        buy_price = float(os.getenv("TEST_LIVE_FALLBACK_BUY_PRICE") or "10.0")
        print(f"⚠️  No LTP; using fallback buy_price=₹{buy_price} (set TEST_LIVE_FALLBACK_BUY_PRICE or --buy-price)")

    tick = upstox_service.get_tick_size_by_instrument_key(ikey) or 0.05
    stop_loss = args.stop_loss
    if stop_loss is None:
        stop_loss = round(buy_price - 2 * tick, 2)
        if stop_loss <= 0:
            stop_loss = round(buy_price * 0.9, 2)

    print("=== Live trading round-trip test ===")
    print(json.dumps({"instrument_key": ikey, "label": label, "qty": args.qty, "buy_price": buy_price, "stop_loss": stop_loss}, indent=2))
    print(f"trading_live: {live_trading.get_trading_live_value()}")
    print(f"market_open_ist: {upstox_service.is_market_open_ist()}")

    if args.dry_run or not args.execute:
        print("\n[DRY-RUN] Would call:")
        print(f"  place_live_upstox_gtt_entry({ikey!r}, qty={args.qty}, ..., buy_price={buy_price}, stop_loss={stop_loss})")
        print("  then place_live_upstox_exit(..., buy_order_id=<from entry>)")
        return 0

    print("\n--- ENTRY (GTT) ---")
    entry = live_trading.place_live_upstox_gtt_entry(
        instrument_key=ikey,
        qty=args.qty,
        stock_name=args.symbol.upper(),
        option_contract=label,
        buy_price=buy_price,
        stop_loss=stop_loss,
    )
    print(json.dumps(entry, indent=2, default=str))

    oid = entry.get("order_id") if isinstance(entry, dict) else None
    if not oid and isinstance(entry, dict):
        data = entry.get("data") or {}
        inner = (data.get("data") or {}) if isinstance(data, dict) else {}
        ids = inner.get("gtt_order_ids") if isinstance(inner, dict) else None
        if isinstance(ids, list) and ids:
            oid = ids[0]
    if not entry.get("success"):
        print("Entry failed or skipped; not calling exit.", file=sys.stderr)
        return 1

    time.sleep(2)

    print("\n--- EXIT (cancel GTT or square-off per order status) ---")
    if not oid:
        print("No order_id / gtt_order_ids on entry response; cannot exit", file=sys.stderr)
        return 1

    ex = live_trading.place_live_upstox_exit(
        instrument_key=ikey,
        qty=args.qty,
        stock_name=args.symbol.upper(),
        option_contract=label,
        buy_order_id=str(oid),
        tag="test_live_roundtrip",
    )
    print(json.dumps(ex, indent=2, default=str))
    return 0 if ex.get("success") or ex.get("skipped") else 1


if __name__ == "__main__":
    raise SystemExit(main())

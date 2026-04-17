#!/usr/bin/env python3
"""
Patch buy_price for specific Smart Futures rows (smart_futures_daily).

Usage (from repo root):
  PYTHONPATH=. python backend/scripts/patch_smart_futures_buy_prices.py
  PYTHONPATH=. python backend/scripts/patch_smart_futures_buy_prices.py --session-date 2026-04-17
"""
from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.smart_futures_session_date import effective_session_date_ist_for_trend

# (stock column / underlying, buy_price)
PATCHES: list[tuple[str, float]] = [
    ("VBL", 466.75),
    ("BHEL", 310.66),
    ("ADANIENSOL", 1248.90),
]


def main() -> int:
    p = argparse.ArgumentParser(description="Patch smart_futures_daily.buy_price for named stocks.")
    p.add_argument(
        "--session-date",
        type=str,
        default=None,
        help="IST session date YYYY-MM-DD (default: effective_session_date_ist_for_trend())",
    )
    args = p.parse_args()
    if args.session_date:
        sd = date.fromisoformat(args.session_date.strip())
    else:
        sd = effective_session_date_ist_for_trend()

    db = SessionLocal()
    try:
        for stock, price in PATCHES:
            res = db.execute(
                text(
                    """
                    UPDATE smart_futures_daily
                    SET buy_price = :bp, updated_at = CURRENT_TIMESTAMP
                    WHERE session_date = :sd
                      AND UPPER(TRIM(stock)) = UPPER(:stock)
                    RETURNING id, stock, fut_symbol, buy_price, order_status
                    """
                ),
                {"bp": float(price), "sd": sd, "stock": stock.strip()},
            )
            row = res.mappings().first()
            db.commit()
            if row:
                print(
                    f"OK {stock}: id={row['id']} fut_symbol={row['fut_symbol']} "
                    f"buy_price={row['buy_price']} order_status={row['order_status']}"
                )
            else:
                print(f"NO ROW for stock={stock!r} session_date={sd} (no update)")
        return 0
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())

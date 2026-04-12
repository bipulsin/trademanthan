#!/usr/bin/env python3
"""
Remove and add arbitrage_master rows, mirror car_nifty200, then refresh keys/LTPs via daily setup.

Usage (repo root, production venv):
  PYTHONPATH=. python backend/scripts/arbitrage_master_replace_symbols.py

Edits REMOVE_STOCKS / ADD_STOCKS in this file for future rotations.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import text  # noqa: E402

from backend.database import engine  # noqa: E402

REMOVE_STOCKS = ("HUDCO", "PPLPHARMA", "TATATECH", "TORNTPOWER")
ADD_STOCKS = ("ADANIPOWER", "HYUNDAI")


def main() -> int:
    remove = tuple(s.strip().upper() for s in REMOVE_STOCKS)
    add = tuple(s.strip().upper() for s in ADD_STOCKS)

    with engine.begin() as conn:
        for s in remove:
            r1 = conn.execute(
                text("DELETE FROM arbitrage_master WHERE UPPER(TRIM(stock)) = :s"),
                {"s": s},
            ).rowcount
            r2 = conn.execute(
                text("DELETE FROM car_nifty200 WHERE UPPER(TRIM(stock)) = :s"),
                {"s": s},
            ).rowcount
            print(f"removed {s}: arbitrage_master={r1} car_nifty200={r2}")

        for s in add:
            conn.execute(
                text(
                    """
                    INSERT INTO arbitrage_master (stock)
                    VALUES (:s)
                    ON CONFLICT (stock) DO NOTHING
                    """
                ),
                {"s": s},
            )
            print(f"inserted arbitrage_master stock={s} (if not already present)")

    from backend.services.arbitrage_daily_setup_scheduler import run_arbitrage_daily_setup_now  # noqa: E402

    out = run_arbitrage_daily_setup_now()
    print("run_arbitrage_daily_setup_now:", out)

    with engine.begin() as conn:
        for s in add:
            conn.execute(
                text(
                    """
                    INSERT INTO car_nifty200 (stock, stock_instrument_key, stock_ltp)
                    SELECT m.stock, m.stock_instrument_key, m.stock_ltp
                    FROM arbitrage_master m
                    WHERE UPPER(TRIM(m.stock)) = :s
                    ON CONFLICT (stock) DO UPDATE SET
                        stock_instrument_key = EXCLUDED.stock_instrument_key,
                        stock_ltp = EXCLUDED.stock_ltp
                    """
                ),
                {"s": s},
            )
        print("car_nifty200 upserted for:", add)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

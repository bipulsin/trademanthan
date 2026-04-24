#!/usr/bin/env python3
"""
Insert one or more NSE underliers into arbitrage_master, then run arbitrage daily setup
(Upstox keys + current/next FUT + LTPs + sector_index) and mirror into car_nifty200.

  PYTHONPATH=. python backend/scripts/add_arbitrage_master_symbols.py COCHINSHIP
  PYTHONPATH=. python backend/scripts/add_arbitrage_master_symbols.py COCHINSHIP TATASTEEL
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import text  # noqa: E402

from backend.database import engine  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "symbols",
        nargs="*",
        default=["COCHINSHIP"],
        help="NSE underlying symbols (default: COCHINSHIP)",
    )
    args = ap.parse_args()
    syms = tuple(s.strip().upper() for s in args.symbols if s and str(s).strip())
    if not syms:
        ap.error("at least one symbol required")

    with engine.begin() as conn:
        for s in syms:
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
        print("inserted (if missing):", syms)

    from backend.services.arbitrage_daily_setup_scheduler import (  # noqa: E402
        run_arbitrage_daily_setup_now,
    )

    out = run_arbitrage_daily_setup_now()
    print("run_arbitrage_daily_setup_now:", out)

    with engine.begin() as conn:
        for s in syms:
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
        print("car_nifty200 upserted for:", syms)

    with engine.connect() as conn:
        for s in syms:
            row = conn.execute(
                text(
                    """
                    SELECT stock,
                           stock_instrument_key IS NOT NULL AND TRIM(stock_instrument_key) <> '' AS has_eq,
                           nextmth_future_instrement_key IS NOT NULL AND TRIM(nextmth_future_instrement_key) <> '' AS has_next_fut
                    FROM arbitrage_master
                    WHERE UPPER(TRIM(stock)) = :s
                    """
                ),
                {"s": s},
            ).mappings().first()
            print("verify", s, ":", dict(row) if row else None)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

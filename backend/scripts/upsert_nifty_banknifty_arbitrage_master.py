#!/usr/bin/env python3
"""
Ensure NIFTY and BANKNIFTY live in arbitrage_master with NSE_INDEX spot keys
and current/next month index FUT keys (same columns as stock FO rows).

  NIFTY      → NSE_INDEX|Nifty 50   + NIFTY FUT …
  BANKNIFTY  → NSE_INDEX|Nifty Bank + BANKNIFTY FUT …

Sector label = index display name; sector_index = self.
Stock-FUT universes exclude these via INDEX_FUT_UNDERLYINGS.

Usage (repo root / app container):
  PYTHONPATH=. python backend/scripts/upsert_nifty_banknifty_arbitrage_master.py
  PYTHONPATH=. python backend/scripts/upsert_nifty_banknifty_arbitrage_master.py --skip-roll
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import bindparam, text  # noqa: E402

from backend.database import engine  # noqa: E402
from backend.services.arbitrage_universe import INDEX_MASTER_SPOT_KEYS  # noqa: E402

INDEX_ROWS = (
    ("NIFTY", "Nifty 50", INDEX_MASTER_SPOT_KEYS["NIFTY"]),
    ("BANKNIFTY", "Nifty Bank", INDEX_MASTER_SPOT_KEYS["BANKNIFTY"]),
)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--skip-roll",
        action="store_true",
        help="Do not refresh Upstox EQ/FUT keys after the SQL upsert",
    )
    args = ap.parse_args()

    with engine.begin() as conn:
        # FK: arbitrage_master.sector_instrument_key → nifty_benchmark_reference
        conn.execute(
            text(
                """
                INSERT INTO nifty_benchmark_reference
                    (instrument_key, display_label, benchmark_kind, breakfast_sort_order)
                VALUES
                    ('NSE_INDEX|Nifty Bank', 'BANKNIFTY', 'broad', NULL)
                ON CONFLICT (instrument_key) DO UPDATE SET
                    display_label = EXCLUDED.display_label,
                    benchmark_kind = EXCLUDED.benchmark_kind,
                    breakfast_sort_order = EXCLUDED.breakfast_sort_order,
                    updated_at = NOW()
                """
            )
        )
        print("nifty_benchmark_reference: ensured NSE_INDEX|Nifty Bank (BANKNIFTY broad)")

        for stock, sector, idx in INDEX_ROWS:
            conn.execute(
                text(
                    """
                    INSERT INTO arbitrage_master (
                        stock, sector, sector_index, sector_instrument_key, stock_instrument_key
                    )
                    VALUES (:s, :sector, :idx, :idx, :idx)
                    ON CONFLICT (stock) DO UPDATE SET
                        sector = EXCLUDED.sector,
                        sector_index = EXCLUDED.sector_index,
                        sector_instrument_key = EXCLUDED.sector_instrument_key,
                        stock_instrument_key = COALESCE(
                            NULLIF(TRIM(arbitrage_master.stock_instrument_key), ''),
                            EXCLUDED.stock_instrument_key
                        )
                    """
                ),
                {"s": stock, "sector": sector, "idx": idx},
            )
        print(
            "upserted:",
            [(s, sector, idx) for s, sector, idx in INDEX_ROWS],
        )

    if not args.skip_roll:
        from backend.services.arbitrage_daily_setup_scheduler import (  # noqa: E402
            run_arbitrage_metadata_roll_now,
        )

        out = run_arbitrage_metadata_roll_now(apply_roll_window=True)
        print("run_arbitrage_metadata_roll_now:", out)

    with engine.begin() as conn:
        # Re-assert sector labels after roll (roll only sets sector_index).
        for stock, sector, idx in INDEX_ROWS:
            conn.execute(
                text(
                    """
                    UPDATE arbitrage_master
                    SET sector = :sector,
                        sector_index = :idx,
                        sector_instrument_key = :idx,
                        stock_instrument_key = COALESCE(
                            NULLIF(TRIM(stock_instrument_key), ''),
                            :idx
                        )
                    WHERE UPPER(TRIM(stock)) = :s
                    """
                ),
                {"s": stock, "sector": sector, "idx": idx},
            )

        rows = conn.execute(
            text(
                """
                SELECT stock, sector, sector_index, sector_instrument_key,
                       stock_instrument_key,
                       currmth_future_symbol, currmth_future_instrument_key,
                       nextmth_future_symbol, nextmth_future_instrement_key
                FROM arbitrage_master
                WHERE UPPER(TRIM(stock)) IN :syms
                ORDER BY stock
                """
            ).bindparams(bindparam("syms", expanding=True)),
            {"syms": [s for s, _, _ in INDEX_ROWS]},
        ).mappings().all()
        for r in rows:
            print("verify", dict(r))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

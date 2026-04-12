#!/usr/bin/env python3
"""
Set arbitrage_master.sector_index from backend/fno_sector_mapping.csv for matching stocks.

Usage (repo root):
  PYTHONPATH=. python backend/scripts/apply_fno_sector_mapping_to_arbitrage_master.py
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import text  # noqa: E402

from backend.database import engine  # noqa: E402
from backend.services.fno_sector_mapping_csv import load_fno_sector_index_map  # noqa: E402
from backend.services.sector_movers import normalize_sector_instrument_key  # noqa: E402


def main() -> int:
    m = load_fno_sector_index_map()
    if not m:
        print("No mapping loaded (missing backend/fno_sector_mapping.csv?)")
        return 1
    updated = 0
    missing = 0
    with engine.begin() as conn:
        for sym, idx in m.items():
            canon = normalize_sector_instrument_key(idx)
            if not canon:
                continue
            r = conn.execute(
                text(
                    """
                    UPDATE arbitrage_master
                    SET sector_index = :idx
                    WHERE UPPER(TRIM(stock)) = :sym
                    """
                ),
                {"sym": sym, "idx": canon},
            ).rowcount
            if r:
                updated += r
            else:
                missing += 1
    print(f"csv_symbols={len(m)} rows_updated={updated} csv_rows_not_in_arbitrage_master={missing}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

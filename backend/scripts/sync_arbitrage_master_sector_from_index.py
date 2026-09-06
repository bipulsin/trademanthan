#!/usr/bin/env python3
"""Set arbitrage_master.sector from sector_instrument_key / sector_index.

Usage:
  PYTHONPATH=. python backend/scripts/sync_arbitrage_master_sector_from_index.py [--dry-run]
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
from backend.services.sector_label_from_index import sector_label_from_index  # noqa: E402

_SQL = (ROOT / "backend/migrations/sync_arbitrage_master_sector_from_index.sql").read_text(
    encoding="utf-8"
)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    with engine.connect() as conn:
        dist = conn.execute(
            text(
                """
                SELECT COALESCE(NULLIF(TRIM(sector_instrument_key), ''), TRIM(sector_index)) AS idx,
                       TRIM(sector) AS sector,
                       COUNT(*) AS n
                FROM arbitrage_master
                GROUP BY 1, 2
                ORDER BY 1, 2
                """
            )
        ).mappings().all()
        print("before:")
        for r in dist:
            idx = r["idx"]
            want = sector_label_from_index(idx)
            flag = "" if (want and want == r["sector"]) else f" -> {want}"
            print(f"  {idx!s:40} {r['sector']!s:20} n={r['n']}{flag}")

        if args.dry_run:
            print("dry-run: no writes")
            return 0

    with engine.begin() as conn:
        conn.execute(text(_SQL))

    with engine.connect() as conn:
        print("after:")
        after = conn.execute(
            text(
                """
                SELECT COALESCE(NULLIF(TRIM(sector_instrument_key), ''), TRIM(sector_index)) AS idx,
                       TRIM(sector) AS sector,
                       COUNT(*) AS n
                FROM arbitrage_master
                GROUP BY 1, 2
                ORDER BY 1, 2
                """
            )
        ).mappings().all()
        for r in after:
            print(f"  {r['idx']!s:40} {r['sector']!s:20} n={r['n']}")
        bad = conn.execute(
            text(
                """
                SELECT stock, sector, sector_index, sector_instrument_key
                FROM arbitrage_master
                WHERE sector IS DISTINCT FROM (
                    CASE COALESCE(NULLIF(TRIM(sector_instrument_key), ''), TRIM(sector_index))
                        WHEN 'NSE_INDEX|Nifty Fin Service' THEN 'Fin Service'
                        ELSE NULL
                    END
                )
                  AND COALESCE(NULLIF(TRIM(sector_instrument_key), ''), TRIM(sector_index))
                      = 'NSE_INDEX|Nifty Fin Service'
                """
            )
        ).mappings().all()
        sample = conn.execute(
            text(
                """
                SELECT stock, sector, sector_index
                FROM arbitrage_master
                WHERE UPPER(TRIM(stock)) IN ('POLICYBZR', 'PAYTM')
                ORDER BY stock
                """
            )
        ).mappings().all()
        print("POLICYBZR/PAYTM:", [dict(r) for r in sample])
        if bad:
            print("Fin Service mismatches:", [dict(r) for r in bad])
            return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

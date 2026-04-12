#!/usr/bin/env python3
"""
One-shot: rewrite legacy ``arbitrage_master.sector_index`` values to Upstox ``instrument_key`` strings.

Uses the same alias table as ``sector_movers.SECTOR_INDEX_INSTRUMENT_ALIASES``. Safe to re-run.

Usage (repo root):
  PYTHONPATH=. python backend/scripts/fix_arbitrage_master_sector_index_keys.py
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import backend.env_bootstrap  # noqa: F401
from sqlalchemy import text  # noqa: E402

from backend.database import engine  # noqa: E402
from backend.services.sector_movers import SECTOR_INDEX_INSTRUMENT_ALIASES  # noqa: E402


def main() -> int:
    total = 0
    with engine.begin() as conn:
        for old, new in SECTOR_INDEX_INSTRUMENT_ALIASES.items():
            r = conn.execute(
                text(
                    """
                    UPDATE arbitrage_master
                    SET sector_index = :new
                    WHERE sector_index = :old
                    """
                ),
                {"old": old, "new": new},
            ).rowcount
            if r:
                print(f"updated {r} row(s): {old!r} -> {new!r}")
            total += int(r or 0)
    print(f"done total_rows_updated={total}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

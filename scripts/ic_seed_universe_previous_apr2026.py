#!/usr/bin/env python3
"""Apply Iron Condor universe_master reference closes — run from repo root (local or EC2).

Example:
  cd /home/ubuntu/trademanthan && PYTHONPATH=. python3 scripts/ic_seed_universe_previous_apr2026.py
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

ASOF = "2026-04-30"

# User-supplied closes (prior session reference). Omit symbols left NULL for previous_day_close.
MANUAL_CLOSE: dict[str, float] = {
    "AXISBANK": 1268.30,
    "BAJFINANCE": 937.00,
    "BHARTIARTL": 1886.80,
    "HDFCBANK": 771.70,
    "ICICIBANK": 1263.40,
    "INFOSYS": 1181.80,
    "ITC": 314.90,
    "KOTAKBANK": 383.30,
    "LT": 4014.00,
    "RELIANCE": 1430.80,
    "SBIN": 1068.45,
    "TCS": 2473.90,
}


def main() -> int:
    from sqlalchemy import text

    from backend.database import SessionLocal
    from backend.services.iron_condor_service import refresh_ic_universe_master_memory

    db = SessionLocal()
    try:
        db.execute(
            text(
                """
                UPDATE iron_condor_universe_master
                SET previous_close_as_of = CAST(:d AS DATE), updated_at = CURRENT_TIMESTAMP
                """
            ),
            {"d": ASOF},
        )
        for sym, px in MANUAL_CLOSE.items():
            sym_u = sym.strip().upper()
            db.execute(
                text(
                    """
                    UPDATE iron_condor_universe_master
                    SET previous_day_close = :px,
                        previous_close_as_of = CAST(:d AS DATE),
                        updated_at = CURRENT_TIMESTAMP
                    WHERE symbol = :sym
                    """
                ),
                {"px": px, "d": ASOF, "sym": sym_u},
            )
        db.commit()
        refresh_ic_universe_master_memory(db)
        print("Applied previous_close_as_of=", ASOF, "for all rows.")
        print("Applied previous_day_close for", len(MANUAL_CLOSE), "symbols.")
    except Exception as e:
        db.rollback()
        print("ERROR:", e)
        return 1
    finally:
        db.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

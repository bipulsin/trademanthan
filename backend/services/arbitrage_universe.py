"""Current-month futures universe from arbitrage_master (shared across FO features)."""
from __future__ import annotations

from typing import Dict, List

from sqlalchemy import text

from backend.database import SessionLocal


def load_arbitrage_curr_mth_universe() -> List[Dict[str, str]]:
    """Return stock / future_symbol / instrument_key for all curr-month FO rows."""
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT stock, currmth_future_symbol, currmth_future_instrument_key
                FROM arbitrage_master
                WHERE currmth_future_instrument_key IS NOT NULL
                  AND TRIM(currmth_future_instrument_key) <> ''
                ORDER BY stock
                """
            )
        ).fetchall()
        return [
            {
                "stock": str(r[0] or "").strip(),
                "future_symbol": str(r[1] or "").strip(),
                "instrument_key": str(r[2] or "").strip(),
            }
            for r in rows
        ]
    finally:
        db.close()

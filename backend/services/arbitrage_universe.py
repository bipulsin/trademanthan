"""Current-month futures universe from arbitrage_master (shared across FO features)."""
from __future__ import annotations

from typing import Dict, List

from sqlalchemy import text

from backend.database import SessionLocal

# Index / macro underlyings in arbitrage_master (NSE_INDEX spot + NSE_FO index FUT).
# Stock-futures consumers (Vajra / Smart / volume mismatch / RS) exclude these by default.
INDEX_FUT_UNDERLYINGS = frozenset(
    {
        "NIFTY",
        "BANKNIFTY",
        "FINNIFTY",
        "MIDCPNIFTY",
        "NIFTYNXT50",
        "SENSEX",
        "BANKEX",
    }
)

# arbitrage_master.stock → NSE_INDEX instrument_key (also used as own sector_index).
INDEX_MASTER_SPOT_KEYS = {
    "NIFTY": "NSE_INDEX|Nifty 50",
    "BANKNIFTY": "NSE_INDEX|Nifty Bank",
}


def load_arbitrage_curr_mth_universe(*, include_index: bool = False) -> List[Dict[str, str]]:
    """Return stock / future_symbol / instrument_key for curr-month FO rows.

    By default excludes index underlyings (NIFTY/BANKNIFTY/…) so stock-FUT features
    keep an equity universe. Pass ``include_index=True`` when index FUT rows are wanted.
    """
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
        out: List[Dict[str, str]] = []
        for r in rows:
            stock = str(r[0] or "").strip()
            if not include_index and stock.upper() in INDEX_FUT_UNDERLYINGS:
                continue
            out.append(
                {
                    "stock": stock,
                    "future_symbol": str(r[1] or "").strip(),
                    "instrument_key": str(r[2] or "").strip(),
                }
            )
        return out
    finally:
        db.close()

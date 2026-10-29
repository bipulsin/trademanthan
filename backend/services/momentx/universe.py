"""arbitrage_master universe for MomentX (equity key + current-month future)."""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.arbitrage_universe import INDEX_FUT_UNDERLYINGS
from backend.services.chart_instrument_resolver import lookup_future_lot_size


def load_momentx_universe(*, include_index: bool = False) -> List[Dict[str, Any]]:
    """Return stock / equity_key / future_symbol / future_key / lot_size rows.

    Current-month future = ``currmth_future_*`` as maintained by the BOD
    arbitrage daily setup (nearest expiry on/after today).
    """
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT stock,
                       stock_instrument_key,
                       currmth_future_symbol,
                       currmth_future_instrument_key
                FROM arbitrage_master
                WHERE stock_instrument_key IS NOT NULL
                  AND TRIM(stock_instrument_key) <> ''
                  AND currmth_future_instrument_key IS NOT NULL
                  AND TRIM(currmth_future_instrument_key) <> ''
                ORDER BY stock
                """
            )
        ).fetchall()
        out: List[Dict[str, Any]] = []
        for r in rows:
            stock = str(r[0] or "").strip()
            if not stock:
                continue
            if not include_index and stock.upper() in INDEX_FUT_UNDERLYINGS:
                continue
            eq = str(r[1] or "").strip()
            fut_sym = str(r[2] or "").strip()
            fut_key = str(r[3] or "").strip()
            if not eq or not fut_key:
                continue
            lot: Optional[int] = lookup_future_lot_size(
                instrument_key=fut_key,
                trading_symbol=fut_sym or None,
            )
            out.append(
                {
                    "stock": stock,
                    "equity_instrument_key": eq,
                    "future_symbol": fut_sym or stock,
                    "future_instrument_key": fut_key,
                    "lot_size": int(lot) if lot else None,
                }
            )
        return out
    finally:
        db.close()

"""Database access for arbitrage_master market data."""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import pytz
from sqlalchemy import text

from backend.database import engine
from backend.services.market_data.schema import ensure_market_data_columns

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

_UNIVERSE_SQL = text(
    """
    SELECT
        stock,
        stock_instrument_key,
        stock_ltp,
        stock_vwap,
        stock_ema5,
        stock_last_updated,
        currmth_future_symbol,
        currmth_future_instrument_key,
        currmth_future_ltp,
        currmth_future_vwap,
        currmth_future_ema5,
        currmth_future_last_updated,
        currmth_candle_open_5m,
        currmth_candle_high_5m,
        currmth_candle_low_5m,
        currmth_candle_close_5m,
        currmth_candle_volume_5m,
        nextmth_future_symbol,
        nextmth_future_instrement_key,
        nextmth_future_ltp,
        nextmth_future_vwap,
        nextmth_future_ema5,
        nextmth_future_last_updated,
        market_data_source,
        market_data_refresh_status,
        market_data_refresh_error,
        market_data_last_updated,
        sector_index
    FROM arbitrage_master
    ORDER BY stock
    """
)


def load_universe_rows() -> List[Dict[str, Any]]:
    ensure_market_data_columns()
    with engine.connect() as conn:
        rows = conn.execute(_UNIVERSE_SQL).mappings().all()
    return [dict(r) for r in rows]


def bulk_update_market_data(updates: List[Dict[str, Any]]) -> int:
    """Bulk UPDATE per stock row."""
    if not updates:
        return 0
    ensure_market_data_columns()
    sql = text(
        """
        UPDATE arbitrage_master SET
            stock_ltp = COALESCE(:stock_ltp, stock_ltp),
            stock_vwap = COALESCE(:stock_vwap, stock_vwap),
            stock_ema5 = COALESCE(:stock_ema5, stock_ema5),
            stock_last_updated = COALESCE(:stock_last_updated, stock_last_updated),
            currmth_future_ltp = COALESCE(:currmth_future_ltp, currmth_future_ltp),
            currmth_future_vwap = COALESCE(:currmth_future_vwap, currmth_future_vwap),
            currmth_future_ema5 = COALESCE(:currmth_future_ema5, currmth_future_ema5),
            currmth_future_last_updated = COALESCE(:currmth_future_last_updated, currmth_future_last_updated),
            currmth_candle_open_5m = COALESCE(:currmth_candle_open_5m, currmth_candle_open_5m),
            currmth_candle_high_5m = COALESCE(:currmth_candle_high_5m, currmth_candle_high_5m),
            currmth_candle_low_5m = COALESCE(:currmth_candle_low_5m, currmth_candle_low_5m),
            currmth_candle_close_5m = COALESCE(:currmth_candle_close_5m, currmth_candle_close_5m),
            currmth_candle_volume_5m = COALESCE(:currmth_candle_volume_5m, currmth_candle_volume_5m),
            nextmth_future_ltp = COALESCE(:nextmth_future_ltp, nextmth_future_ltp),
            nextmth_future_vwap = COALESCE(:nextmth_future_vwap, nextmth_future_vwap),
            nextmth_future_ema5 = COALESCE(:nextmth_future_ema5, nextmth_future_ema5),
            nextmth_future_last_updated = COALESCE(:nextmth_future_last_updated, nextmth_future_last_updated),
            market_data_source = COALESCE(:market_data_source, market_data_source),
            market_data_refresh_status = COALESCE(:market_data_refresh_status, market_data_refresh_status),
            market_data_refresh_error = COALESCE(:market_data_refresh_error, market_data_refresh_error),
            market_data_last_updated = COALESCE(:market_data_last_updated, market_data_last_updated)
        WHERE stock = :stock
        """
    )
    with engine.begin() as conn:
        conn.execute(sql, updates)
    return len(updates)


def load_row_by_stock(stock: str) -> Optional[Dict[str, Any]]:
    sym = (stock or "").strip().upper()
    if not sym:
        return None
    ensure_market_data_columns()
    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT *
                FROM arbitrage_master
                WHERE UPPER(TRIM(stock)) = :s
                LIMIT 1
                """
            ),
            {"s": sym},
        ).mappings().first()
    return dict(row) if row else None


def load_key_index() -> Dict[str, Dict[str, Any]]:
    """Map instrument_key -> row snapshot fields for LTP lookup."""
    out: Dict[str, Dict[str, Any]] = {}
    for row in load_universe_rows():
        sk = (row.get("stock_instrument_key") or "").strip()
        ck = (row.get("currmth_future_instrument_key") or "").strip()
        nk = (row.get("nextmth_future_instrement_key") or "").strip()
        if sk:
            out[sk] = {
                "leg": "stock",
                "stock": row.get("stock"),
                "ltp": row.get("stock_ltp"),
                "vwap": row.get("stock_vwap"),
                "ema5": row.get("stock_ema5"),
                "updated": row.get("stock_last_updated"),
            }
        if ck:
            out[ck] = {
                "leg": "currmth",
                "stock": row.get("stock"),
                "ltp": row.get("currmth_future_ltp"),
                "vwap": row.get("currmth_future_vwap"),
                "ema5": row.get("currmth_future_ema5"),
                "updated": row.get("currmth_future_last_updated"),
            }
        if nk:
            out[nk] = {
                "leg": "nextmth",
                "stock": row.get("stock"),
                "ltp": row.get("nextmth_future_ltp"),
                "vwap": row.get("nextmth_future_vwap"),
                "ema5": row.get("nextmth_future_ema5"),
                "updated": row.get("nextmth_future_last_updated"),
            }
    return out


def _now_ist() -> datetime:
    return datetime.now(IST)

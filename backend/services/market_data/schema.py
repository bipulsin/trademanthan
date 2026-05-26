"""arbitrage_master column definitions and runtime migrations."""
from __future__ import annotations

import logging
from typing import List, Tuple

from sqlalchemy import inspect, text

from backend.database import engine

logger = logging.getLogger(__name__)

# (column_name, postgres_type)
MARKET_DATA_COLUMNS: List[Tuple[str, str]] = [
    ("stock_vwap", "DOUBLE PRECISION"),
    ("stock_ema5", "DOUBLE PRECISION"),
    ("stock_last_updated", "TIMESTAMPTZ"),
    ("currmth_future_vwap", "DOUBLE PRECISION"),
    ("currmth_future_ema5", "DOUBLE PRECISION"),
    ("currmth_future_last_updated", "TIMESTAMPTZ"),
    ("nextmth_future_vwap", "DOUBLE PRECISION"),
    ("nextmth_future_ema5", "DOUBLE PRECISION"),
    ("nextmth_future_last_updated", "TIMESTAMPTZ"),
    ("currmth_candle_open_5m", "DOUBLE PRECISION"),
    ("currmth_candle_high_5m", "DOUBLE PRECISION"),
    ("currmth_candle_low_5m", "DOUBLE PRECISION"),
    ("currmth_candle_close_5m", "DOUBLE PRECISION"),
    ("currmth_candle_volume_5m", "DOUBLE PRECISION"),
    ("market_data_source", "TEXT"),
    ("market_data_refresh_status", "TEXT"),
    ("market_data_refresh_error", "TEXT"),
    ("market_data_last_updated", "TIMESTAMPTZ"),
]


def ensure_market_data_columns() -> None:
    """Add market-data columns to arbitrage_master if missing."""
    insp = inspect(engine)
    if "arbitrage_master" not in insp.get_table_names():
        return
    existing = {c["name"] for c in insp.get_columns("arbitrage_master")}
    with engine.begin() as conn:
        for col, pg_type in MARKET_DATA_COLUMNS:
            if col in existing:
                continue
            conn.execute(text(f"ALTER TABLE arbitrage_master ADD COLUMN {col} {pg_type}"))
            logger.info("arbitrage_master: added column %s", col)
        try:
            conn.execute(
                text(
                    """
                    CREATE INDEX IF NOT EXISTS idx_arbitrage_master_currmth_key
                    ON arbitrage_master (currmth_future_instrument_key)
                    WHERE currmth_future_instrument_key IS NOT NULL
                    """
                )
            )
        except Exception as e:
            logger.debug("idx_arbitrage_master_currmth_key: %s", e)

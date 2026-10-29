"""Persist MomentX day-board filter events to Postgres."""
from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any, Dict, Iterable, Optional

from sqlalchemy import text

from backend.database import SessionLocal

logger = logging.getLogger(__name__)

_DDL = """
CREATE TABLE IF NOT EXISTS momentx_filtered_symbols (
    id BIGSERIAL PRIMARY KEY,
    trade_date DATE NOT NULL,
    stock VARCHAR(64) NOT NULL,
    future_symbol VARCHAR(128),
    future_instrument_key VARCHAR(128),
    lot_size INTEGER,
    filtered_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_momentx_filtered_trade_date
    ON momentx_filtered_symbols (trade_date);
CREATE INDEX IF NOT EXISTS ix_momentx_filtered_stock_date
    ON momentx_filtered_symbols (stock, trade_date);
"""


def ensure_momentx_tables() -> None:
    db = SessionLocal()
    try:
        for stmt in _DDL.strip().split(";"):
            s = stmt.strip()
            if s:
                db.execute(text(s))
        db.commit()
    except Exception:
        db.rollback()
        logger.exception("momentx ensure tables failed")
        raise
    finally:
        db.close()


def record_filtered_symbols(
    cards: Iterable[Dict[str, Any]],
    *,
    trade_date: date,
    filtered_at: datetime,
) -> int:
    """Insert one row per card that was newly Stage-1 filtered onto the board."""
    rows = list(cards or [])
    if not rows:
        return 0
    db = SessionLocal()
    inserted = 0
    try:
        for c in rows:
            stock = str(c.get("stock") or "").strip().upper()
            if not stock:
                continue
            lot = c.get("lot_size")
            try:
                lot_i = int(lot) if lot is not None else None
            except (TypeError, ValueError):
                lot_i = None
            db.execute(
                text(
                    """
                    INSERT INTO momentx_filtered_symbols
                        (trade_date, stock, future_symbol, future_instrument_key, lot_size, filtered_at)
                    VALUES
                        (:trade_date, :stock, :future_symbol, :future_instrument_key, :lot_size, :filtered_at)
                    """
                ),
                {
                    "trade_date": trade_date,
                    "stock": stock,
                    "future_symbol": (c.get("future_symbol") or None),
                    "future_instrument_key": (c.get("future_instrument_key") or None),
                    "lot_size": lot_i,
                    "filtered_at": filtered_at,
                },
            )
            inserted += 1
        db.commit()
        return inserted
    except Exception:
        db.rollback()
        logger.exception("momentx record_filtered_symbols failed")
        return 0
    finally:
        db.close()

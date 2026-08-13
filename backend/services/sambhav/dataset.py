"""Sambhav dataset versioning — reproducibility metadata (no OHLC mutation)."""

from __future__ import annotations

import json
import logging
from datetime import date, datetime
from typing import Any, Dict, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from backend.services.sambhav.config import (
    DATASET_VERSION_V1,
    HISTORICAL_SOURCE,
    INSTRUMENT_DISPLAY,
    INSTRUMENT_KEY,
    IST,
)
from backend.services.sambhav.tables import ensure_sambhav_tables

logger = logging.getLogger(__name__)


def register_dataset_version(
    db: Session,
    *,
    dataset_version: str = DATASET_VERSION_V1,
    instrument: str = INSTRUMENT_DISPLAY,
    instrument_key: str = INSTRUMENT_KEY,
    interval: str = "10m",
    start_date: date,
    end_date: date,
    regular_session_count: int,
    regular_candle_count: int,
    total_candle_count: int,
    excluded_session_count: int,
    excluded_holiday_count: int = 0,
    source: str = HISTORICAL_SOURCE,
    meta: Optional[Dict[str, Any]] = None,
    activate: bool = True,
) -> Dict[str, Any]:
    """Upsert dataset version metadata. Does not modify candle OHLC."""
    ensure_sambhav_tables()
    payload = {
        "dv": dataset_version,
        "instrument": instrument,
        "ik": instrument_key,
        "interval": interval,
        "sd": start_date,
        "ed": end_date,
        "rsc": int(regular_session_count),
        "rcc": int(regular_candle_count),
        "tcc": int(total_candle_count),
        "esc": int(excluded_session_count),
        "ehc": int(excluded_holiday_count),
        "source": source,
        "meta": json.dumps(meta or {}),
        "active": bool(activate),
    }
    if activate:
        db.execute(text("UPDATE sambhav_dataset_versions SET is_active = FALSE WHERE is_active = TRUE"))
    db.execute(
        text(
            """
            INSERT INTO sambhav_dataset_versions (
                dataset_version, instrument, instrument_key, interval,
                start_date, end_date, regular_session_count, regular_candle_count,
                total_candle_count, excluded_session_count, excluded_holiday_count,
                source, meta_json, is_active, created_at
            ) VALUES (
                :dv, :instrument, :ik, :interval,
                :sd, :ed, :rsc, :rcc,
                :tcc, :esc, :ehc,
                :source, CAST(:meta AS jsonb), :active, CURRENT_TIMESTAMP
            )
            ON CONFLICT (dataset_version) DO UPDATE SET
                instrument = EXCLUDED.instrument,
                instrument_key = EXCLUDED.instrument_key,
                interval = EXCLUDED.interval,
                start_date = EXCLUDED.start_date,
                end_date = EXCLUDED.end_date,
                regular_session_count = EXCLUDED.regular_session_count,
                regular_candle_count = EXCLUDED.regular_candle_count,
                total_candle_count = EXCLUDED.total_candle_count,
                excluded_session_count = EXCLUDED.excluded_session_count,
                excluded_holiday_count = EXCLUDED.excluded_holiday_count,
                source = EXCLUDED.source,
                meta_json = EXCLUDED.meta_json,
                is_active = EXCLUDED.is_active
            """
        ),
        payload,
    )
    db.commit()
    logger.info("registered sambhav dataset version %s", dataset_version)
    return get_dataset_version(db, dataset_version) or {"dataset_version": dataset_version}


def get_dataset_version(db: Session, dataset_version: str) -> Optional[Dict[str, Any]]:
    ensure_sambhav_tables()
    row = db.execute(
        text(
            """
            SELECT dataset_version, instrument, instrument_key, interval,
                   start_date, end_date, regular_session_count, regular_candle_count,
                   total_candle_count, excluded_session_count, excluded_holiday_count,
                   source, meta_json, is_active, created_at
            FROM sambhav_dataset_versions WHERE dataset_version = :dv
            """
        ),
        {"dv": dataset_version},
    ).fetchone()
    if not row:
        return None
    return _row_to_dict(row)


def get_active_dataset_version(db: Session) -> Optional[Dict[str, Any]]:
    ensure_sambhav_tables()
    row = db.execute(
        text(
            """
            SELECT dataset_version, instrument, instrument_key, interval,
                   start_date, end_date, regular_session_count, regular_candle_count,
                   total_candle_count, excluded_session_count, excluded_holiday_count,
                   source, meta_json, is_active, created_at
            FROM sambhav_dataset_versions
            WHERE is_active = TRUE
            ORDER BY created_at DESC
            LIMIT 1
            """
        )
    ).fetchone()
    return _row_to_dict(row) if row else None


def _row_to_dict(row: Any) -> Dict[str, Any]:
    return {
        "dataset_version": row.dataset_version,
        "instrument": row.instrument,
        "instrument_key": row.instrument_key,
        "interval": row.interval,
        "start_date": str(row.start_date) if row.start_date else None,
        "end_date": str(row.end_date) if row.end_date else None,
        "regular_session_count": row.regular_session_count,
        "regular_candle_count": row.regular_candle_count,
        "total_candle_count": row.total_candle_count,
        "excluded_session_count": row.excluded_session_count,
        "excluded_holiday_count": row.excluded_holiday_count,
        "source": row.source,
        "meta_json": row.meta_json,
        "is_active": bool(row.is_active),
        "created_at": row.created_at.isoformat() if isinstance(row.created_at, datetime) else row.created_at,
        "created_at_ist": (
            row.created_at.astimezone(IST).isoformat()
            if isinstance(row.created_at, datetime)
            else None
        ),
    }

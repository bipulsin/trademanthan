"""Sambhav PostgreSQL tables — ensure_* pattern, sambhav_ prefix only."""

from __future__ import annotations

import logging
import threading

from sqlalchemy import text

from backend.database import engine

logger = logging.getLogger(__name__)

_LOCK = threading.Lock()
_READY = False

_DDL = """
CREATE TABLE IF NOT EXISTS sambhav_raw_candles (
    id BIGSERIAL PRIMARY KEY,
    instrument_key TEXT NOT NULL,
    candle_ts TIMESTAMPTZ NOT NULL,
    open DOUBLE PRECISION NOT NULL,
    high DOUBLE PRECISION NOT NULL,
    low DOUBLE PRECISION NOT NULL,
    close DOUBLE PRECISION NOT NULL,
    volume DOUBLE PRECISION NOT NULL DEFAULT 0,
    source TEXT NOT NULL DEFAULT 'upstox',
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (instrument_key, candle_ts)
);
CREATE INDEX IF NOT EXISTS idx_sambhav_raw_ik_ts
    ON sambhav_raw_candles (instrument_key, candle_ts);

CREATE TABLE IF NOT EXISTS sambhav_10m_candles (
    id BIGSERIAL PRIMARY KEY,
    instrument_key TEXT NOT NULL,
    candle_start TIMESTAMPTZ NOT NULL,
    candle_end TIMESTAMPTZ NOT NULL,
    open DOUBLE PRECISION NOT NULL,
    high DOUBLE PRECISION NOT NULL,
    low DOUBLE PRECISION NOT NULL,
    close DOUBLE PRECISION NOT NULL,
    volume DOUBLE PRECISION NOT NULL DEFAULT 0,
    n_1m INTEGER NOT NULL DEFAULT 0,
    is_complete BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (instrument_key, candle_start)
);
CREATE INDEX IF NOT EXISTS idx_sambhav_10m_ik_start
    ON sambhav_10m_candles (instrument_key, candle_start);

CREATE TABLE IF NOT EXISTS sambhav_models (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    model_type TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'RESEARCH',
    artifact_path TEXT,
    feature_list_json JSONB,
    train_start TIMESTAMPTZ,
    train_end TIMESTAMPTZ,
    metrics_json JSONB,
    calibration_method TEXT,
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_sambhav_models_status ON sambhav_models (status);

CREATE TABLE IF NOT EXISTS sambhav_predictions (
    id BIGSERIAL PRIMARY KEY,
    instrument_key TEXT NOT NULL,
    candle_start TIMESTAMPTZ NOT NULL,
    predict_at TIMESTAMPTZ NOT NULL,
    horizon_minutes INTEGER NOT NULL DEFAULT 30,
    model_id INTEGER REFERENCES sambhav_models(id),
    p_up_raw DOUBLE PRECISION,
    p_down_raw DOUBLE PRECISION,
    p_up_calibrated DOUBLE PRECISION,
    p_down_calibrated DOUBLE PRECISION,
    predicted_direction TEXT,
    features_json JSONB,
    status TEXT NOT NULL DEFAULT 'PENDING',
    future_close DOUBLE PRECISION,
    future_return DOUBLE PRECISION,
    actual_direction TEXT,
    resolved_at TIMESTAMPTZ,
    source TEXT NOT NULL DEFAULT 'live',
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (instrument_key, candle_start, model_id, source)
);
CREATE INDEX IF NOT EXISTS idx_sambhav_pred_status
    ON sambhav_predictions (status, candle_start);

CREATE TABLE IF NOT EXISTS sambhav_metrics (
    id SERIAL PRIMARY KEY,
    model_id INTEGER REFERENCES sambhav_models(id),
    eval_type TEXT NOT NULL,
    window_start TIMESTAMPTZ,
    window_end TIMESTAMPTZ,
    n_samples INTEGER,
    metrics_json JSONB NOT NULL,
    calibration_buckets_json JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS sambhav_import_state (
    id SERIAL PRIMARY KEY,
    instrument_key TEXT NOT NULL UNIQUE,
    last_imported_ts TIMESTAMPTZ,
    last_from_date DATE,
    last_to_date DATE,
    status TEXT NOT NULL DEFAULT 'idle',
    detail TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""


def ensure_sambhav_tables() -> None:
    global _READY
    if _READY:
        return
    with _LOCK:
        if _READY:
            return
        if engine is None:
            raise RuntimeError("Database engine not initialized")
        with engine.begin() as conn:
            conn.execute(text(_DDL))
        _READY = True
        logger.info("sambhav tables ensured")

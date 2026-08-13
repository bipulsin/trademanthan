"""Sambhav PostgreSQL tables — ensure_* pattern, sambhav_ prefix only.

SOURCE DATA (immutable for ML):
  sambhav_10m_candles — canonical Upstox V3 10m OHLC (never mutated by features/train)
  sambhav_raw_candles — reserved for optional V2 1m study

DERIVED:
  sambhav_sessions — session classification (REGULAR / EXCLUDED_*)
  sambhav_dataset_versions — reproducible dataset snapshots
  sambhav_features — future feature store (separate from source OHLC)
"""

from __future__ import annotations

import logging
import threading

from sqlalchemy import text

from backend.database import engine

logger = logging.getLogger(__name__)

_LOCK = threading.Lock()
_READY_VERSION = 0
_SCHEMA_VERSION = 4

# sambhav_raw_candles is retained for a possible Sambhav V2 1-minute study.
# V1 does not download or import 1-minute historical candles.
_ALTER = """
ALTER TABLE sambhav_10m_candles ADD COLUMN IF NOT EXISTS open_interest DOUBLE PRECISION;
ALTER TABLE sambhav_10m_candles ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'upstox';
ALTER TABLE sambhav_models ADD COLUMN IF NOT EXISTS dataset_version TEXT;
ALTER TABLE sambhav_models ADD COLUMN IF NOT EXISTS feature_version TEXT;
ALTER TABLE sambhav_models ADD COLUMN IF NOT EXISTS model_version TEXT;
ALTER TABLE sambhav_features ADD COLUMN IF NOT EXISTS candle_close TIMESTAMPTZ;
ALTER TABLE sambhav_features ADD COLUMN IF NOT EXISTS current_price DOUBLE PRECISION;
ALTER TABLE sambhav_features ADD COLUMN IF NOT EXISTS target_future_close DOUBLE PRECISION;
ALTER TABLE sambhav_features ADD COLUMN IF NOT EXISTS target_future_return DOUBLE PRECISION;
ALTER TABLE sambhav_features ADD COLUMN IF NOT EXISTS target_direction TEXT;
ALTER TABLE sambhav_features ADD COLUMN IF NOT EXISTS target_timestamp TIMESTAMPTZ;
ALTER TABLE sambhav_features ADD COLUMN IF NOT EXISTS session_date DATE;
ALTER TABLE sambhav_features ADD COLUMN IF NOT EXISTS volume_available BOOLEAN;
ALTER TABLE sambhav_features ADD COLUMN IF NOT EXISTS features_complete BOOLEAN;
"""

_DDL = """
-- 1-minute data may be added in a future Sambhav V2 feature-enhancement study.
-- V1 does not populate this table.
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

-- SOURCE / IMMUTABLE OHLC for Sambhav V1 (features live in sambhav_features).
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
    open_interest DOUBLE PRECISION,
    source TEXT NOT NULL DEFAULT 'upstox',
    n_1m INTEGER NOT NULL DEFAULT 0,
    is_complete BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (instrument_key, candle_start)
);
CREATE INDEX IF NOT EXISTS idx_sambhav_10m_ik_start
    ON sambhav_10m_candles (instrument_key, candle_start);

CREATE TABLE IF NOT EXISTS sambhav_sessions (
    id BIGSERIAL PRIMARY KEY,
    instrument_key TEXT NOT NULL,
    session_date DATE NOT NULL,
    session_type TEXT NOT NULL,
    included_in_sambhav_v1 BOOLEAN NOT NULL DEFAULT FALSE,
    candle_count INTEGER NOT NULL DEFAULT 0,
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (instrument_key, session_date)
);
CREATE INDEX IF NOT EXISTS idx_sambhav_sessions_v1
    ON sambhav_sessions (instrument_key, included_in_sambhav_v1, session_date);
CREATE INDEX IF NOT EXISTS idx_sambhav_sessions_type
    ON sambhav_sessions (instrument_key, session_type);

CREATE TABLE IF NOT EXISTS sambhav_dataset_versions (
    dataset_version TEXT PRIMARY KEY,
    instrument TEXT NOT NULL,
    instrument_key TEXT NOT NULL,
    interval TEXT NOT NULL DEFAULT '10m',
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    regular_session_count INTEGER NOT NULL DEFAULT 0,
    regular_candle_count INTEGER NOT NULL DEFAULT 0,
    total_candle_count INTEGER NOT NULL DEFAULT 0,
    excluded_session_count INTEGER NOT NULL DEFAULT 0,
    excluded_holiday_count INTEGER NOT NULL DEFAULT 0,
    source TEXT NOT NULL DEFAULT 'upstox_v3_10m',
    meta_json JSONB,
    is_active BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Feature store (separate from source OHLC).
CREATE TABLE IF NOT EXISTS sambhav_features (
    id BIGSERIAL PRIMARY KEY,
    instrument_key TEXT NOT NULL,
    candle_start TIMESTAMPTZ NOT NULL,
    candle_close TIMESTAMPTZ,
    feature_version TEXT NOT NULL,
    dataset_version TEXT,
    current_price DOUBLE PRECISION,
    features_json JSONB NOT NULL,
    target_future_close DOUBLE PRECISION,
    target_future_return DOUBLE PRECISION,
    target_direction TEXT,
    target_timestamp TIMESTAMPTZ,
    session_date DATE,
    volume_available BOOLEAN,
    features_complete BOOLEAN,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (instrument_key, candle_start, feature_version)
);
CREATE INDEX IF NOT EXISTS idx_sambhav_features_ik_start
    ON sambhav_features (instrument_key, candle_start);

CREATE TABLE IF NOT EXISTS sambhav_research_status (
    phase TEXT PRIMARY KEY,
    status_json JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

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
    dataset_version TEXT,
    feature_version TEXT,
    model_version TEXT,
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
    global _READY_VERSION
    if _READY_VERSION >= _SCHEMA_VERSION:
        return
    with _LOCK:
        if _READY_VERSION >= _SCHEMA_VERSION:
            return
        if engine is None:
            raise RuntimeError("Database engine not initialized")
        with engine.begin() as conn:
            conn.execute(text(_DDL))
            conn.execute(text(_ALTER))
        _READY_VERSION = _SCHEMA_VERSION
        logger.info("sambhav tables ensured (schema v%s)", _SCHEMA_VERSION)

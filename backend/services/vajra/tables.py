"""DDL helpers for vajra_futures_rating."""
from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.orm import Session

_EXTRA_COLUMNS = (
    ("tps_score", "DOUBLE PRECISION"),
    ("ecs_score", "DOUBLE PRECISION"),
    ("transition_state", "TEXT"),
    ("vwap_reclaim_status", "TEXT"),
    ("ema_reclaim_status", "TEXT"),
    ("rsi_transition_status", "TEXT"),
    ("pullback_quality_score", "DOUBLE PRECISION"),
    ("extension_risk_score", "DOUBLE PRECISION"),
    ("execution_validated", "BOOLEAN DEFAULT FALSE"),
    ("execution_step", "TEXT"),
    ("pipeline_stage", "TEXT"),
    ("alertable", "BOOLEAN DEFAULT FALSE"),
    ("ees_score", "DOUBLE PRECISION"),
    ("entry_state", "TEXT"),
    ("enter_action", "TEXT"),
    ("enter_enabled", "BOOLEAN DEFAULT FALSE"),
    ("ees_alerts", "JSONB DEFAULT '[]'"),
    ("trade_quality_score", "DOUBLE PRECISION"),
)


def ensure_vajra_futures_rating_table(db: Session) -> None:
    db.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS vajra_futures_rating (
                id BIGSERIAL PRIMARY KEY,
                session_date DATE NOT NULL,
                stock TEXT NOT NULL,
                future_symbol TEXT,
                instrument_key TEXT NOT NULL,
                trade_type TEXT NOT NULL,
                confidence DOUBLE PRECISION NOT NULL,
                bull_score DOUBLE PRECISION,
                bear_score DOUBLE PRECISION,
                structure_pass BOOLEAN NOT NULL DEFAULT FALSE,
                momentum_pass BOOLEAN NOT NULL DEFAULT FALSE,
                trend_pass BOOLEAN NOT NULL DEFAULT FALSE,
                volume_pass BOOLEAN NOT NULL DEFAULT FALSE,
                obv_label TEXT,
                market_phase TEXT,
                reversal_risk TEXT,
                computed_at TIMESTAMPTZ NOT NULL,
                tps_score DOUBLE PRECISION,
                ecs_score DOUBLE PRECISION,
                transition_state TEXT,
                vwap_reclaim_status TEXT,
                ema_reclaim_status TEXT,
                rsi_transition_status TEXT,
                pullback_quality_score DOUBLE PRECISION,
                extension_risk_score DOUBLE PRECISION,
                execution_validated BOOLEAN DEFAULT FALSE,
                execution_step TEXT,
                pipeline_stage TEXT,
                alertable BOOLEAN DEFAULT FALSE,
                CONSTRAINT uq_vajra_session_instrument UNIQUE (session_date, instrument_key)
            )
            """
        )
    )
    for col, col_type in _EXTRA_COLUMNS:
        db.execute(
            text(
                f"ALTER TABLE vajra_futures_rating ADD COLUMN IF NOT EXISTS {col} {col_type}"
            )
        )
    db.execute(
        text(
            "CREATE INDEX IF NOT EXISTS idx_vajra_session_conf "
            "ON vajra_futures_rating (session_date, trade_type, confidence DESC)"
        )
    )
    db.execute(
        text(
            "CREATE INDEX IF NOT EXISTS idx_vajra_session_tps "
            "ON vajra_futures_rating (session_date, tps_score DESC NULLS LAST)"
        )
    )

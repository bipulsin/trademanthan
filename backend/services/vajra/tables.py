"""DDL helpers for vajra_futures_rating."""
from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.orm import Session


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
                CONSTRAINT uq_vajra_session_instrument UNIQUE (session_date, instrument_key)
            )
            """
        )
    )
    db.execute(
        text(
            "CREATE INDEX IF NOT EXISTS idx_vajra_session_conf "
            "ON vajra_futures_rating (session_date, trade_type, confidence DESC)"
        )
    )

"""DDL for volume_mismatch_signals."""
from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.orm import Session

_EXTRA_COLUMNS = (
    ("future_symbol", "TEXT"),
    ("first_15m_high", "DOUBLE PRECISION"),
    ("first_15m_low", "DOUBLE PRECISION"),
    ("first_15m_open", "DOUBLE PRECISION"),
    ("first_15m_close", "DOUBLE PRECISION"),
    ("bb_upper", "DOUBLE PRECISION"),
    ("bb_middle", "DOUBLE PRECISION"),
    ("bb_lower", "DOUBLE PRECISION"),
    ("vwap", "DOUBLE PRECISION"),
    ("preferred_entry", "DOUBLE PRECISION"),
    ("target1", "DOUBLE PRECISION"),
    ("target2", "DOUBLE PRECISION"),
)


def ensure_volume_mismatch_signals_table(db: Session) -> None:
    db.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS volume_mismatch_signals (
                id BIGSERIAL PRIMARY KEY,
                symbol TEXT NOT NULL,
                instrument_token TEXT,
                trade_date DATE NOT NULL,
                direction TEXT NOT NULL,
                gap_percent DOUBLE PRECISION,
                first_15m_volume DOUBLE PRECISION,
                relative_volume DOUBLE PRECISION,
                net_volume DOUBLE PRECISION,
                score DOUBLE PRECISION,
                entry_price DOUBLE PRECISION,
                stop_loss DOUBLE PRECISION,
                target1 DOUBLE PRECISION,
                target2 DOUBLE PRECISION,
                current_price DOUBLE PRECISION,
                entry_status TEXT NOT NULL DEFAULT 'WAITING',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                CONSTRAINT uq_vm_mismatch_date_symbol UNIQUE (trade_date, symbol)
            )
            """
        )
    )
    for col, col_type in _EXTRA_COLUMNS:
        db.execute(
            text(
                f"ALTER TABLE volume_mismatch_signals ADD COLUMN IF NOT EXISTS {col} {col_type}"
            )
        )
    db.execute(
        text(
            """
            CREATE INDEX IF NOT EXISTS idx_vm_mismatch_trade_date
            ON volume_mismatch_signals (trade_date DESC)
            """
        )
    )

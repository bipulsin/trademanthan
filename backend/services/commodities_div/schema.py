"""Ensure Commodities Div tables exist."""
from __future__ import annotations

import logging
from pathlib import Path

from sqlalchemy import text

from backend.database import engine

logger = logging.getLogger(__name__)

_MIGRATION = Path(__file__).resolve().parents[2] / "migrations" / "add_commodities_div.sql"
_ENSURED = False


def ensure_commodities_div_tables() -> None:
    global _ENSURED
    if _ENSURED:
        return
    if not _MIGRATION.is_file():
        raise FileNotFoundError(f"migration missing: {_MIGRATION}")
    sql = _MIGRATION.read_text(encoding="utf-8")
    with engine.begin() as conn:
        # Drop legacy global one-active index before migration SQL (may recreate per-symbol).
        conn.execute(text("DROP INDEX IF EXISTS uq_commodities_div_one_active"))
        conn.execute(text(sql))
        # Widen disposition CHECK for Section 4a (existing DBs keep old constraint otherwise).
        conn.execute(
            text(
                """
                ALTER TABLE commodities_div_webhook_log
                    DROP CONSTRAINT IF EXISTS commodities_div_webhook_log_disposition_check
                """
            )
        )
        conn.execute(
            text(
                """
                ALTER TABLE commodities_div_webhook_log
                    ADD CONSTRAINT commodities_div_webhook_log_disposition_check
                    CHECK (
                        disposition IS NULL OR disposition IN (
                            'applied', 'replaced_prior', 'ignored_in_trade_block',
                            'unmatched', 'parse_failed',
                            'blocked_different_symbol_active',
                            'blocked_different_direction'
                        )
                    )
                """
            )
        )
        # Ensure per-symbol unique (idempotent if migration already created it).
        conn.execute(
            text(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS uq_commodities_div_one_active_per_symbol
                    ON commodities_div_signals (symbol_mapped)
                    WHERE status <> 'History'
                """
            )
        )
        # Discretionary edit: PAPER | LIVE (default PAPER).
        conn.execute(
            text(
                """
                ALTER TABLE commodities_div_signals
                    ADD COLUMN IF NOT EXISTS trade_mode TEXT NOT NULL DEFAULT 'PAPER'
                """
            )
        )
        conn.execute(
            text(
                """
                UPDATE commodities_div_signals
                SET trade_mode = 'PAPER'
                WHERE trade_mode IS NULL OR trade_mode NOT IN ('PAPER', 'LIVE')
                """
            )
        )
    _ENSURED = True
    logger.info("commodities_div tables ensured")

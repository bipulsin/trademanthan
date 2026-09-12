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
        conn.execute(text(sql))
    _ENSURED = True
    logger.info("commodities_div tables ensured")

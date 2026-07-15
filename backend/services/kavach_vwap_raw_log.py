"""Shadow-only raw VWAP time-series for the lock / top-5 universe.

Logs every poll for symbols currently on ``daily_snapshot`` (morning lock),
independent of pre-gate READY. Does not touch live trade_state or gates.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from backend.database import engine

logger = logging.getLogger(__name__)

_ENSURED = False


def ensure_vwap_raw_log() -> None:
    global _ENSURED
    if _ENSURED:
        return
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS kavach_vwap_raw_log (
                    id SERIAL PRIMARY KEY,
                    session_date DATE NOT NULL,
                    symbol VARCHAR(32) NOT NULL,
                    direction VARCHAR(8),
                    lock_rank INTEGER,
                    lock_direction VARCHAR(16),
                    vwap_slope_score NUMERIC(12,4),
                    steep_ok BOOLEAN,
                    vwap_extension_pct NUMERIC(12,6),
                    logged_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS idx_vwap_raw_session_sym
                ON kavach_vwap_raw_log (session_date, symbol, logged_at DESC)
                """
            )
        )
    _ENSURED = True


def log_vwap_raw(db, rows: List[Dict[str, Any]]) -> int:
    """Best-effort append-only insert. Never raises into the enrich path."""
    if not rows:
        return 0
    try:
        ensure_vwap_raw_log()
        for r in rows:
            db.execute(
                text(
                    """
                    INSERT INTO kavach_vwap_raw_log (
                        session_date, symbol, direction,
                        lock_rank, lock_direction,
                        vwap_slope_score, steep_ok, vwap_extension_pct
                    ) VALUES (
                        CAST(:d AS date), :sym, :dir,
                        :lr, :ld,
                        :vs, :so, :ext
                    )
                    """
                ),
                {
                    "d": r.get("session_date"),
                    "sym": r.get("symbol"),
                    "dir": r.get("direction"),
                    "lr": r.get("lock_rank"),
                    "ld": r.get("lock_direction"),
                    "vs": r.get("vwap_slope_score"),
                    "so": r.get("steep_ok"),
                    "ext": r.get("vwap_extension_pct"),
                },
            )
        db.commit()
        return len(rows)
    except Exception as exc:
        logger.debug("vwap raw log failed: %s", exc)
        try:
            db.rollback()
        except Exception:
            pass
        return 0


def lock_direction_to_side(lock_direction: Optional[str]) -> str:
    d = (lock_direction or "").upper()
    if d in ("BEAR", "BEARISH", "SHORT"):
        return "SHORT"
    return "LONG"


def build_raw_row(
    *,
    session_date: str,
    symbol: str,
    direction: Optional[str],
    lock_rank: Optional[int],
    lock_direction: Optional[str],
    slope_score: Optional[float],
    steep_ok: Optional[bool],
    vwap_extension_pct: Optional[float],
) -> Dict[str, Any]:
    return {
        "session_date": session_date,
        "symbol": (symbol or "").upper(),
        "direction": direction,
        "lock_rank": lock_rank,
        "lock_direction": lock_direction,
        "vwap_slope_score": slope_score,
        "steep_ok": steep_ok,
        "vwap_extension_pct": vwap_extension_pct,
    }

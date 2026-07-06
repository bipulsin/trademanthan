"""Fast Watch — unconfirmed chart-level BUY flips for locked checklist symbols."""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Set

import pytz
from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.daily_checklist_snapshot import get_locked_symbols
from backend.services.kavach_engine import BEARISH_STATES, BULLISH_STATES
from backend.services.rs_conviction_config import get_config

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

BULL_FLIP = frozenset({"BUY", "READY"})
BEAR_FLIP = frozenset({"SELL", "READY SHORT"})


def today_ist() -> str:
    return datetime.now(IST).strftime("%Y-%m-%d")


def _flip_state(kavach_state: Optional[str], direction: str) -> bool:
    k = (kavach_state or "").upper()
    if direction == "SHORT":
        return k in BEAR_FLIP
    return k in BULL_FLIP


def _conflict(kavach_state: Optional[str], direction: str) -> bool:
    k = (kavach_state or "").upper()
    if direction == "SHORT":
        return k in BULLISH_STATES
    return k in BEARISH_STATES


def record_fast_watch_flips(
    session_date: str,
    updates: List[Dict[str, Any]],
    *,
    locked_only: bool = True,
) -> int:
    """Insert first-flip rows for locked symbols. Returns count of new rows."""
    if not get_config().get("fast_watch_enabled", True):
        return 0
    db = SessionLocal()
    inserted = 0
    try:
        locked: Set[str] = set(get_locked_symbols(db, session_date)) if locked_only else set()
        for u in updates:
            sym = (u.get("symbol") or "").strip().upper()
            direction = (u.get("direction") or "LONG").upper()
            if locked_only and sym not in locked:
                continue
            kav = u.get("dashboard_kavach") or u.get("kavach_state")
            if not _flip_state(kav, direction):
                continue
            exists = db.execute(
                text(
                    """
                    SELECT 1 FROM rs_fast_watch
                    WHERE session_date = CAST(:d AS date) AND symbol = :sym AND direction = :dir
                    """
                ),
                {"d": session_date, "sym": sym, "dir": direction},
            ).fetchone()
            if exists:
                continue
            now = datetime.now(IST)
            db.execute(
                text(
                    """
                    INSERT INTO rs_fast_watch (
                        session_date, symbol, direction, first_flip_at,
                        kavach_state, trade_score, confidence_grade
                    ) VALUES (
                        CAST(:d AS date), :sym, :dir, :t, :k, :score, :grade
                    )
                    ON CONFLICT (session_date, symbol, direction) DO NOTHING
                    """
                ),
                {
                    "d": session_date,
                    "sym": sym,
                    "dir": direction,
                    "t": now,
                    "k": kav,
                    "score": u.get("kavach_score_entry") or u.get("trade_score"),
                    "grade": u.get("confidence"),
                },
            )
            inserted += 1
        db.commit()
    finally:
        db.close()
    return inserted


def get_fast_watch(session_date: Optional[str] = None) -> List[Dict[str, Any]]:
    sd = session_date or today_ist()
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT symbol, direction, first_flip_at, kavach_state,
                       trade_score, confidence_grade
                FROM rs_fast_watch
                WHERE session_date = CAST(:d AS date)
                ORDER BY first_flip_at DESC
                """
            ),
            {"d": sd},
        ).fetchall()
        out = []
        for r in rows:
            out.append(
                {
                    "symbol": r.symbol,
                    "direction": r.direction,
                    "first_flip_at": r.first_flip_at.isoformat() if r.first_flip_at else None,
                    "kavach_state": r.kavach_state,
                    "trade_score": r.trade_score,
                    "confidence_grade": r.confidence_grade,
                    "label": "unconfirmed",
                }
            )
        return out
    finally:
        db.close()

"""Fast Watch — unconfirmed chart-level BUY flips outside morning lock visibility.

Scope (default): morning-locked checklist symbols ∪ current RS top-5 per side.
Records first BUY/READY flip per symbol/day; UI highlights symbols not on the lock.
"""
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
SCOPE_LOCKED_ONLY = "locked_only"
SCOPE_LOCKED_OR_TOP5 = "locked_or_top5"


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


def _direction_from_ranking(ranking_type: Optional[str]) -> str:
    return "SHORT" if (ranking_type or "").upper() == "BEARISH" else "LONG"


def fast_watch_scope() -> str:
    scope = (get_config().get("fast_watch_scope") or SCOPE_LOCKED_OR_TOP5).strip().lower()
    if scope in (SCOPE_LOCKED_ONLY, SCOPE_LOCKED_OR_TOP5):
        return scope
    return SCOPE_LOCKED_OR_TOP5


def universe_symbols(
    session_date: str,
    *,
    locked: Optional[Set[str]] = None,
    top5_symbols: Optional[Set[str]] = None,
    db=None,
) -> Set[str]:
    """Symbols eligible for Fast Watch recording this cycle."""
    scope = fast_watch_scope()
    if locked is None:
        own_db = db is None
        if own_db:
            db = SessionLocal()
        try:
            locked = set(get_locked_symbols(db, session_date))
        finally:
            if own_db and db is not None:
                db.close()
    else:
        locked = set(locked)
    if scope == SCOPE_LOCKED_ONLY:
        return locked
    top5 = set(top5_symbols or ())
    return locked | top5


def record_fast_watch_flips(
    session_date: str,
    updates: List[Dict[str, Any]],
    *,
    locked_symbols: Optional[Set[str]] = None,
    top5_symbols: Optional[Set[str]] = None,
) -> int:
    """Insert first-flip rows for symbols in the configured universe. Returns new row count."""
    if not get_config().get("fast_watch_enabled", True):
        return 0
    eligible = universe_symbols(session_date, locked=locked_symbols, top5_symbols=top5_symbols)
    locked_set = set(locked_symbols or ())
    if not locked_set:
        db = SessionLocal()
        try:
            locked_set = set(get_locked_symbols(db, session_date))
        finally:
            db.close()

    db = SessionLocal()
    inserted = 0
    try:
        for u in updates:
            sym = (u.get("symbol") or "").strip().upper()
            if not sym or sym not in eligible:
                continue
            direction = (u.get("direction") or "LONG").upper()
            if direction not in ("LONG", "SHORT"):
                direction = "SHORT" if direction == "BEAR" else "LONG"
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
                    "grade": u.get("confidence") or u.get("confidence_grade"),
                },
            )
            inserted += 1
        db.commit()
    finally:
        db.close()
    return inserted


def get_fast_watch(
    session_date: Optional[str] = None,
    *,
    off_lock_only: bool = True,
) -> List[Dict[str, Any]]:
    """Return today's Fast Watch flips. Default: symbols not on morning lock (visibility gap)."""
    sd = session_date or today_ist()
    db = SessionLocal()
    try:
        locked = set(get_locked_symbols(db, sd))
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
            on_lock = r.symbol in locked
            if off_lock_only and on_lock:
                continue
            out.append(
                {
                    "symbol": r.symbol,
                    "direction": r.direction,
                    "first_flip_at": r.first_flip_at.isoformat() if r.first_flip_at else None,
                    "kavach_state": r.kavach_state,
                    "trade_score": r.trade_score,
                    "confidence_grade": r.confidence_grade,
                    "on_locked_list": on_lock,
                    "label": "unconfirmed",
                }
            )
        return out
    finally:
        db.close()

"""Shadow log for stretch penalty (pre/post score + grade). Research-only writes."""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import pytz
from sqlalchemy import text

from backend.database import engine

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

_ENSURED = False


def ensure_stretch_penalty_log() -> None:
    global _ENSURED
    if _ENSURED:
        return
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS kavach_stretch_penalty_log (
                    id SERIAL PRIMARY KEY,
                    session_date DATE NOT NULL,
                    symbol VARCHAR(32) NOT NULL,
                    direction VARCHAR(8),
                    logged_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    bar_at TIMESTAMPTZ,
                    source VARCHAR(32) NOT NULL DEFAULT 'ready',
                    rendered_state VARCHAR(32),
                    stretch_pct NUMERIC(12,6),
                    stretch_score_penalty INTEGER,
                    stretch_letter_penalty INTEGER,
                    trade_score_pre_stretch INTEGER,
                    trade_score_post_stretch INTEGER,
                    base_grade_pre_stretch VARCHAR(8),
                    base_grade_post_stretch VARCHAR(8),
                    promote_transition_floor_would_have_fired_pre_penalty BOOLEAN,
                    stretch_penalty_live BOOLEAN NOT NULL DEFAULT FALSE,
                    card_surfaced BOOLEAN,
                    would_suppress_ready BOOLEAN,
                    soft_stretch_pct NUMERIC(8,4),
                    hard_stretch_pct NUMERIC(8,4),
                    close_px NUMERIC(16,4),
                    ema10 NUMERIC(16,4),
                    vwap NUMERIC(16,4)
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS idx_stretch_pen_session_sym
                ON kavach_stretch_penalty_log (session_date, symbol, logged_at)
                """
            )
        )
    _ENSURED = True


def log_stretch_penalty(
    db,
    *,
    session_date: str,
    symbol: str,
    stretch: Dict[str, Any],
    direction: Optional[str] = None,
    source: str = "ready",
    rendered_state: Optional[str] = None,
    bar_at: Optional[datetime] = None,
    close_px: Optional[float] = None,
    ema10: Optional[float] = None,
    vwap: Optional[float] = None,
    card_surfaced: Optional[bool] = None,
    would_suppress_ready: Optional[bool] = None,
    logged_at: Optional[datetime] = None,
) -> None:
    """Best-effort insert of one stretch shadow row."""
    if not stretch:
        return
    try:
        ensure_stretch_penalty_log()
        now = logged_at or datetime.now(IST)
        if now.tzinfo is None:
            now = IST.localize(now)
        bat = bar_at or now
        if isinstance(bat, datetime) and bat.tzinfo is None:
            bat = IST.localize(bat)
        db.execute(
            text(
                """
                INSERT INTO kavach_stretch_penalty_log (
                    session_date, symbol, direction, logged_at, bar_at, source,
                    rendered_state, stretch_pct, stretch_score_penalty,
                    stretch_letter_penalty, trade_score_pre_stretch,
                    trade_score_post_stretch, base_grade_pre_stretch,
                    base_grade_post_stretch,
                    promote_transition_floor_would_have_fired_pre_penalty,
                    stretch_penalty_live, card_surfaced, would_suppress_ready,
                    soft_stretch_pct, hard_stretch_pct, close_px, ema10, vwap
                ) VALUES (
                    CAST(:d AS date), :sym, :dir, :lat, :bat, :src,
                    :rst, :sp, :ssp, :slp, :pre, :post, :gpre, :gpost,
                    :tfpre, :live, :surf, :supp, :soft, :hard, :close, :e10, :vw
                )
                """
            ),
            {
                "d": session_date,
                "sym": (symbol or "").upper(),
                "dir": (direction or "").upper() or None,
                "lat": now,
                "bat": bat,
                "src": source,
                "rst": rendered_state,
                "sp": stretch.get("stretch_pct"),
                "ssp": stretch.get("stretch_score_penalty"),
                "slp": stretch.get("stretch_letter_penalty"),
                "pre": stretch.get("trade_score_pre_stretch"),
                "post": stretch.get("trade_score_post_stretch"),
                "gpre": stretch.get("base_grade_pre_stretch"),
                "gpost": stretch.get("base_grade_post_stretch"),
                "tfpre": stretch.get(
                    "promote_transition_floor_would_have_fired_pre_penalty"
                ),
                "live": bool(stretch.get("stretch_penalty_live")),
                "surf": card_surfaced,
                "supp": would_suppress_ready,
                "soft": stretch.get("soft_stretch_pct"),
                "hard": stretch.get("hard_stretch_pct"),
                "close": close_px,
                "e10": ema10,
                "vw": vwap,
            },
        )
    except Exception as exc:
        logger.debug("stretch penalty log failed %s: %s", symbol, exc)


def first_ready_stretch_rows(db, session_date: str) -> List[Dict[str, Any]]:
    """One row per symbol: first Ready-ish stretch emission that day."""
    ensure_stretch_penalty_log()
    rows = db.execute(
        text(
            """
            SELECT DISTINCT ON (UPPER(symbol))
                symbol, direction, logged_at, bar_at, source, rendered_state,
                stretch_pct, stretch_score_penalty, stretch_letter_penalty,
                trade_score_pre_stretch, trade_score_post_stretch,
                base_grade_pre_stretch, base_grade_post_stretch,
                promote_transition_floor_would_have_fired_pre_penalty,
                stretch_penalty_live, card_surfaced, would_suppress_ready,
                soft_stretch_pct, hard_stretch_pct, close_px, ema10, vwap
            FROM kavach_stretch_penalty_log
            WHERE session_date = CAST(:d AS date)
            ORDER BY UPPER(symbol), logged_at ASC, id ASC
            """
        ),
        {"d": session_date},
    ).fetchall()
    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "symbol": r.symbol,
                "direction": r.direction,
                "logged_at": r.logged_at.isoformat() if r.logged_at else None,
                "bar_at": r.bar_at.isoformat() if r.bar_at else None,
                "source": r.source,
                "rendered_state": r.rendered_state,
                "stretch_pct": float(r.stretch_pct) if r.stretch_pct is not None else None,
                "stretch_score_penalty": r.stretch_score_penalty,
                "stretch_letter_penalty": r.stretch_letter_penalty,
                "trade_score_pre_stretch": r.trade_score_pre_stretch,
                "trade_score_post_stretch": r.trade_score_post_stretch,
                "base_grade_pre_stretch": r.base_grade_pre_stretch,
                "base_grade_post_stretch": r.base_grade_post_stretch,
                "promote_transition_floor_would_have_fired_pre_penalty": (
                    r.promote_transition_floor_would_have_fired_pre_penalty
                ),
                "stretch_penalty_live": bool(r.stretch_penalty_live),
                "card_surfaced": r.card_surfaced,
                "would_suppress_ready": r.would_suppress_ready,
                "soft_stretch_pct": float(r.soft_stretch_pct)
                if r.soft_stretch_pct is not None
                else None,
                "hard_stretch_pct": float(r.hard_stretch_pct)
                if r.hard_stretch_pct is not None
                else None,
                "close_px": float(r.close_px) if r.close_px is not None else None,
                "ema10": float(r.ema10) if r.ema10 is not None else None,
                "vwap": float(r.vwap) if r.vwap is not None else None,
            }
        )
    return out

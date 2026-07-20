"""Prospective RS scan exclusion / rank-depth audit log.

Research + diagnostic only: never mutates ranking, lock, or checklist outcomes.
Written after each relative_strength scan for symbols that did not enter the
persisted Top-N (PERSIST_TOP_N) snapshot — including metrics failures, NEUTRAL
Kavach, and beyond-persist ranks (with would-be rank + cutoffs).
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import pytz
from sqlalchemy import text

from backend.database import SessionLocal, engine

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

_ENSURED = False

# Canonical exclusion_reason codes
REASON_MISSING_KEY = "missing_key"
REASON_MISSING_CANDLES = "missing_candles_or_min_bars"
REASON_NO_PREV_CLOSE = "no_prev_close"
REASON_NO_CLOSED_BAR = "no_closed_bar"
REASON_EXCEPTION = "exception"
REASON_NEUTRAL = "neutral_kavach"
REASON_BEYOND_PERSIST = "beyond_persist_top_n"
REASON_NIFTY_ABORT = "nifty_unavailable"  # scan-level, not per-symbol

_INSERT = text(
    """
    INSERT INTO rs_scan_exclusion_log (
        session_date, scan_time, symbol, instrument_key, exclusion_reason, detail,
        kavach_state, relative_strength, trade_score, confidence_grade, ranking_side,
        would_be_rank, rank_cutoff, top_n_cutoff, cutoff_rs_persist, cutoff_rs_top_n,
        current_price, volume_ratio, volume_label, scan_trigger
    ) VALUES (
        CAST(:session_date AS date), :scan_time, :symbol, :instrument_key,
        :exclusion_reason, :detail,
        :kavach_state, :relative_strength, :trade_score, :confidence_grade, :ranking_side,
        :would_be_rank, :rank_cutoff, :top_n_cutoff, :cutoff_rs_persist, :cutoff_rs_top_n,
        :current_price, :volume_ratio, :volume_label, :scan_trigger
    )
    ON CONFLICT (scan_time, symbol) DO UPDATE SET
        exclusion_reason = EXCLUDED.exclusion_reason,
        detail = EXCLUDED.detail,
        kavach_state = EXCLUDED.kavach_state,
        relative_strength = EXCLUDED.relative_strength,
        trade_score = EXCLUDED.trade_score,
        confidence_grade = EXCLUDED.confidence_grade,
        ranking_side = EXCLUDED.ranking_side,
        would_be_rank = EXCLUDED.would_be_rank,
        rank_cutoff = EXCLUDED.rank_cutoff,
        top_n_cutoff = EXCLUDED.top_n_cutoff,
        cutoff_rs_persist = EXCLUDED.cutoff_rs_persist,
        cutoff_rs_top_n = EXCLUDED.cutoff_rs_top_n,
        current_price = EXCLUDED.current_price,
        volume_ratio = EXCLUDED.volume_ratio,
        volume_label = EXCLUDED.volume_label,
        scan_trigger = EXCLUDED.scan_trigger,
        instrument_key = EXCLUDED.instrument_key
    """
)


def ensure_rs_scan_exclusion_log() -> None:
    global _ENSURED
    if _ENSURED:
        return
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS rs_scan_exclusion_log (
                    id SERIAL PRIMARY KEY,
                    session_date DATE NOT NULL,
                    scan_time TIMESTAMPTZ NOT NULL,
                    symbol VARCHAR(32) NOT NULL,
                    instrument_key TEXT,
                    exclusion_reason VARCHAR(64) NOT NULL,
                    detail TEXT,
                    kavach_state VARCHAR(32),
                    relative_strength DOUBLE PRECISION,
                    trade_score DOUBLE PRECISION,
                    confidence_grade TEXT,
                    ranking_side VARCHAR(16),
                    would_be_rank INTEGER,
                    rank_cutoff INTEGER,
                    top_n_cutoff INTEGER,
                    cutoff_rs_persist DOUBLE PRECISION,
                    cutoff_rs_top_n DOUBLE PRECISION,
                    current_price DOUBLE PRECISION,
                    volume_ratio DOUBLE PRECISION,
                    volume_label TEXT,
                    scan_trigger VARCHAR(64),
                    logged_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (scan_time, symbol)
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS idx_rs_excl_session_sym
                ON rs_scan_exclusion_log (session_date, symbol, scan_time)
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS idx_rs_excl_scan
                ON rs_scan_exclusion_log (scan_time)
                """
            )
        )
    _ENSURED = True


def exclusion_row(
    *,
    symbol: str,
    exclusion_reason: str,
    instrument_key: Optional[str] = None,
    detail: Optional[str] = None,
    kavach_state: Optional[str] = None,
    relative_strength: Optional[float] = None,
    trade_score: Optional[float] = None,
    confidence_grade: Optional[str] = None,
    ranking_side: Optional[str] = None,
    would_be_rank: Optional[int] = None,
    rank_cutoff: Optional[int] = None,
    top_n_cutoff: Optional[int] = None,
    cutoff_rs_persist: Optional[float] = None,
    cutoff_rs_top_n: Optional[float] = None,
    current_price: Optional[float] = None,
    volume_ratio: Optional[float] = None,
    volume_label: Optional[str] = None,
) -> Dict[str, Any]:
    return {
        "symbol": (symbol or "").upper(),
        "instrument_key": instrument_key or None,
        "exclusion_reason": exclusion_reason,
        "detail": detail,
        "kavach_state": kavach_state,
        "relative_strength": relative_strength,
        "trade_score": trade_score,
        "confidence_grade": confidence_grade,
        "ranking_side": ranking_side,
        "would_be_rank": would_be_rank,
        "rank_cutoff": rank_cutoff,
        "top_n_cutoff": top_n_cutoff,
        "cutoff_rs_persist": cutoff_rs_persist,
        "cutoff_rs_top_n": cutoff_rs_top_n,
        "current_price": current_price,
        "volume_ratio": volume_ratio,
        "volume_label": volume_label,
    }


def write_exclusion_log(
    *,
    scan_time: datetime,
    scan_trigger: str,
    exclusions: List[Dict[str, Any]],
) -> int:
    """Persist exclusion rows. Never raises into the scan path (swallows errors)."""
    if not exclusions:
        return 0
    try:
        ensure_rs_scan_exclusion_log()
        session_date = scan_time.astimezone(IST).strftime("%Y-%m-%d") if scan_time.tzinfo else scan_time.strftime("%Y-%m-%d")
        params = []
        for e in exclusions:
            sym = (e.get("symbol") or "").upper()
            if not sym or not e.get("exclusion_reason"):
                continue
            params.append(
                {
                    "session_date": session_date,
                    "scan_time": scan_time,
                    "symbol": sym,
                    "instrument_key": e.get("instrument_key"),
                    "exclusion_reason": e.get("exclusion_reason"),
                    "detail": e.get("detail"),
                    "kavach_state": e.get("kavach_state"),
                    "relative_strength": e.get("relative_strength"),
                    "trade_score": e.get("trade_score"),
                    "confidence_grade": e.get("confidence_grade"),
                    "ranking_side": e.get("ranking_side"),
                    "would_be_rank": e.get("would_be_rank"),
                    "rank_cutoff": e.get("rank_cutoff"),
                    "top_n_cutoff": e.get("top_n_cutoff"),
                    "cutoff_rs_persist": e.get("cutoff_rs_persist"),
                    "cutoff_rs_top_n": e.get("cutoff_rs_top_n"),
                    "current_price": e.get("current_price"),
                    "volume_ratio": e.get("volume_ratio"),
                    "volume_label": e.get("volume_label"),
                    "scan_trigger": scan_trigger,
                }
            )
        if not params:
            return 0
        db = SessionLocal()
        try:
            db.execute(_INSERT, params)
            db.commit()
            return len(params)
        finally:
            db.close()
    except Exception as exc:
        logger.warning("rs_scan_exclusion_log write failed: %s", exc)
        return 0


def fetch_exclusions_for_symbol(
    db, *, session_date: str, symbol: str
) -> List[Dict[str, Any]]:
    ensure_rs_scan_exclusion_log()
    rows = db.execute(
        text(
            """
            SELECT scan_time, symbol, instrument_key, exclusion_reason, detail,
                   kavach_state, relative_strength, trade_score, confidence_grade,
                   ranking_side, would_be_rank, rank_cutoff, top_n_cutoff,
                   cutoff_rs_persist, cutoff_rs_top_n, current_price,
                   volume_ratio, volume_label, scan_trigger, logged_at
            FROM rs_scan_exclusion_log
            WHERE session_date = CAST(:d AS date) AND UPPER(symbol) = :s
            ORDER BY scan_time
            """
        ),
        {"d": session_date, "s": symbol.upper()},
    ).fetchall()
    return [dict(r._mapping) for r in rows]

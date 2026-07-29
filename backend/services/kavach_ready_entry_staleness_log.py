"""Shadow-only: READY NOW entry-price staleness vs LTP / EMA5.

Instrumentation only — does not change entry calculation, promotion, countdown,
or Take Trade. Captures gap between card entry and live price at READY /
recheck moments so we can tell whether entry was re-anchored.

Table: kavach_ready_entry_staleness_log
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, time as dtime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pytz
from sqlalchemy import text

from backend.database import engine

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

_ENSURED = False
TABLE = "kavach_ready_entry_staleness_log"

# App-level event labels (no DB CHECK).
EVENT_INITIAL = "initial_promotion"
EVENT_RECHECK = "recheck"

# Skip identical spam within this window (seconds) unless entry/attempt changes.
_DEDUP_SEC = 45


def ensure_ready_entry_staleness_log() -> None:
    global _ENSURED
    if _ENSURED:
        return
    with engine.begin() as conn:
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {TABLE} (
                    id BIGSERIAL PRIMARY KEY,
                    session_date DATE NOT NULL,
                    symbol VARCHAR(32) NOT NULL,
                    direction VARCHAR(8),
                    logged_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    event_type VARCHAR(32) NOT NULL,
                    attempt_number INTEGER,
                    rendered_state VARCHAR(32),
                    card_visible BOOLEAN,
                    dwell_soft_hold BOOLEAN,
                    trade_take_enabled BOOLEAN,
                    entry_price DOUBLE PRECISION,
                    entry_price_last_computed_ts TIMESTAMPTZ,
                    entry_matches_ema5 BOOLEAN,
                    current_ltp DOUBLE PRECISION,
                    gap_pct DOUBLE PRECISION,
                    gap_pts DOUBLE PRECISION,
                    ema5_value DOUBLE PRECISION,
                    ema10_value DOUBLE PRECISION,
                    confidence_grade TEXT,
                    trade_score DOUBLE PRECISION,
                    pine_readiness TEXT,
                    atr_pct DOUBLE PRECISION,
                    source VARCHAR(32) NOT NULL DEFAULT 'live',
                    inputs JSONB NOT NULL DEFAULT '{{}}'::jsonb
                )
                """
            )
        )
        conn.execute(
            text(
                f"""
                CREATE INDEX IF NOT EXISTS idx_ready_entry_staleness_sess_sym
                ON {TABLE} (session_date, symbol, logged_at)
                """
            )
        )
        conn.execute(
            text(
                f"""
                CREATE INDEX IF NOT EXISTS idx_ready_entry_staleness_event
                ON {TABLE} (session_date, event_type, gap_pct)
                """
            )
        )
    _ENSURED = True


def _f(v: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if v is None or v == "":
            return default
        return float(v)
    except (TypeError, ValueError):
        return default


def _as_ist(ts: Any) -> Optional[datetime]:
    if ts is None:
        return None
    if isinstance(ts, datetime):
        return ts.astimezone(IST) if ts.tzinfo else IST.localize(ts)
    try:
        dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except Exception:
        return None
    return dt.astimezone(IST) if dt.tzinfo else IST.localize(dt)


def _ten_min_slot_index(ts: datetime) -> int:
    """IST session-relative 10m bucket index from 09:15 (matches FE countdown slots)."""
    t = ts.astimezone(IST) if ts.tzinfo else IST.localize(ts)
    minutes = t.hour * 60 + t.minute
    open_m = 9 * 60 + 15
    if minutes < open_m:
        return 0
    return (minutes - open_m) // 10


def _attempt_from_since(since: Optional[datetime], now: datetime) -> int:
    """Approximate FE attempt: 1 + number of 10m boundaries crossed while still READY."""
    if since is None:
        return 1
    s = _as_ist(since) or since
    n = _as_ist(now) or now
    if n <= s:
        return 1
    return 1 + max(0, _ten_min_slot_index(n) - _ten_min_slot_index(s))


def _last_row(db, session_date: str, symbol: str) -> Optional[Dict[str, Any]]:
    try:
        row = db.execute(
            text(
                f"""
                SELECT entry_price, entry_price_last_computed_ts, event_type,
                       attempt_number, logged_at, gap_pct, rendered_state
                FROM {TABLE}
                WHERE session_date = CAST(:d AS date) AND UPPER(symbol) = :sym
                ORDER BY logged_at DESC, id DESC
                LIMIT 1
                """
            ),
            {"d": session_date, "sym": symbol.upper()},
        ).mappings().fetchone()
        return dict(row) if row else None
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass
        return None


def _resolve_computed_ts(
    *,
    entry: Optional[float],
    ema5: Optional[float],
    now: datetime,
    prev: Optional[Dict[str, Any]],
) -> Tuple[datetime, bool]:
    """If entry ≈ live EMA5, treat as freshly computed; else carry sticky timestamp."""
    matches = False
    if entry is not None and ema5 is not None:
        matches = abs(float(entry) - round(float(ema5), 2)) <= 0.02
    if matches:
        return now, True
    if prev and prev.get("entry_price") is not None and entry is not None:
        if abs(float(prev["entry_price"]) - float(entry)) <= 0.02:
            prev_ts = _as_ist(prev.get("entry_price_last_computed_ts"))
            if prev_ts is not None:
                return prev_ts, False
    return now, matches


def build_staleness_row(
    *,
    session_date: str,
    stock: Dict[str, Any],
    levels: Optional[Dict[str, Any]] = None,
    now: Optional[datetime] = None,
    prev: Optional[Dict[str, Any]] = None,
    source: str = "live",
) -> Optional[Dict[str, Any]]:
    """Build one shadow row if the card is READY-family or soft-held READY card."""
    levels = levels or {}
    sym = (stock.get("symbol") or "").upper()
    if not sym:
        return None
    state = (stock.get("trade_state") or "").upper()
    ready_family = state in ("READY", "READY(RECHECK)") or state.startswith("READY")
    card_visible = bool(stock.get("card_visible"))
    soft = bool(stock.get("dwell_soft_hold"))
    # Surface cases: live READY, or dwell soft-hold still showing the card.
    if not (ready_family or (card_visible and soft)):
        return None

    clock = now or datetime.now(IST)
    if clock.tzinfo is None:
        clock = IST.localize(clock)
    else:
        clock = clock.astimezone(IST)

    entry = _f(stock.get("trade_entry"))
    ema5 = _f(
        stock.get("live_candle_ema5")
        or levels.get("ema5")
        or levels.get("ema5_10m")
    )
    ema10 = _f(
        stock.get("live_candle_ema10")
        or levels.get("ema10")
        or levels.get("ema10_10m")
        or stock.get("trade_sl")
    )
    ltp = _f(
        stock.get("live_candle_price")
        or levels.get("price")
        or stock.get("ltp")
    )
    computed_ts, matches = _resolve_computed_ts(
        entry=entry, ema5=ema5, now=clock, prev=prev
    )

    gap_pct = None
    gap_pts = None
    if entry is not None and entry != 0 and ltp is not None:
        gap_pts = round(float(ltp) - float(entry), 4)
        gap_pct = round(100.0 * gap_pts / float(entry), 4)

    since = _as_ist(stock.get("ready_visible_since"))
    attempt = _attempt_from_since(since, clock)

    # Spell continuity: no prior row today, or last row was different state / long gap → initial.
    event_type = EVENT_RECHECK
    if prev is None:
        event_type = EVENT_INITIAL
    else:
        prev_at = _as_ist(prev.get("logged_at"))
        prev_state = (prev.get("rendered_state") or "").upper()
        if prev_state and prev_state not in ("READY", "READY(RECHECK)") and not prev_state.startswith("READY"):
            event_type = EVENT_INITIAL
        elif prev_at is not None and (clock - prev_at).total_seconds() > 20 * 60:
            event_type = EVENT_INITIAL
        elif prev.get("event_type") == EVENT_INITIAL and attempt <= 1:
            # Still first attempt after initial — call subsequent polls recheck only after attempt≥2
            # or entry drift; first few polls stay recheck for continuity of gap tracking.
            event_type = EVENT_RECHECK

    grade = stock.get("confidence") or stock.get("dashboard_kavach") or levels.get(
        "confidence_grade"
    )
    score = _f(stock.get("trade_score") or stock.get("dashboard_score") or levels.get("trade_score"))

    return {
        "session_date": session_date,
        "symbol": sym,
        "direction": (stock.get("direction") or "LONG").upper(),
        "logged_at": clock,
        "event_type": event_type,
        "attempt_number": int(attempt),
        "rendered_state": stock.get("trade_state"),
        "card_visible": card_visible,
        "dwell_soft_hold": soft,
        "trade_take_enabled": bool(stock.get("trade_take_enabled")),
        "entry_price": entry,
        "entry_price_last_computed_ts": computed_ts,
        "entry_matches_ema5": matches,
        "current_ltp": ltp,
        "gap_pct": gap_pct,
        "gap_pts": gap_pts,
        "ema5_value": round(ema5, 4) if ema5 is not None else None,
        "ema10_value": round(ema10, 4) if ema10 is not None else None,
        "confidence_grade": str(grade) if grade is not None else None,
        "trade_score": score,
        "pine_readiness": stock.get("pine_readiness"),
        "atr_pct": _f(stock.get("atr_pct") or (stock.get("atr_consumed") or {}).get("atr_pct")),
        "source": source,
        "inputs": {
            "ready_visible_since": stock.get("ready_visible_since"),
            "trade_state_reason": stock.get("trade_state_reason"),
            "trade_sl": stock.get("trade_sl"),
            "entry_vs_ema5_pts": (
                round(float(entry) - float(ema5), 4)
                if entry is not None and ema5 is not None
                else None
            ),
        },
    }


def _should_skip_dedup(prev: Optional[Dict[str, Any]], row: Dict[str, Any], now: datetime) -> bool:
    if not prev:
        return False
    prev_at = _as_ist(prev.get("logged_at"))
    if prev_at is None:
        return False
    if (now - prev_at).total_seconds() > _DEDUP_SEC:
        return False
    same_entry = (
        prev.get("entry_price") is not None
        and row.get("entry_price") is not None
        and abs(float(prev["entry_price"]) - float(row["entry_price"])) <= 0.02
    )
    same_attempt = int(prev.get("attempt_number") or 0) == int(row.get("attempt_number") or 0)
    same_event = prev.get("event_type") == row.get("event_type")
    # Always keep if gap crossed a 2% threshold vs previous
    prev_gap = abs(_f(prev.get("gap_pct"), 0.0) or 0.0)
    cur_gap = abs(_f(row.get("gap_pct"), 0.0) or 0.0)
    crossed = (prev_gap < 2.0 <= cur_gap) or (prev_gap < 5.0 <= cur_gap)
    if crossed:
        return False
    return same_entry and same_attempt and same_event


def insert_staleness_row(db, row: Dict[str, Any]) -> Optional[int]:
    ensure_ready_entry_staleness_log()
    try:
        rid = db.execute(
            text(
                f"""
                INSERT INTO {TABLE} (
                    session_date, symbol, direction, logged_at, event_type, attempt_number,
                    rendered_state, card_visible, dwell_soft_hold, trade_take_enabled,
                    entry_price, entry_price_last_computed_ts, entry_matches_ema5,
                    current_ltp, gap_pct, gap_pts, ema5_value, ema10_value,
                    confidence_grade, trade_score, pine_readiness, atr_pct, source, inputs
                ) VALUES (
                    CAST(:session_date AS date), :symbol, :direction, :logged_at,
                    :event_type, :attempt_number,
                    :rendered_state, :card_visible, :dwell_soft_hold, :trade_take_enabled,
                    :entry_price, :entry_price_last_computed_ts, :entry_matches_ema5,
                    :current_ltp, :gap_pct, :gap_pts, :ema5_value, :ema10_value,
                    :confidence_grade, :trade_score, :pine_readiness, :atr_pct, :source,
                    CAST(:inputs AS jsonb)
                )
                RETURNING id
                """
            ),
            {
                **row,
                "inputs": json.dumps(row.get("inputs") or {}),
            },
        ).scalar()
        return int(rid) if rid is not None else None
    except Exception as exc:
        logger.debug("ready entry staleness insert skipped: %s", exc)
        try:
            db.rollback()
        except Exception:
            pass
        return None


def log_ready_entry_staleness_for_stocks(
    db,
    *,
    session_date: str,
    stocks: List[Dict[str, Any]],
    levels_map: Optional[Dict[str, Dict[str, Any]]] = None,
    source: str = "live",
    now: Optional[datetime] = None,
) -> int:
    """Best-effort shadow write for READY / soft-held cards. Never raises to caller."""
    if not stocks:
        return 0
    try:
        ensure_ready_entry_staleness_log()
    except Exception as exc:
        logger.debug("ready entry staleness ensure skipped: %s", exc)
        return 0

    levels_map = levels_map or {}
    clock = now or datetime.now(IST)
    if clock.tzinfo is None:
        clock = IST.localize(clock)
    else:
        clock = clock.astimezone(IST)

    n = 0
    for s in stocks:
        sym = (s.get("symbol") or "").upper()
        if not sym:
            continue
        try:
            prev = _last_row(db, session_date, sym)
            row = build_staleness_row(
                session_date=session_date,
                stock=s,
                levels=levels_map.get(sym) or {},
                now=clock,
                prev=prev,
                source=source,
            )
            if not row:
                continue
            if _should_skip_dedup(prev, row, clock):
                continue
            if insert_staleness_row(db, row):
                n += 1
        except Exception as exc:
            logger.debug("ready entry staleness row skipped %s: %s", sym, exc)
    if n:
        try:
            db.commit()
        except Exception as exc:
            logger.debug("ready entry staleness commit skipped: %s", exc)
            try:
                db.rollback()
            except Exception:
                pass
    return n

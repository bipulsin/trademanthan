"""Rule 27 session journal — daily stop-trading behavior (shadow logging).

Tracks whether the trader stopped mid-window voluntarily vs loss-cap / EOD /
no-setups, for later comparison of early profit-lock vs full-window days.
No live gating.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, time, timedelta
from typing import Any, Dict, Optional

import pytz
from sqlalchemy import text

from backend.database import SessionLocal, engine

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

TABLE = "trade_session_log"

# Entry window matches READY NOW / Take Trade (09:45–14:30 IST).
ENTRY_WINDOW_OPEN = time(9, 45)
ENTRY_WINDOW_CLOSE = time(14, 30)

SESSION_END_REASONS = (
    "voluntary_profit_lock",
    "daily_loss_cap_hit",
    "time_square_off",
    "no_setups_available",
)

_CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    session_date DATE PRIMARY KEY,
    trades_taken_count INTEGER NOT NULL DEFAULT 0,
    last_exit_time TIME,
    entry_window_remaining_at_last_exit BOOLEAN,
    entry_window_remaining_minutes INTEGER,
    session_end_reason TEXT,
    net_pnl_at_session_end DOUBLE PRECISION,
    notes TEXT,
    source TEXT DEFAULT 'manual',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
"""

_UPSERT = text(
    f"""
    INSERT INTO {TABLE} (
        session_date, trades_taken_count, last_exit_time,
        entry_window_remaining_at_last_exit, entry_window_remaining_minutes,
        session_end_reason, net_pnl_at_session_end, notes, source, updated_at
    ) VALUES (
        CAST(:session_date AS date), :trades_taken_count, CAST(:last_exit_time AS time),
        :entry_window_remaining_at_last_exit, :entry_window_remaining_minutes,
        :session_end_reason, :net_pnl_at_session_end, :notes, :source, NOW()
    )
    ON CONFLICT (session_date) DO UPDATE SET
        trades_taken_count = EXCLUDED.trades_taken_count,
        last_exit_time = COALESCE(EXCLUDED.last_exit_time, {TABLE}.last_exit_time),
        entry_window_remaining_at_last_exit = COALESCE(
            EXCLUDED.entry_window_remaining_at_last_exit,
            {TABLE}.entry_window_remaining_at_last_exit
        ),
        entry_window_remaining_minutes = COALESCE(
            EXCLUDED.entry_window_remaining_minutes,
            {TABLE}.entry_window_remaining_minutes
        ),
        session_end_reason = COALESCE(EXCLUDED.session_end_reason, {TABLE}.session_end_reason),
        net_pnl_at_session_end = COALESCE(
            EXCLUDED.net_pnl_at_session_end, {TABLE}.net_pnl_at_session_end
        ),
        notes = COALESCE(EXCLUDED.notes, {TABLE}.notes),
        source = EXCLUDED.source,
        updated_at = NOW()
    RETURNING session_date
    """
)


def ensure_trade_session_log_table() -> None:
    with engine.begin() as conn:
        conn.execute(text(_CREATE_SQL))


def normalize_session_end_reason(val: Any) -> Optional[str]:
    if val is None or val == "":
        return None
    s = str(val).strip().lower().replace("-", "_").replace(" ", "_")
    aliases = {
        "profit_lock": "voluntary_profit_lock",
        "voluntary": "voluntary_profit_lock",
        "loss_cap": "daily_loss_cap_hit",
        "daily_loss": "daily_loss_cap_hit",
        "square_off": "time_square_off",
        "eod": "time_square_off",
        "no_setups": "no_setups_available",
    }
    s = aliases.get(s, s)
    return s if s in SESSION_END_REASONS else s


def _as_date(val: Any) -> Optional[date]:
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    return date.fromisoformat(str(val)[:10])


def _as_time(val: Any) -> Optional[time]:
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.time().replace(microsecond=0)
    if isinstance(val, time):
        return val.replace(microsecond=0)
    s = str(val).strip()
    for fmt in ("%H:%M:%S", "%H:%M"):
        try:
            return datetime.strptime(s, fmt).time()
        except ValueError:
            continue
    return None


def minutes_remaining_in_entry_window(last_exit: Optional[time]) -> Optional[int]:
    """Minutes from last_exit to 14:30 IST; 0 if at/after close; None if unknown."""
    if last_exit is None:
        return None
    base = datetime(2000, 1, 1)
    rem = (datetime.combine(base.date(), ENTRY_WINDOW_CLOSE) - datetime.combine(base.date(), last_exit)).total_seconds() / 60.0
    return max(0, int(round(rem)))


def row_params(payload: Dict[str, Any]) -> Dict[str, Any]:
    last_exit = _as_time(payload.get("last_exit_time"))
    rem_min = payload.get("entry_window_remaining_minutes")
    if rem_min is None and last_exit is not None:
        rem_min = minutes_remaining_in_entry_window(last_exit)
    rem_flag = payload.get("entry_window_remaining_at_last_exit")
    if rem_flag is None and rem_min is not None:
        rem_flag = rem_min > 0
    elif isinstance(rem_flag, str):
        rem_flag = rem_flag.strip().lower() in ("yes", "y", "true", "1")
    return {
        "session_date": str(_as_date(payload["session_date"])),
        "trades_taken_count": int(payload.get("trades_taken_count") or 0),
        "last_exit_time": last_exit.strftime("%H:%M:%S") if last_exit else None,
        "entry_window_remaining_at_last_exit": bool(rem_flag) if rem_flag is not None else None,
        "entry_window_remaining_minutes": int(rem_min) if rem_min is not None else None,
        "session_end_reason": normalize_session_end_reason(payload.get("session_end_reason")),
        "net_pnl_at_session_end": (
            float(payload["net_pnl_at_session_end"])
            if payload.get("net_pnl_at_session_end") is not None
            else None
        ),
        "notes": payload.get("notes"),
        "source": payload.get("source") or "manual",
    }


def upsert_session(db, payload: Dict[str, Any]) -> str:
    ensure_trade_session_log_table()
    params = row_params(payload)
    sd = db.execute(_UPSERT, params).scalar()
    return str(sd)


def net_pnl_from_trade_log(db, session_date: str) -> float:
    row = db.execute(
        text(
            """
            SELECT COALESCE(SUM(points_captured * qty), 0) AS pnl
            FROM trade_log
            WHERE session_date = CAST(:d AS date)
              AND points_captured IS NOT NULL
              AND qty IS NOT NULL
            """
        ),
        {"d": session_date},
    ).mappings().first()
    return round(float(row["pnl"] or 0), 2)

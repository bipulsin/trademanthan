"""Shadow-only: Watching ∩ Grade A/A+ appearance counter + leave-reason log.

Prospective instrumentation for 8-Aug (no live gates):
  - ``kavach_watching_grade_a_counter`` — running seq per symbol×direction×contract_month
  - ``kavach_watching_grade_a_episode`` — sticky open episode; on leave writes reason

Watching predicate prefers live ``pine_readiness == WATCHING`` with grade A/A+
(family). Fallback when readiness missing: Grade A/A+ + not ops READY + in_lock.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

import pytz
from sqlalchemy import text

from backend.database import engine

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")
_ENSURED = False

COUNTER_TABLE = "kavach_watching_grade_a_counter"
EPISODE_TABLE = "kavach_watching_grade_a_episode"

LEAVE_REASONS = (
    "grade_decay_below_a",
    "direction_flip",
    "lock_removed",
    "session_eod",
    "promoted_to_ready",
    "expired_move",
    "left_watching",
    "unknown",
)

_CREATE_COUNTER = f"""
CREATE TABLE IF NOT EXISTS {COUNTER_TABLE} (
    symbol TEXT NOT NULL,
    direction TEXT NOT NULL,
    contract_month TEXT NOT NULL,
    appearance_seq INTEGER NOT NULL DEFAULT 0,
    cycle_started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    first_seen_at TIMESTAMPTZ,
    last_seen_at TIMESTAMPTZ,
    last_session_date DATE,
    last_grade TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (symbol, direction, contract_month)
)
"""

_CREATE_EPISODE = f"""
CREATE TABLE IF NOT EXISTS {EPISODE_TABLE} (
    id BIGSERIAL PRIMARY KEY,
    session_date DATE NOT NULL,
    symbol TEXT NOT NULL,
    direction TEXT NOT NULL,
    contract_month TEXT NOT NULL,
    appearance_seq INTEGER NOT NULL,
    entered_at TIMESTAMPTZ NOT NULL,
    grade_at_enter TEXT,
    pine_readiness_at_enter TEXT,
    left_at TIMESTAMPTZ,
    leave_reason TEXT,
    grade_at_leave TEXT,
    pine_readiness_at_leave TEXT,
    trade_state_at_leave TEXT,
    in_lock_at_leave BOOLEAN,
    source TEXT DEFAULT 'live',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
"""


def ensure_watching_shadow_tables() -> None:
    global _ENSURED
    if _ENSURED:
        return
    with engine.begin() as conn:
        conn.execute(text(_CREATE_COUNTER))
        conn.execute(text(_CREATE_EPISODE))
        conn.execute(
            text(
                f"CREATE INDEX IF NOT EXISTS idx_{EPISODE_TABLE}_open "
                f"ON {EPISODE_TABLE} (session_date, symbol) "
                f"WHERE left_at IS NULL"
            )
        )
        conn.execute(
            text(
                f"CREATE INDEX IF NOT EXISTS idx_{EPISODE_TABLE}_session "
                f"ON {EPISODE_TABLE} (session_date DESC, symbol)"
            )
        )
    _ENSURED = True


def _norm_grade_base(g: Optional[str]) -> Optional[str]:
    if not g:
        return None
    s = str(g).strip().upper().replace("*", "")
    if s.startswith("A+"):
        return "A+"
    if s.startswith("A"):
        return "A"
    if s.startswith("B"):
        return "B"
    if s.startswith("C"):
        return "C"
    if s.startswith("D"):
        return "D"
    return s


def is_grade_a_family(g: Optional[str]) -> bool:
    return _norm_grade_base(g) in ("A", "A+")


def is_ready_like(state: Optional[str]) -> bool:
    s = (state or "").upper()
    return s in ("READY", "READY(RECHECK)") or s.startswith("READY")


def is_watching_grade_a(stock: Dict[str, Any], *, in_lock: bool) -> bool:
    """Live Watching ∩ A/A+ (prefer pine_readiness)."""
    if not in_lock:
        return False
    if is_ready_like(stock.get("trade_state")):
        return False
    grade = stock.get("confidence") or stock.get("dashboard_kavach") or stock.get("confidence_grade")
    if not is_grade_a_family(grade):
        return False
    pine = (stock.get("pine_readiness") or "").strip().upper()
    if pine:
        return pine == "WATCHING"
    # Fallback when readiness not attached: grade A/A+ + not READY already checked
    return True


def contract_month_for_symbol(db, symbol: str) -> str:
    """YYYY-MM of front-month FUT expiry from arbitrage_master + instruments file."""
    from backend.config import get_instruments_file_path
    import json

    sym = (symbol or "").upper()
    row = db.execute(
        text(
            """
            SELECT currmth_future_instrument_key AS ikey,
                   currmth_future_symbol AS tsym
            FROM arbitrage_master
            WHERE UPPER(stock) = :s
            LIMIT 1
            """
        ),
        {"s": sym},
    ).mappings().first()
    ikey = (row or {}).get("ikey")
    tsym = (row or {}).get("tsym") or ""
    if tsym:
        parts = str(tsym).upper().split()
        if "FUT" in parts:
            i = parts.index("FUT")
            if i + 3 < len(parts):
                mon, yy = parts[i + 2], parts[i + 3]
                months = {
                    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
                    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
                }
                if mon in months and yy.isdigit():
                    year = 2000 + int(yy) if len(yy) == 2 else int(yy)
                    return f"{year:04d}-{months[mon]:02d}"
    if not ikey:
        return "UNKNOWN"
    try:
        path = get_instruments_file_path()
        data = json.loads(path.read_text(encoding="utf-8")) if path.exists() else []
        for inst in data:
            if not isinstance(inst, dict):
                continue
            if str(inst.get("instrument_key") or "").strip() != str(ikey).strip():
                continue
            exp = inst.get("expiry")
            if exp:
                dt = datetime.fromtimestamp(float(exp) / 1000.0, tz=IST)
                return dt.strftime("%Y-%m")
            ts = str(inst.get("trading_symbol") or "")
            parts = ts.upper().split()
            if "FUT" in parts:
                i = parts.index("FUT")
                if i + 3 < len(parts):
                    mon, yy = parts[i + 2], parts[i + 3]
                    months = {
                        "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
                        "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
                    }
                    if mon in months and str(yy).isdigit():
                        year = 2000 + int(yy) if len(yy) == 2 else int(yy)
                        return f"{year:04d}-{months[mon]:02d}"
    except Exception:
        logger.debug("contract_month resolve failed for %s", sym, exc_info=True)
    return str(ikey)


def _now_ist(now: Optional[datetime] = None) -> datetime:
    if now is None:
        return datetime.now(IST)
    if now.tzinfo is None:
        return IST.localize(now)
    return now.astimezone(IST)


def _increment_counter(
    db,
    *,
    symbol: str,
    direction: str,
    contract_month: str,
    session_date: str,
    grade: Optional[str],
    now: datetime,
) -> int:
    row = db.execute(
        text(
            f"""
            INSERT INTO {COUNTER_TABLE} (
                symbol, direction, contract_month, appearance_seq,
                cycle_started_at, first_seen_at, last_seen_at,
                last_session_date, last_grade, updated_at
            ) VALUES (
                :s, :d, :cm, 1, :now, :now, :now, CAST(:sd AS date), :g, :now
            )
            ON CONFLICT (symbol, direction, contract_month) DO UPDATE SET
                appearance_seq = {COUNTER_TABLE}.appearance_seq + 1,
                last_seen_at = EXCLUDED.last_seen_at,
                last_session_date = EXCLUDED.last_session_date,
                last_grade = EXCLUDED.last_grade,
                updated_at = EXCLUDED.updated_at
            RETURNING appearance_seq
            """
        ),
        {
            "s": symbol,
            "d": direction,
            "cm": contract_month,
            "now": now,
            "sd": session_date,
            "g": grade,
        },
    ).scalar()
    return int(row or 1)


def _load_open_episodes(db, session_date: str) -> Dict[Tuple[str, str], Dict[str, Any]]:
    rows = db.execute(
        text(
            f"""
            SELECT id, session_date, symbol, direction, contract_month,
                   appearance_seq, entered_at, grade_at_enter
            FROM {EPISODE_TABLE}
            WHERE session_date = CAST(:d AS date)
              AND left_at IS NULL
            """
        ),
        {"d": session_date},
    ).mappings()
    out: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for r in rows:
        out[(str(r["symbol"]).upper(), str(r["direction"] or "").upper())] = dict(r)
    return out


def _close_episode(
    db,
    *,
    episode_id: int,
    leave_reason: str,
    grade_at_leave: Optional[str],
    pine_readiness_at_leave: Optional[str],
    trade_state_at_leave: Optional[str],
    in_lock_at_leave: Optional[bool],
    now: datetime,
) -> None:
    reason = leave_reason if leave_reason in LEAVE_REASONS else "unknown"
    db.execute(
        text(
            f"""
            UPDATE {EPISODE_TABLE}
            SET left_at = :now,
                leave_reason = :reason,
                grade_at_leave = :g,
                pine_readiness_at_leave = :pine,
                trade_state_at_leave = :ts,
                in_lock_at_leave = :il,
                updated_at = :now
            WHERE id = :id AND left_at IS NULL
            """
        ),
        {
            "now": now,
            "reason": reason,
            "g": grade_at_leave,
            "pine": pine_readiness_at_leave,
            "ts": trade_state_at_leave,
            "il": in_lock_at_leave,
            "id": episode_id,
        },
    )


def _classify_leave(
    *,
    prev_dir: str,
    stock: Optional[Dict[str, Any]],
    still_in_universe: bool,
    in_lock: bool,
    now: datetime,
) -> str:
    if not still_in_universe or not in_lock:
        return "lock_removed"
    if stock is None:
        return "lock_removed"
    if is_ready_like(stock.get("trade_state")):
        return "promoted_to_ready"
    pine = (stock.get("pine_readiness") or "").strip().upper()
    if pine.startswith("READY TO"):
        return "promoted_to_ready"
    st = (stock.get("trade_state") or "").upper()
    if st == "EXPIRED" or stock.get("trade_expiry_crossed"):
        return "expired_move"
    grade = stock.get("confidence") or stock.get("dashboard_kavach")
    if not is_grade_a_family(grade):
        return "grade_decay_below_a"
    cur_dir = (stock.get("direction") or "").upper()
    if cur_dir and prev_dir and cur_dir != prev_dir:
        return "direction_flip"
    if pine and pine != "WATCHING":
        return "left_watching"
    # After square-off with no more specific cause.
    if now.timetz().replace(tzinfo=None) >= datetime.strptime("15:15", "%H:%M").time():
        return "session_eod"
    return "unknown"


def update_watching_grade_a_shadow(
    db,
    *,
    session_date: str,
    stocks: List[Dict[str, Any]],
    lock_map: Optional[Dict[str, Any]] = None,
    now: Optional[datetime] = None,
    source: str = "live",
) -> Dict[str, int]:
    """Start/end Watching∩A episodes; increment contract-cycle appearance counter.

    Best-effort; never raises into enrich.
    """
    stats = {"started": 0, "ended": 0, "touched": 0}
    try:
        ensure_watching_shadow_tables()
        now_i = _now_ist(now)
        lock_map = lock_map or {}
        open_eps = _load_open_episodes(db, session_date)
        # Close stale open episodes from prior sessions (missed EOD).
        try:
            stale = db.execute(
                text(
                    f"""
                    SELECT id FROM {EPISODE_TABLE}
                    WHERE left_at IS NULL
                      AND session_date < CAST(:d AS date)
                    """
                ),
                {"d": session_date},
            ).fetchall()
            for (sid,) in stale:
                _close_episode(
                    db,
                    episode_id=int(sid),
                    leave_reason="session_eod",
                    grade_at_leave=None,
                    pine_readiness_at_leave=None,
                    trade_state_at_leave=None,
                    in_lock_at_leave=None,
                    now=now_i,
                )
                stats["ended"] += 1
        except Exception:
            logger.debug("stale watching episode close skipped", exc_info=True)
        seen_active: Set[Tuple[str, str]] = set()
        stock_by: Dict[str, Dict[str, Any]] = {}
        for s in stocks:
            sym = (s.get("symbol") or "").upper()
            if sym:
                stock_by[sym] = s

        # Contract month cache
        cm_cache: Dict[str, str] = {}

        def cm(sym: str) -> str:
            if sym not in cm_cache:
                try:
                    cm_cache[sym] = contract_month_for_symbol(db, sym)
                except Exception:
                    cm_cache[sym] = "UNKNOWN"
            return cm_cache[sym]

        for s in stocks:
            sym = (s.get("symbol") or "").upper()
            if not sym:
                continue
            direction = (s.get("direction") or "LONG").upper()
            in_lock = bool(s.get("in_lock")) or sym in lock_map
            key = (sym, direction)
            active = is_watching_grade_a(s, in_lock=in_lock)
            grade = s.get("confidence") or s.get("dashboard_kavach")
            pine = s.get("pine_readiness")

            if active:
                seen_active.add(key)
                stats["touched"] += 1
                if key not in open_eps:
                    # New appearance this cycle
                    seq = _increment_counter(
                        db,
                        symbol=sym,
                        direction=direction,
                        contract_month=cm(sym),
                        session_date=session_date,
                        grade=str(grade) if grade else None,
                        now=now_i,
                    )
                    db.execute(
                        text(
                            f"""
                            INSERT INTO {EPISODE_TABLE} (
                                session_date, symbol, direction, contract_month,
                                appearance_seq, entered_at, grade_at_enter,
                                pine_readiness_at_enter, source, updated_at
                            ) VALUES (
                                CAST(:sd AS date), :s, :d, :cm, :seq, :now, :g,
                                :pine, :src, :now
                            )
                            """
                        ),
                        {
                            "sd": session_date,
                            "s": sym,
                            "d": direction,
                            "cm": cm(sym),
                            "seq": seq,
                            "now": now_i,
                            "g": grade,
                            "pine": pine,
                            "src": source,
                        },
                    )
                    stats["started"] += 1
                    # refresh open map for same-loop safety
                    open_eps[key] = {"id": None, "direction": direction}
                else:
                    # touch counter last_seen without incrementing
                    db.execute(
                        text(
                            f"""
                            UPDATE {COUNTER_TABLE}
                            SET last_seen_at = :now,
                                last_session_date = CAST(:sd AS date),
                                last_grade = :g,
                                updated_at = :now
                            WHERE symbol = :s AND direction = :d AND contract_month = :cm
                            """
                        ),
                        {
                            "now": now_i,
                            "sd": session_date,
                            "g": grade,
                            "s": sym,
                            "d": direction,
                            "cm": cm(sym),
                        },
                    )

        # Close episodes no longer active
        for key, ep in list(open_eps.items()):
            if key in seen_active:
                continue
            sym, prev_dir = key
            stock = stock_by.get(sym)
            in_lock = bool(stock and (stock.get("in_lock") or sym in lock_map))
            reason = _classify_leave(
                prev_dir=prev_dir,
                stock=stock,
                still_in_universe=stock is not None,
                in_lock=in_lock,
                now=now_i,
            )
            # Need episode id — reload if missing
            eid = ep.get("id")
            if eid is None:
                row = db.execute(
                    text(
                        f"""
                        SELECT id FROM {EPISODE_TABLE}
                        WHERE session_date = CAST(:d AS date)
                          AND UPPER(symbol) = :s AND UPPER(direction) = :dir
                          AND left_at IS NULL
                        ORDER BY entered_at DESC LIMIT 1
                        """
                    ),
                    {"d": session_date, "s": sym, "dir": prev_dir},
                ).fetchone()
                eid = row[0] if row else None
            if eid is None:
                continue
            _close_episode(
                db,
                episode_id=int(eid),
                leave_reason=reason,
                grade_at_leave=(
                    (stock.get("confidence") or stock.get("dashboard_kavach")) if stock else None
                ),
                pine_readiness_at_leave=(stock.get("pine_readiness") if stock else None),
                trade_state_at_leave=(stock.get("trade_state") if stock else None),
                in_lock_at_leave=in_lock if stock else False,
                now=now_i,
            )
            stats["ended"] += 1

        # EOD: close any remaining open if past square-off
        if now_i.timetz().replace(tzinfo=None) >= datetime.strptime("15:15", "%H:%M").time():
            still_open = db.execute(
                text(
                    f"""
                    SELECT id, symbol, direction FROM {EPISODE_TABLE}
                    WHERE session_date = CAST(:d AS date) AND left_at IS NULL
                    """
                ),
                {"d": session_date},
            ).mappings()
            for r in still_open:
                key = (str(r["symbol"]).upper(), str(r["direction"]).upper())
                if key in seen_active:
                    _close_episode(
                        db,
                        episode_id=int(r["id"]),
                        leave_reason="session_eod",
                        grade_at_leave=None,
                        pine_readiness_at_leave=None,
                        trade_state_at_leave=None,
                        in_lock_at_leave=True,
                        now=now_i,
                    )
                    stats["ended"] += 1
    except Exception:
        logger.debug("watching grade A shadow skipped", exc_info=True)
    return stats

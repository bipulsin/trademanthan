"""Fast Watch — unconfirmed chart-level BUY flips outside morning lock visibility.

Scope (default): morning-locked checklist symbols ∪ current RS top-5 per side.
Records first BUY/READY flip per symbol/day; UI highlights symbols not on the lock.
Display: top N per side by grade + score (READY deprioritized), with slot retention.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

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

GRADE_RANK: Dict[str, int] = {"A+": 5, "A": 4, "B": 3, "C": 2, "D": 1}
CONFIRMED_LONG = frozenset({"BUY"})
CONFIRMED_SHORT = frozenset({"SELL"})
WEAK_LONG = frozenset({"READY"})
WEAK_SHORT = frozenset({"READY SHORT"})

# session_date -> side (LONG/SHORT) -> symbol -> slot entered (IST)
_featured_slots: Dict[str, Dict[str, Dict[str, datetime]]] = {}


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


def grade_rank(grade: Optional[str]) -> int:
    return GRADE_RANK.get((grade or "").strip().upper(), 0)


def state_rank(kavach_state: Optional[str], direction: str) -> int:
    """BUY/SELL rank above READY / READY SHORT."""
    k = (kavach_state or "").upper()
    if direction == "SHORT":
        if k in CONFIRMED_SHORT:
            return 2
        if k in WEAK_SHORT:
            return 1
    else:
        if k in CONFIRMED_LONG:
            return 2
        if k in WEAK_LONG:
            return 1
    return 0


def rank_key(item: Dict[str, Any]) -> Tuple[int, int, float]:
    """Higher tuple = stronger candidate (grade tier, then state, then score)."""
    live_grade = item.get("live_grade") or item.get("confidence_grade")
    live_kavach = item.get("live_kavach") or item.get("kavach_state")
    live_score = float(item.get("live_score") if item.get("live_score") is not None else item.get("trade_score") or 0)
    return (grade_rank(live_grade), state_rank(live_kavach, item.get("direction", "LONG")), live_score)


def is_degraded(item: Dict[str, Any]) -> bool:
    """Drop retention early when live strength meaningfully fades."""
    flip_grade = grade_rank(item.get("flip_grade") or item.get("confidence_grade"))
    live_grade = grade_rank(item.get("live_grade") or item.get("confidence_grade"))
    if flip_grade >= 3 and live_grade <= 1:
        return True
    if flip_grade - live_grade >= 2:
        return True
    flip_score = float(item.get("flip_score") if item.get("flip_score") is not None else item.get("trade_score") or 0)
    live_score = float(item.get("live_score") if item.get("live_score") is not None else item.get("trade_score") or 0)
    if flip_score - live_score >= 20:
        return True
    direction = (item.get("direction") or "LONG").upper()
    live_kavach = item.get("live_kavach") or item.get("kavach_state")
    if not _flip_state(live_kavach, direction):
        return True
    return False


def momentum_label(flip_score: Optional[float], live_score: Optional[float]) -> str:
    fs = float(flip_score or 0)
    ls = float(live_score if live_score is not None else flip_score or 0)
    delta = ls - fs
    if delta >= 5:
        return "rising"
    if delta <= -5:
        return "fading"
    return "flat"


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


def _load_live_rs(symbols: List[str], session_date: str) -> Dict[str, Dict[str, Any]]:
    if not symbols:
        return {}
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT DISTINCT ON (s.symbol)
                    s.symbol, s.kavach_state, s.trade_score, s.confidence_grade
                FROM relative_strength_snapshot s
                WHERE s.symbol = ANY(:syms)
                  AND s.scan_time::date = CAST(:d AS date)
                ORDER BY s.symbol, s.scan_time DESC
                """
            ),
            {"syms": symbols, "d": session_date},
        ).fetchall()
        return {
            r.symbol: {
                "live_kavach": r.kavach_state,
                "live_score": r.trade_score,
                "live_grade": r.confidence_grade,
            }
            for r in rows
        }
    finally:
        db.close()


def _enrich_item(raw: Dict[str, Any], live: Dict[str, Any], now: datetime) -> Dict[str, Any]:
    flip_at = raw.get("first_flip_at")
    flip_dt = None
    if flip_at:
        try:
            flip_dt = datetime.fromisoformat(flip_at.replace("Z", "+00:00"))
            if flip_dt.tzinfo is None:
                flip_dt = IST.localize(flip_dt)
        except (TypeError, ValueError):
            flip_dt = None

    flip_score = raw.get("trade_score")
    flip_grade = raw.get("confidence_grade")
    live_score = live.get("live_score", flip_score)
    live_grade = live.get("live_grade", flip_grade)
    live_kavach = live.get("live_kavach", raw.get("kavach_state"))

    minutes_since = 0
    if flip_dt:
        minutes_since = max(0, int((now - flip_dt.astimezone(IST)).total_seconds() // 60))

    score_delta = float(live_score or 0) - float(flip_score or 0)

    return {
        **raw,
        "flip_score": flip_score,
        "flip_grade": flip_grade,
        "live_kavach": live_kavach,
        "live_score": live_score,
        "live_grade": live_grade,
        "kavach_state": live_kavach,
        "confidence_grade": live_grade,
        "trade_score": live_score,
        "minutes_since_flip": minutes_since,
        "score_delta": round(score_delta, 1),
        "momentum": momentum_label(flip_score, live_score),
    }


def _dedupe_by_symbol(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    best: Dict[str, Dict[str, Any]] = {}
    for item in items:
        sym = item["symbol"]
        if sym not in best or rank_key(item) > rank_key(best[sym]):
            best[sym] = item
    return list(best.values())


def _select_featured(
    candidates: List[Dict[str, Any]],
    side: str,
    session_date: str,
    *,
    top_n: int,
    retention_min: int,
    now: datetime,
) -> List[Dict[str, Any]]:
    """Pick top N with minimum retention for symbols already featured."""
    ranked = sorted(_dedupe_by_symbol(candidates), key=rank_key, reverse=True)
    valid_syms = {c["symbol"] for c in ranked}
    by_sym = {c["symbol"]: c for c in ranked}

    day_slots = _featured_slots.setdefault(session_date, {})
    slots: Dict[str, datetime] = day_slots.setdefault(side, {})

    for sym in list(slots.keys()):
        if sym not in valid_syms or is_degraded(by_sym[sym]):
            slots.pop(sym, None)

    protected: List[Dict[str, Any]] = []
    for sym, entered in list(slots.items()):
        if sym not in by_sym:
            continue
        item = by_sym[sym]
        if is_degraded(item):
            slots.pop(sym, None)
            continue
        elapsed_min = (now - entered).total_seconds() / 60.0
        if elapsed_min < retention_min:
            protected.append({**item, "retention_hold": True, "slot_minutes": int(elapsed_min)})

    protected = sorted(protected, key=rank_key, reverse=True)[:top_n]
    output_syms = {p["symbol"] for p in protected}
    output: List[Dict[str, Any]] = list(protected)

    for item in ranked:
        if len(output) >= top_n:
            break
        if item["symbol"] in output_syms:
            continue
        sym = item["symbol"]
        if sym not in slots:
            slots[sym] = now
        elapsed_min = (now - slots[sym]).total_seconds() / 60.0
        output.append({
            **item,
            "retention_hold": elapsed_min < retention_min,
            "slot_minutes": int(elapsed_min),
        })
        output_syms.add(sym)

    output = sorted(output, key=rank_key, reverse=True)[:top_n]
    day_slots[side] = {item["symbol"]: slots.get(item["symbol"], now) for item in output}
    return output


def get_fast_watch(
    session_date: Optional[str] = None,
    *,
    off_lock_only: bool = True,
) -> Dict[str, Any]:
    """Return ranked Fast Watch payload for UI (top N per side + full list)."""
    sd = session_date or today_ist()
    cfg = get_config()
    top_n = max(1, int(cfg.get("fast_watch_top_n") or 2))
    retention_min = max(5, int(cfg.get("fast_watch_retention_minutes") or 12))
    now = datetime.now(IST)

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
    finally:
        db.close()

    raw_items: List[Dict[str, Any]] = []
    for r in rows:
        on_lock = r.symbol in locked
        if off_lock_only and on_lock:
            continue
        flip_at = r.first_flip_at
        raw_items.append(
            {
                "symbol": r.symbol,
                "direction": r.direction,
                "first_flip_at": flip_at.isoformat() if flip_at else None,
                "kavach_state": r.kavach_state,
                "trade_score": r.trade_score,
                "confidence_grade": r.confidence_grade,
                "on_locked_list": on_lock,
                "label": "unconfirmed",
            }
        )

    live_map = _load_live_rs([x["symbol"] for x in raw_items], sd)
    enriched = [_enrich_item(item, live_map.get(item["symbol"], {}), now) for item in raw_items]
    enriched.sort(key=rank_key, reverse=True)

    long_items = [x for x in enriched if (x.get("direction") or "LONG").upper() != "SHORT"]
    short_items = [x for x in enriched if (x.get("direction") or "LONG").upper() == "SHORT"]

    featured_long = _select_featured(long_items, "LONG", sd, top_n=top_n, retention_min=retention_min, now=now)
    featured_short = _select_featured(short_items, "SHORT", sd, top_n=top_n, retention_min=retention_min, now=now)

    return {
        "featured": {"long": featured_long, "short": featured_short},
        "all": enriched,
        "total_count": len(enriched),
    }

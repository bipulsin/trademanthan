"""Morning snapshot lock for Daily RS Checklist — Top 5+5 locked at/after 09:25 IST."""
from __future__ import annotations

import logging
from datetime import datetime
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Tuple

import pytz
from sqlalchemy import text

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")
LOCK_MINUTES_IST = 9 * 60 + 25  # 09:25
# Intraday promote cutoff matches checklist hard entry window end (14:30 IST).
PROMOTION_CUTOFF_MIN = 14 * 60 + 30
PROMOTION_SCANS_REQUIRED = 2  # consecutive Top-5 RS scans, either side


def at_or_after_lock_time(now: Optional[datetime] = None) -> bool:
    now = now or datetime.now(IST)
    m = now.hour * 60 + now.minute
    return m >= LOCK_MINUTES_IST


def is_snapshot_locked(db, session_date: str) -> bool:
    r = db.execute(
        text("SELECT 1 FROM snapshot_lock WHERE lock_date = :d"),
        {"d": session_date},
    ).fetchone()
    return r is not None


def get_lock_info(db, session_date: str) -> Optional[Dict[str, Any]]:
    r = db.execute(
        text("SELECT locked_at, locked_by FROM snapshot_lock WHERE lock_date = :d"),
        {"d": session_date},
    ).fetchone()
    if not r:
        return None
    return {
        "locked_at": r.locked_at.isoformat() if r.locked_at else None,
        "locked_by": r.locked_by or "auto",
    }


def get_locked_symbol_rows(db, session_date: str) -> List[Any]:
    return db.execute(
        text(
            """
            SELECT symbol, direction, rank, rs_score
            FROM daily_snapshot
            WHERE snapshot_date = :d
            ORDER BY CASE direction WHEN 'BULL' THEN 0 ELSE 1 END, rank
            """
        ),
        {"d": session_date},
    ).fetchall()


def get_locked_symbols(db, session_date: str) -> List[str]:
    return [r.symbol for r in get_locked_symbol_rows(db, session_date)]


def locked_direction_map(db, session_date: str) -> Dict[str, str]:
    """Morning-lock direction per symbol (LONG / SHORT from daily_snapshot)."""
    return {
        r.symbol: "LONG" if (r.direction or "").upper() == "BULL" else "SHORT"
        for r in get_locked_symbol_rows(db, session_date)
    }


def snapshot_lock_counts(db, session_date: str) -> Dict[str, int]:
    """Per-side counts from morning daily_snapshot (BULL / BEAR)."""
    rows = db.execute(
        text(
            """
            SELECT direction, COUNT(*)::int AS n
            FROM daily_snapshot
            WHERE snapshot_date = :d
            GROUP BY direction
            """
        ),
        {"d": session_date},
    ).fetchall()
    return {str(r.direction): int(r.n) for r in rows}


def audit_checklist_lock_coverage(
    db,
    session_date: str,
    *,
    rs_rows: Optional[List[Any]] = None,
) -> List[str]:
    """Warn when persisted checklist rows fall short of the morning snapshot lock."""
    snap = snapshot_lock_counts(db, session_date)
    cl = {
        r.direction: int(r.n)
        for r in db.execute(
            text(
                """
                SELECT direction, COUNT(*)::int AS n
                FROM daily_checklist
                WHERE session_date = :d
                GROUP BY direction
                """
            ),
            {"d": session_date},
        ).fetchall()
    }
    warnings: List[str] = []
    for snap_dir, cl_dir in (("BULL", "LONG"), ("BEAR", "SHORT")):
        snap_n = snap.get(snap_dir, 0)
        cl_n = cl.get(cl_dir, 0)
        if snap_n >= 5 and cl_n < snap_n:
            warnings.append(
                f"{cl_dir} checklist has {cl_n} rows but morning snapshot locked {snap_n} ({snap_dir})"
            )
    if rs_rows is not None:
        rs_bull = sum(1 for r in rs_rows if (getattr(r, "ranking_type", None) or "").upper() != "BEARISH")
        rs_bear = sum(1 for r in rs_rows if (getattr(r, "ranking_type", None) or "").upper() == "BEARISH")
        if rs_bull >= 5 and snap.get("BULL", 0) < 5:
            warnings.append(
                f"morning snapshot locked only {snap.get('BULL', 0)} BULL names despite {rs_bull} in RS top-5"
            )
        if rs_bear >= 5 and snap.get("BEAR", 0) < 5:
            warnings.append(
                f"morning snapshot locked only {snap.get('BEAR', 0)} BEAR names despite {rs_bear} in RS top-5"
            )
    for msg in warnings:
        logger.warning("daily_checklist lock coverage mismatch: %s", msg)
    return warnings


def lock_morning_snapshot(
    db,
    session_date: str,
    bull_rows: List[Any],
    bear_rows: List[Any],
    *,
    locked_by: str = "auto",
) -> int:
    """Persist Top-5 bull/bear into daily_snapshot and write snapshot_lock. Returns count locked."""
    now = datetime.now(IST)
    count = 0
    for rank, row in enumerate(bull_rows[:5], start=1):
        sym = getattr(row, "symbol", None) or row.get("symbol")
        rs = getattr(row, "relative_strength", None)
        if rs is None and isinstance(row, dict):
            rs = row.get("relative_strength")
        db.execute(
            text(
                """
                INSERT INTO daily_snapshot
                    (snapshot_date, symbol, direction, rank, rs_score, locked_at)
                VALUES (:d, :sym, 'BULL', :rank, :rs, :now)
                ON CONFLICT (snapshot_date, symbol, direction) DO NOTHING
                """
            ),
            {"d": session_date, "sym": sym, "rank": rank, "rs": rs, "now": now},
        )
        count += 1
    for rank, row in enumerate(bear_rows[:5], start=1):
        sym = getattr(row, "symbol", None) or row.get("symbol")
        rs = getattr(row, "relative_strength", None)
        if rs is None and isinstance(row, dict):
            rs = row.get("relative_strength")
        db.execute(
            text(
                """
                INSERT INTO daily_snapshot
                    (snapshot_date, symbol, direction, rank, rs_score, locked_at)
                VALUES (:d, :sym, 'BEAR', :rank, :rs, :now)
                ON CONFLICT (snapshot_date, symbol, direction) DO NOTHING
                """
            ),
            {"d": session_date, "sym": sym, "rank": rank, "rs": rs, "now": now},
        )
        count += 1
    db.execute(
        text(
            """
            INSERT INTO snapshot_lock (lock_date, locked_at, locked_by)
            VALUES (:d, :now, :by)
            ON CONFLICT (lock_date) DO UPDATE SET
                locked_at = EXCLUDED.locked_at,
                locked_by = EXCLUDED.locked_by
            """
        ),
        {"d": session_date, "now": now, "by": locked_by},
    )
    logger.info(
        "daily_checklist: morning snapshot locked for %s (%d symbols, by=%s)",
        session_date,
        count,
        locked_by,
    )
    return count


def clear_snapshot_for_date(db, session_date: str) -> None:
    db.execute(text("DELETE FROM daily_snapshot WHERE snapshot_date = :d"), {"d": session_date})
    db.execute(text("DELETE FROM snapshot_lock WHERE lock_date = :d"), {"d": session_date})


def _snap_side_from_ranking(ranking_type: Optional[str]) -> str:
    return "BEAR" if (ranking_type or "").upper() == "BEARISH" else "BULL"


def _top5_by_scan(db, session_date: str, scan_time) -> Dict[Tuple[str, str], Dict[str, Any]]:
    """Map (symbol, BULL|BEAR) → {rank, rs_score} for one RS scan."""
    rows = db.execute(
        text(
            """
            SELECT UPPER(symbol) AS symbol, ranking_type, rank_position, relative_strength
            FROM relative_strength_snapshot
            WHERE scan_time = :st
              AND scan_time::date = CAST(:d AS date)
              AND rank_position IS NOT NULL
              AND rank_position <= 5
            """
        ),
        {"st": scan_time, "d": session_date},
    ).fetchall()
    out: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for r in rows:
        sym = (r.symbol or "").strip().upper()
        if not sym:
            continue
        side = _snap_side_from_ranking(r.ranking_type)
        out[(sym, side)] = {
            "rank": int(r.rank_position) if r.rank_position is not None else 99,
            "rs_score": r.relative_strength,
        }
    return out


def _eligible_consecutive_top5(
    db,
    session_date: str,
    *,
    now: Optional[datetime] = None,
) -> Dict[Tuple[str, str], Dict[str, Any]]:
    """Symbols on Top-5 for 2 consecutive RS scans (same side), through 14:30 / now."""
    now = now or datetime.now(IST)
    if now.tzinfo is None:
        now = IST.localize(now)
    else:
        now = now.astimezone(IST)

    scan_rows = db.execute(
        text(
            """
            SELECT DISTINCT scan_time
            FROM relative_strength_snapshot
            WHERE scan_time::date = CAST(:d AS date)
              AND scan_time <= :now
            ORDER BY scan_time
            """
        ),
        {"d": session_date, "now": now},
    ).fetchall()

    times = []
    for r in scan_rows:
        st = r.scan_time
        if st is None:
            continue
        t = st.astimezone(IST) if getattr(st, "tzinfo", None) else IST.localize(st)
        if (t.hour * 60 + t.minute) > PROMOTION_CUTOFF_MIN:
            continue
        times.append(st)

    if len(times) < PROMOTION_SCANS_REQUIRED:
        return {}

    eligible: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for i in range(len(times) - 1):
        a = _top5_by_scan(db, session_date, times[i])
        b = _top5_by_scan(db, session_date, times[i + 1])
        for key in set(a) & set(b):
            # Prefer later scan's rank/rs (times[i+1])
            meta = dict(b[key])
            meta["qualified_at"] = times[i + 1]
            eligible[key] = meta
    return eligible


def _upsert_snapshot_row(
    db,
    session_date: str,
    symbol: str,
    direction: str,
    rank: int,
    rs_score: Any,
    locked_at: datetime,
) -> None:
    db.execute(
        text(
            """
            INSERT INTO daily_snapshot
                (snapshot_date, symbol, direction, rank, rs_score, locked_at)
            VALUES (:d, :sym, :dir, :rank, :rs, :now)
            ON CONFLICT (snapshot_date, symbol, direction) DO UPDATE SET
                rank = EXCLUDED.rank,
                rs_score = EXCLUDED.rs_score,
                locked_at = EXCLUDED.locked_at
            """
        ),
        {
            "d": session_date,
            "sym": symbol,
            "dir": direction,
            "rank": rank,
            "rs": rs_score,
            "now": locked_at,
        },
    )


def promote_intraday_from_rs(
    db,
    session_date: str,
    *,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Promote Top-5 RS names (either side) into daily_snapshot after 2 consecutive scans.

    Morning lock remains the initial set. This adds late/same-side and direction-flip
    promotions through 14:30 IST so checklist / Fast Watch / GO Board can see them.
    Does not loosen the 2-scan threshold — only removes the 09:25-only membership freeze.
    """
    now = now or datetime.now(IST)
    if now.tzinfo is None:
        now = IST.localize(now)
    else:
        now = now.astimezone(IST)

    if not is_snapshot_locked(db, session_date):
        return {"promoted": [], "flipped": [], "updated": [], "reason": "not_locked"}
    if (now.hour * 60 + now.minute) > PROMOTION_CUTOFF_MIN:
        return {"promoted": [], "flipped": [], "updated": [], "reason": "past_cutoff"}

    eligible = _eligible_consecutive_top5(db, session_date, now=now)
    if not eligible:
        return {"promoted": [], "flipped": [], "updated": [], "reason": "no_eligible"}

    existing_rows = get_locked_symbol_rows(db, session_date)
    # One active direction per symbol (prefer existing row if somehow duplicated)
    by_sym: Dict[str, Any] = {}
    for r in existing_rows:
        by_sym[str(r.symbol).upper()] = r

    promoted: List[Dict[str, Any]] = []
    flipped: List[Dict[str, Any]] = []
    updated: List[Dict[str, Any]] = []

    for (sym, side), meta in sorted(
        eligible.items(),
        key=lambda kv: (
            kv[1].get("qualified_at") or now,
            kv[1].get("rank") or 99,
            kv[0][0],
            kv[0][1],
        ),
    ):
        rank = int(meta.get("rank") or 99)
        rs = meta.get("rs_score")
        locked_at = meta.get("qualified_at") or now
        cur = by_sym.get(sym)

        if cur is None:
            _upsert_snapshot_row(db, session_date, sym, side, rank, rs, locked_at)
            by_sym[sym] = SimpleNamespace(symbol=sym, direction=side, rank=rank, rs_score=rs)
            promoted.append({"symbol": sym, "direction": side, "rank": rank})
            continue

        cur_dir = (cur.direction or "").upper()
        if cur_dir == side:
            # Already locked same side — refresh rank/score only
            _upsert_snapshot_row(db, session_date, sym, side, rank, rs, locked_at)
            updated.append({"symbol": sym, "direction": side, "rank": rank})
            continue

        # Direction flip: drop old side, write new side (one direction per symbol)
        db.execute(
            text(
                """
                DELETE FROM daily_snapshot
                WHERE snapshot_date = CAST(:d AS date)
                  AND UPPER(symbol) = :sym
                  AND direction = :old
                """
            ),
            {"d": session_date, "sym": sym, "old": cur_dir},
        )
        _upsert_snapshot_row(db, session_date, sym, side, rank, rs, locked_at)
        by_sym[sym] = SimpleNamespace(symbol=sym, direction=side, rank=rank, rs_score=rs)
        flipped.append(
            {
                "symbol": sym,
                "from_direction": cur_dir,
                "to_direction": side,
                "rank": rank,
            }
        )

    if promoted or flipped:
        logger.info(
            "daily_checklist: intraday promote %s promoted=%s flipped=%s",
            session_date,
            [p["symbol"] for p in promoted],
            [f["symbol"] for f in flipped],
        )
    return {
        "promoted": promoted,
        "flipped": flipped,
        "updated": updated,
        "reason": "ok",
    }


def sort_by_snapshot_rank(stocks: List[Dict[str, Any]], rank_map: Dict[str, Tuple[int, int]]) -> List[Dict[str, Any]]:
    """Order stocks by (direction bucket, rank) from daily_snapshot."""

    def key(s: Dict[str, Any]) -> Tuple[int, int, str]:
        sym = s.get("symbol") or ""
        if sym in rank_map:
            return (*rank_map[sym], sym)
        d = 0 if (s.get("direction") or "LONG") == "LONG" else 1
        return (d, 99, sym)

    return sorted(stocks, key=key)

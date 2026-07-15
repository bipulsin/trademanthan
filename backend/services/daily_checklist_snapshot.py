"""Morning snapshot lock + intraday promote/remove for Daily RS Checklist.

Entry: morning Top-5+5 at/after 09:25, plus intraday promote after 2 consecutive
Top-5 RS scans (either side) through 14:30.

Removal (harder than entry — hysteresis):
  R1 — last N=8 confirmed 10m closes all opposite session VWAP (same N as Layer 3)
  R2 — RS rank outside configurable band (default Top-10) for M consecutive scans
        (default M=3) in the lock direction

Direction flips are never a swap: old side must fail R1/R2, new side must clear
entry independently. Persistence score is display/ordering only.
"""
from __future__ import annotations

import logging
import os
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
ENTRY_TOP_N = 5

# Removal hysteresis (configurable — harder bar than entry).
REMOVAL_RANK_BAND = max(ENTRY_TOP_N, int(os.getenv("RS_LOCK_REMOVAL_RANK_BAND", "10")))
REMOVAL_RANK_SCANS = max(1, int(os.getenv("RS_LOCK_REMOVAL_RANK_SCANS", "3")))


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
            SELECT symbol, direction, rank, rs_score, locked_at
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
    """Lock direction per symbol (LONG / SHORT from daily_snapshot)."""
    return {
        r.symbol: "LONG" if (r.direction or "").upper() == "BULL" else "SHORT"
        for r in get_locked_symbol_rows(db, session_date)
    }


def snapshot_lock_counts(db, session_date: str) -> Dict[str, int]:
    """Per-side counts from daily_snapshot (BULL / BEAR)."""
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


def _log_membership(
    db,
    session_date: str,
    *,
    symbol: str,
    direction: str,
    event_type: str,
    rule: str,
    rank: Optional[int] = None,
    detail: Optional[Dict[str, Any]] = None,
    event_at: Optional[datetime] = None,
    persistence_top5_frac: Optional[float] = None,
    persistence_clean_bars: Optional[int] = None,
) -> None:
    import json

    try:
        db.execute(text("SAVEPOINT rs_lock_audit_sp"))
        db.execute(
            text(
                """
                INSERT INTO rs_lock_membership_audit
                    (session_date, symbol, direction, event_type, rule, rank,
                     persistence_top5_frac, persistence_clean_bars, detail, event_at)
                VALUES
                    (CAST(:d AS date), :sym, :dir, :etype, :rule, :rank,
                     :pfrac, :pclean, CAST(:detail AS jsonb), :eat)
                """
            ),
            {
                "d": session_date,
                "sym": symbol,
                "dir": direction,
                "etype": event_type,
                "rule": rule,
                "rank": rank,
                "pfrac": persistence_top5_frac,
                "pclean": persistence_clean_bars,
                "detail": json.dumps(detail or {}),
                "eat": event_at or datetime.now(IST),
            },
        )
        db.execute(text("RELEASE SAVEPOINT rs_lock_audit_sp"))
    except Exception as exc:
        try:
            db.execute(text("ROLLBACK TO SAVEPOINT rs_lock_audit_sp"))
        except Exception:
            pass
        logger.debug("rs_lock_membership_audit write skipped: %s", exc)


def lock_morning_snapshot(
    db,
    session_date: str,
    bull_rows: List[Any],
    bear_rows: List[Any],
    *,
    locked_by: str = "auto",
    now: Optional[datetime] = None,
) -> int:
    """Persist Top-5 bull/bear into daily_snapshot and write snapshot_lock. Returns count locked."""
    now = now or datetime.now(IST)
    if now.tzinfo is None:
        now = IST.localize(now)
    else:
        now = now.astimezone(IST)
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
        _log_membership(
            db,
            session_date,
            symbol=str(sym).upper(),
            direction="BULL",
            event_type="entry",
            rule="morning_lock",
            rank=rank,
            detail={"locked_by": locked_by},
            event_at=now,
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
        _log_membership(
            db,
            session_date,
            symbol=str(sym).upper(),
            direction="BEAR",
            event_type="entry",
            rule="morning_lock",
            rank=rank,
            detail={"locked_by": locked_by},
            event_at=now,
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


def _checklist_dir(side: str) -> str:
    return "SHORT" if (side or "").upper() == "BEAR" else "LONG"


def _scan_times_through(db, session_date: str, now: datetime) -> List[Any]:
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
    return times


def _ranks_by_scan(
    db, session_date: str, scan_time, *, max_rank: int
) -> Dict[Tuple[str, str], Dict[str, Any]]:
    """Map (symbol, BULL|BEAR) → {rank, rs_score} for one RS scan up to max_rank."""
    rows = db.execute(
        text(
            """
            SELECT UPPER(symbol) AS symbol, ranking_type, rank_position, relative_strength
            FROM relative_strength_snapshot
            WHERE scan_time = :st
              AND scan_time::date = CAST(:d AS date)
              AND rank_position IS NOT NULL
              AND rank_position <= :mx
            """
        ),
        {"st": scan_time, "d": session_date, "mx": max_rank},
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


def _top5_by_scan(db, session_date: str, scan_time) -> Dict[Tuple[str, str], Dict[str, Any]]:
    return _ranks_by_scan(db, session_date, scan_time, max_rank=ENTRY_TOP_N)


def _eligible_consecutive_top5(
    db,
    session_date: str,
    *,
    now: Optional[datetime] = None,
) -> Dict[Tuple[str, str], Dict[str, Any]]:
    """Symbols on Top-5 for the *latest* 2 consecutive RS scans (same side).

    Only the most recent scan pair qualifies — using any historical pair would
    re-promote a name forever after an early morning consecutive hit, defeating
    R1/R2 removal hysteresis.
    """
    now = now or datetime.now(IST)
    if now.tzinfo is None:
        now = IST.localize(now)
    else:
        now = now.astimezone(IST)

    times = _scan_times_through(db, session_date, now)
    if len(times) < PROMOTION_SCANS_REQUIRED:
        return {}

    a = _top5_by_scan(db, session_date, times[-2])
    b = _top5_by_scan(db, session_date, times[-1])
    eligible: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for key in set(a) & set(b):
        meta = dict(b[key])
        meta["qualified_at"] = times[-1]
        eligible[key] = meta
    return eligible


def _r2_rank_gone(
    db,
    session_date: str,
    symbol: str,
    side: str,
    *,
    now: datetime,
    band: int = REMOVAL_RANK_BAND,
    scans: int = REMOVAL_RANK_SCANS,
) -> bool:
    """True when symbol is outside Top-``band`` in lock direction for ``scans`` consecutive scans."""
    times = _scan_times_through(db, session_date, now)
    if len(times) < scans:
        return False
    recent = times[-scans:]
    for st in recent:
        ranks = _ranks_by_scan(db, session_date, st, max_rank=band)
        if ranks.get((symbol.upper(), side.upper())) is not None:
            return False
    return True


def _load_candles_for_symbol(db, symbol: str) -> Optional[List[Dict[str, Any]]]:
    try:
        from backend.services.rs_conviction_candles import candles_cache_only, load_instrument_atr_maps

        ikey_map, _ = load_instrument_atr_maps(db, {symbol})
        ikey = ikey_map.get(symbol)
        if not ikey:
            return None
        candles = candles_cache_only(ikey)
        if candles:
            return candles
        from backend.config import settings
        from backend.services.relative_strength_scanner import (
            CANDLE_DAYS_BACK,
            CANDLE_INTERVAL,
            MIN_BARS,
            _sorted_candles,
        )
        from backend.services.upstox_service import UpstoxService

        raw = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET).get_historical_candles_by_instrument_key(
            ikey, interval=CANDLE_INTERVAL, days_back=CANDLE_DAYS_BACK
        )
        if raw and len(raw) >= MIN_BARS:
            return _sorted_candles(raw)
    except Exception as exc:
        logger.debug("lock R1 candle load skipped for %s: %s", symbol, exc)
    return None


def _r1_vwap_trend_broken(db, symbol: str, side: str, *, now: datetime) -> Optional[bool]:
    candles = _load_candles_for_symbol(db, symbol)
    if not candles:
        return None
    try:
        from backend.services.kavach_10m import lock_vwap_trend_broken_10m

        return lock_vwap_trend_broken_10m(
            candles,
            lock_direction=_checklist_dir(side),
            now=now,
        )
    except Exception as exc:
        logger.debug("lock R1 evaluate skipped for %s: %s", symbol, exc)
        return None


def _upsert_snapshot_row(
    db,
    session_date: str,
    symbol: str,
    direction: str,
    rank: int,
    rs_score: Any,
    locked_at: datetime,
    *,
    refresh_locked_at: bool = False,
) -> None:
    if refresh_locked_at:
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
    else:
        db.execute(
            text(
                """
                INSERT INTO daily_snapshot
                    (snapshot_date, symbol, direction, rank, rs_score, locked_at)
                VALUES (:d, :sym, :dir, :rank, :rs, :now)
                ON CONFLICT (snapshot_date, symbol, direction) DO UPDATE SET
                    rank = EXCLUDED.rank,
                    rs_score = EXCLUDED.rs_score
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


def _delete_snapshot_row(db, session_date: str, symbol: str, direction: str) -> None:
    db.execute(
        text(
            """
            DELETE FROM daily_snapshot
            WHERE snapshot_date = CAST(:d AS date)
              AND UPPER(symbol) = :sym
              AND direction = :dir
            """
        ),
        {"d": session_date, "sym": symbol.upper(), "dir": direction},
    )


def compute_persistence(
    db,
    session_date: str,
    symbol: str,
    side: str,
    promoted_at: Optional[datetime],
    *,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Display-only persistence: Top-5 fraction since promote + clean VWAP bar count."""
    now = now or datetime.now(IST)
    if now.tzinfo is None:
        now = IST.localize(now)
    else:
        now = now.astimezone(IST)

    top5_scans = 0
    total_scans = 0
    times = _scan_times_through(db, session_date, now)
    for st in times:
        t = st.astimezone(IST) if getattr(st, "tzinfo", None) else IST.localize(st)
        if promoted_at is not None:
            pa = (
                promoted_at.astimezone(IST)
                if getattr(promoted_at, "tzinfo", None)
                else IST.localize(promoted_at)
            )
            if t < pa:
                continue
        total_scans += 1
        ranks = _top5_by_scan(db, session_date, st)
        if (symbol.upper(), side.upper()) in ranks:
            top5_scans += 1

    frac = round(top5_scans / total_scans, 3) if total_scans else None
    clean_bars: Optional[int] = None
    candles = _load_candles_for_symbol(db, symbol)
    if candles:
        try:
            from backend.services.kavach_10m import _10m_series_upto, last_closed_10m_pair_end_idx
            from backend.services.kavach_volume import _f
            from backend.services.relative_strength_scanner import (
                _current_and_prev_day_close,
                _sorted_candles,
            )
            from backend.services.vajra.indicators import cumulative_vwap

            candles = _sorted_candles(candles)
            split = _current_and_prev_day_close(candles)
            if split:
                _, _, first_today = split
                pair_end = last_closed_10m_pair_end_idx(candles, now=now)
                bars = _10m_series_upto(candles, pair_end)
                is_long = side.upper() == "BULL"
                clean = 0
                for b in reversed(bars):
                    end_idx = int(b["end_5m_idx"])
                    t_highs = [_f(c.get("high")) for c in candles[first_today : end_idx + 1]]
                    t_lows = [_f(c.get("low")) for c in candles[first_today : end_idx + 1]]
                    t_closes = [_f(c.get("close")) for c in candles[first_today : end_idx + 1]]
                    t_vols = [_f(c.get("volume")) for c in candles[first_today : end_idx + 1]]
                    if not t_closes:
                        break
                    v = cumulative_vwap(t_highs, t_lows, t_closes, t_vols)[-1]
                    c = float(b["close"])
                    on_side = (c > v) if is_long else (c < v)
                    if not on_side:
                        break
                    clean += 1
                clean_bars = clean
        except Exception as exc:
            logger.debug("persistence clean_bars skipped for %s: %s", symbol, exc)

    return {
        "top5_fraction": frac,
        "top5_scans": top5_scans,
        "scans_since_promote": total_scans,
        "clean_vwap_bars": clean_bars,
    }


def apply_lock_removals(
    db,
    session_date: str,
    *,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Remove locked symbols that fail R1 or R2. Does not auto-promote opposite side."""
    now = now or datetime.now(IST)
    if now.tzinfo is None:
        now = IST.localize(now)
    else:
        now = now.astimezone(IST)

    removed: List[Dict[str, Any]] = []
    for row in list(get_locked_symbol_rows(db, session_date)):
        sym = str(row.symbol).upper()
        side = (row.direction or "").upper()
        if side not in ("BULL", "BEAR"):
            continue

        rule = None
        detail: Dict[str, Any] = {}
        r1 = _r1_vwap_trend_broken(db, sym, side, now=now)
        if r1 is True:
            rule = "R1"
            detail = {"reason": "vwap_opposite_consecutive", "n_bars": 8}
        elif _r2_rank_gone(db, session_date, sym, side, now=now):
            rule = "R2"
            detail = {
                "reason": "rank_outside_band",
                "band": REMOVAL_RANK_BAND,
                "scans": REMOVAL_RANK_SCANS,
            }

        if not rule:
            continue

        pers = compute_persistence(
            db, session_date, sym, side, getattr(row, "locked_at", None), now=now
        )
        _delete_snapshot_row(db, session_date, sym, side)
        _log_membership(
            db,
            session_date,
            symbol=sym,
            direction=side,
            event_type="remove",
            rule=rule,
            rank=int(row.rank) if row.rank is not None else None,
            detail=detail,
            event_at=now,
            persistence_top5_frac=pers.get("top5_fraction"),
            persistence_clean_bars=pers.get("clean_vwap_bars"),
        )
        try:
            from backend.services.kavach_open_trades import (
                mark_open_trades_exit_on_lock_removal,
            )

            mark_open_trades_exit_on_lock_removal(
                db, session_date, sym, rule, removed_at=now
            )
        except Exception as exc:
            logger.warning(
                "open_trades EXIT_NOW on lock removal failed %s %s: %s",
                sym,
                rule,
                exc,
            )
        removed.append({"symbol": sym, "direction": side, "rule": rule})

    if removed:
        logger.info(
            "daily_checklist: lock removals %s %s",
            session_date,
            [(r["symbol"], r["rule"]) for r in removed],
        )
    return {"removed": removed}


def promote_intraday_from_rs(
    db,
    session_date: str,
    *,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Sync lock membership: removals (R1/R2) then independent Top-5 entries (2-scan).

    No direction swap — opposite-side entry only after the symbol is unlocked
    (or never locked) and clears the normal entry gate on that side.
    """
    now = now or datetime.now(IST)
    if now.tzinfo is None:
        now = IST.localize(now)
    else:
        now = now.astimezone(IST)

    empty = {
        "promoted": [],
        "removed": [],
        "updated": [],
        "flipped": [],
    }
    if not is_snapshot_locked(db, session_date):
        return {**empty, "reason": "not_locked"}
    if (now.hour * 60 + now.minute) > PROMOTION_CUTOFF_MIN:
        return {**empty, "reason": "past_cutoff"}

    removal = apply_lock_removals(db, session_date, now=now)
    removed = removal.get("removed") or []

    eligible = _eligible_consecutive_top5(db, session_date, now=now)
    existing_rows = get_locked_symbol_rows(db, session_date)
    by_sym: Dict[str, Any] = {str(r.symbol).upper(): r for r in existing_rows}

    promoted: List[Dict[str, Any]] = []
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
            _upsert_snapshot_row(
                db, session_date, sym, side, rank, rs, locked_at, refresh_locked_at=True
            )
            by_sym[sym] = SimpleNamespace(
                symbol=sym, direction=side, rank=rank, rs_score=rs, locked_at=locked_at
            )
            _log_membership(
                db,
                session_date,
                symbol=sym,
                direction=side,
                event_type="entry",
                rule="intraday_2scan",
                rank=rank,
                detail={"qualified_at": str(locked_at)},
                event_at=now,
            )
            promoted.append({"symbol": sym, "direction": side, "rank": rank})
            continue

        cur_dir = (cur.direction or "").upper()
        if cur_dir == side:
            _upsert_snapshot_row(
                db, session_date, sym, side, rank, rs, locked_at, refresh_locked_at=False
            )
            updated.append({"symbol": sym, "direction": side, "rank": rank})
            continue

        # Opposite side eligible but still locked the other way — do NOT swap.
        logger.debug(
            "daily_checklist: skip swap %s locked=%s eligible=%s (need removal first)",
            sym,
            cur_dir,
            side,
        )

    if promoted or removed:
        logger.info(
            "daily_checklist: lock sync %s promoted=%s removed=%s",
            session_date,
            [p["symbol"] for p in promoted],
            [r["symbol"] for r in removed],
        )
    return {
        "promoted": promoted,
        "removed": removed,
        "updated": updated,
        "flipped": [],
        "reason": "ok",
        "config": {
            "removal_rank_band": REMOVAL_RANK_BAND,
            "removal_rank_scans": REMOVAL_RANK_SCANS,
            "entry_scans": PROMOTION_SCANS_REQUIRED,
            "entry_top_n": ENTRY_TOP_N,
        },
    }


def persistence_map_for_session(
    db, session_date: str, *, now: Optional[datetime] = None
) -> Dict[str, Dict[str, Any]]:
    """Per-symbol persistence for checklist ordering/display."""
    out: Dict[str, Dict[str, Any]] = {}
    for row in get_locked_symbol_rows(db, session_date):
        sym = str(row.symbol).upper()
        side = (row.direction or "").upper()
        out[sym] = compute_persistence(
            db, session_date, sym, side, getattr(row, "locked_at", None), now=now
        )
        out[sym]["direction"] = side
        out[sym]["locked_at"] = (
            row.locked_at.isoformat() if getattr(row, "locked_at", None) else None
        )
    return out


def sort_by_snapshot_rank(
    stocks: List[Dict[str, Any]], rank_map: Dict[str, Tuple[int, int]]
) -> List[Dict[str, Any]]:
    """Order stocks by (direction bucket, rank) from daily_snapshot."""

    def key(s: Dict[str, Any]) -> Tuple[int, int, str]:
        sym = s.get("symbol") or ""
        if sym in rank_map:
            return (*rank_map[sym], sym)
        d = 0 if (s.get("direction") or "LONG") == "LONG" else 1
        return (d, 99, sym)

    return sorted(stocks, key=key)


def sort_by_persistence(
    stocks: List[Dict[str, Any]],
    persistence: Dict[str, Dict[str, Any]],
    rank_map: Dict[str, Tuple[int, int]],
) -> List[Dict[str, Any]]:
    """Most-persistent first within each direction; then snapshot rank."""

    def key(s: Dict[str, Any]) -> Tuple[int, float, int, str]:
        sym = s.get("symbol") or ""
        d = 0 if (s.get("direction") or "LONG") == "LONG" else 1
        pers = persistence.get(sym) or {}
        frac = pers.get("top5_fraction")
        frac_key = -(frac if frac is not None else -1.0)
        rank = rank_map.get(sym, (d, 99))[1]
        return (d, frac_key, rank, sym)

    return sorted(stocks, key=key)

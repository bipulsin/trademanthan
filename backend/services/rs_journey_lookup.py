"""Read-only per-symbol RS journey tracer (eligibility → ranks → lock).

Merges existing tables with the prospective ``rs_scan_exclusion_log``.
Does not alter ranking, lock, or checklist logic.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import pytz
from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.relative_strength_scanner import PERSIST_TOP_N, TOP_N
from backend.services.rs_confidence_divergence_lookup import _resolve_symbol
from backend.services.rs_exclusion_audit import (
    ensure_rs_scan_exclusion_log,
    fetch_exclusions_for_symbol,
)

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")
MIN_BARS = 40

REASON_LABELS = {
    "missing_key": "Missing instrument key / symbol",
    "missing_candles_or_min_bars": f"Missing candles or fewer than {MIN_BARS} bars",
    "no_prev_close": "No previous-day close available",
    "no_closed_bar": "No closed bar available",
    "exception": "Per-symbol compute exception",
    "neutral_kavach": "Kavach state NEUTRAL (not ranked bullish/bearish)",
    "beyond_persist_top_n": f"Ranked beyond persisted Top-{PERSIST_TOP_N}",
    "not_in_universe": "Not in arbitrage_master curr-mth future universe",
    "absent_from_top10_snapshot": (
        f"Absent from persisted Top-{PERSIST_TOP_N} snapshot "
        "(pre-exclusion-log era or unknown)"
    ),
}


def _iso(v: Any) -> Optional[str]:
    if v is None:
        return None
    if isinstance(v, datetime):
        try:
            return v.isoformat()
        except Exception:
            return str(v)
    return str(v)


def _f(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _ist_hm(ts: Any) -> Optional[str]:
    if not isinstance(ts, datetime):
        return None
    t = ts.astimezone(IST) if ts.tzinfo else IST.localize(ts)
    return t.strftime("%H:%M:%S")


def _pass_fail(
    *,
    status: str,
    rank: Optional[int],
    would_be_rank: Optional[int],
    top_n: int = TOP_N,
    persist_n: int = PERSIST_TOP_N,
) -> Dict[str, Any]:
    r = rank if rank is not None else would_be_rank
    if status == "persisted" and rank is not None:
        return {
            "pass_top_n": rank <= top_n,
            "pass_persist": rank <= persist_n,
            "rank_used": rank,
        }
    if status in ("excluded", "beyond_persist") and would_be_rank is not None:
        return {
            "pass_top_n": False,
            "pass_persist": False,
            "rank_used": would_be_rank,
        }
    if status in ("metrics_fail", "neutral", "not_in_universe"):
        return {"pass_top_n": False, "pass_persist": False, "rank_used": None}
    return {"pass_top_n": None, "pass_persist": None, "rank_used": r}


def lookup_rs_journey(symbol: str, session_date: str) -> Dict[str, Any]:
    """Full chronological RS journey for one symbol-day."""
    db = SessionLocal()
    try:
        resolved = _resolve_symbol(db, symbol)
        sym = resolved.get("resolved")
        if not sym:
            return {
                "ok": False,
                "error": "symbol_not_found",
                "symbol_resolution": resolved,
                "date": session_date,
            }

        master = db.execute(
            text(
                """
                SELECT stock, currmth_future_symbol, currmth_future_instrument_key
                FROM arbitrage_master
                WHERE UPPER(TRIM(stock)) = :s
                LIMIT 1
                """
            ),
            {"s": sym},
        ).fetchone()
        in_universe = bool(
            master
            and master.currmth_future_instrument_key
            and str(master.currmth_future_instrument_key).strip()
        )
        universe = {
            "in_universe": in_universe,
            "stock": master.stock if master else None,
            "currmth_future_symbol": master.currmth_future_symbol if master else None,
            "currmth_future_instrument_key": (
                master.currmth_future_instrument_key if master else None
            ),
            "exclusion_reason": None if in_universe else "not_in_universe",
            "exclusion_label": None
            if in_universe
            else REASON_LABELS["not_in_universe"],
        }

        ensure_rs_scan_exclusion_log()
        scan_times = db.execute(
            text(
                """
                SELECT scan_time FROM (
                    SELECT DISTINCT scan_time
                    FROM relative_strength_snapshot
                    WHERE (scan_time AT TIME ZONE 'Asia/Kolkata')::date = CAST(:d AS date)
                    UNION
                    SELECT DISTINCT scan_time
                    FROM rs_scan_exclusion_log
                    WHERE session_date = CAST(:d AS date)
                ) t
                ORDER BY scan_time
                """
            ),
            {"d": session_date},
        ).fetchall()

        excl_by_scan: Dict[datetime, Dict[str, Any]] = {}
        try:
            for e in fetch_exclusions_for_symbol(
                db, session_date=session_date, symbol=sym
            ):
                st = e.get("scan_time")
                if isinstance(st, datetime):
                    excl_by_scan[st] = e
        except Exception as exc:
            logger.debug("exclusion fetch skipped: %s", exc)

        snap_rows = db.execute(
            text(
                """
                SELECT scan_time, symbol, relative_strength, trade_score, confidence_grade,
                       ranking_type, rank_position, kavach_state, volume_ratio, volume_label,
                       current_price
                FROM relative_strength_snapshot
                WHERE (scan_time AT TIME ZONE 'Asia/Kolkata')::date = CAST(:d AS date)
                  AND UPPER(TRIM(symbol)) = :s
                ORDER BY scan_time
                """
            ),
            {"d": session_date, "s": sym},
        ).fetchall()
        snap_by_scan = {r.scan_time: dict(r._mapping) for r in snap_rows}

        cutoff_rows = db.execute(
            text(
                """
                SELECT scan_time, ranking_type, rank_position, symbol, relative_strength
                FROM relative_strength_snapshot
                WHERE (scan_time AT TIME ZONE 'Asia/Kolkata')::date = CAST(:d AS date)
                  AND rank_position IN (:top_n, :persist_n)
                ORDER BY scan_time, ranking_type, rank_position
                """
            ),
            {"d": session_date, "top_n": TOP_N, "persist_n": PERSIST_TOP_N},
        ).fetchall()
        cutoffs: Dict[datetime, Dict[str, Any]] = {}
        for r in cutoff_rows:
            bucket = cutoffs.setdefault(r.scan_time, {})
            side = r.ranking_type
            side_bucket = bucket.setdefault(side, {})
            side_bucket[int(r.rank_position)] = {
                "symbol": r.symbol,
                "relative_strength": _f(r.relative_strength),
            }

        has_exclusion_coverage = bool(excl_by_scan)
        checkpoints: List[Dict[str, Any]] = []

        if not in_universe:
            checkpoints.append(
                {
                    "scan_time": None,
                    "scan_time_ist": None,
                    "status": "not_in_universe",
                    "exclusion_reason": "not_in_universe",
                    "exclusion_label": REASON_LABELS["not_in_universe"],
                    "ranking_side": None,
                    "rank": None,
                    "would_be_rank": None,
                    "relative_strength": None,
                    "trade_score": None,
                    "confidence_grade": None,
                    "kavach_state": None,
                    "current_price": None,
                    "volume_ratio": None,
                    "volume_label": None,
                    "rank_cutoff_persist": PERSIST_TOP_N,
                    "rank_cutoff_top_n": TOP_N,
                    "cutoff_rs_persist": None,
                    "cutoff_rs_top_n": None,
                    "cutoffs": {},
                    "pass_top_n": False,
                    "pass_persist": False,
                    "detail": None,
                    "scan_trigger": None,
                }
            )

        for st_row in scan_times:
            st = st_row.scan_time
            snap = snap_by_scan.get(st)
            excl = excl_by_scan.get(st)
            cutoff = cutoffs.get(st) or {}

            if snap:
                rank = (
                    int(snap["rank_position"])
                    if snap.get("rank_position") is not None
                    else None
                )
                side = snap.get("ranking_type")
                c_persist = None
                c_top = None
                if side and cutoff.get(side):
                    c_top = (cutoff[side].get(TOP_N) or {}).get("relative_strength")
                    c_persist = (cutoff[side].get(PERSIST_TOP_N) or {}).get(
                        "relative_strength"
                    )
                pf = _pass_fail(status="persisted", rank=rank, would_be_rank=None)
                checkpoints.append(
                    {
                        "scan_time": _iso(st),
                        "scan_time_ist": _ist_hm(st),
                        "status": "persisted",
                        "exclusion_reason": None,
                        "exclusion_label": f"In persisted Top-{PERSIST_TOP_N} snapshot",
                        "ranking_side": side,
                        "rank": rank,
                        "would_be_rank": None,
                        "relative_strength": _f(snap.get("relative_strength")),
                        "trade_score": _f(snap.get("trade_score")),
                        "confidence_grade": snap.get("confidence_grade"),
                        "kavach_state": snap.get("kavach_state"),
                        "current_price": _f(snap.get("current_price")),
                        "volume_ratio": _f(snap.get("volume_ratio")),
                        "volume_label": snap.get("volume_label"),
                        "rank_cutoff_persist": PERSIST_TOP_N,
                        "rank_cutoff_top_n": TOP_N,
                        "cutoff_rs_persist": c_persist,
                        "cutoff_rs_top_n": c_top,
                        "cutoffs": cutoff,
                        "pass_top_n": pf["pass_top_n"],
                        "pass_persist": pf["pass_persist"],
                        "detail": None,
                        "scan_trigger": None,
                    }
                )
                continue

            if excl:
                er = excl.get("exclusion_reason") or "unknown"
                if er == "beyond_persist_top_n":
                    status = "beyond_persist"
                elif er == "neutral_kavach":
                    status = "neutral"
                else:
                    status = "metrics_fail"
                would_be = excl.get("would_be_rank")
                side = excl.get("ranking_side")
                c_persist = _f(excl.get("cutoff_rs_persist"))
                c_top = _f(excl.get("cutoff_rs_top_n"))
                if side and cutoff.get(side):
                    c_top = (cutoff[side].get(TOP_N) or {}).get(
                        "relative_strength", c_top
                    )
                    c_persist = (cutoff[side].get(PERSIST_TOP_N) or {}).get(
                        "relative_strength", c_persist
                    )
                would_be_i = int(would_be) if would_be is not None else None
                pf = _pass_fail(
                    status=status, rank=None, would_be_rank=would_be_i
                )
                checkpoints.append(
                    {
                        "scan_time": _iso(st),
                        "scan_time_ist": _ist_hm(st),
                        "status": status,
                        "exclusion_reason": er,
                        "exclusion_label": REASON_LABELS.get(er, er),
                        "ranking_side": side,
                        "rank": None,
                        "would_be_rank": would_be_i,
                        "relative_strength": _f(excl.get("relative_strength")),
                        "trade_score": _f(excl.get("trade_score")),
                        "confidence_grade": excl.get("confidence_grade"),
                        "kavach_state": excl.get("kavach_state"),
                        "current_price": _f(excl.get("current_price")),
                        "volume_ratio": _f(excl.get("volume_ratio")),
                        "volume_label": excl.get("volume_label"),
                        "rank_cutoff_persist": excl.get("rank_cutoff") or PERSIST_TOP_N,
                        "rank_cutoff_top_n": excl.get("top_n_cutoff") or TOP_N,
                        "cutoff_rs_persist": c_persist,
                        "cutoff_rs_top_n": c_top,
                        "cutoffs": cutoff,
                        "pass_top_n": pf["pass_top_n"],
                        "pass_persist": pf["pass_persist"],
                        "detail": excl.get("detail"),
                        "scan_trigger": excl.get("scan_trigger"),
                    }
                )
                continue

            checkpoints.append(
                {
                    "scan_time": _iso(st),
                    "scan_time_ist": _ist_hm(st),
                    "status": "absent_unknown",
                    "exclusion_reason": "absent_from_top10_snapshot",
                    "exclusion_label": REASON_LABELS["absent_from_top10_snapshot"],
                    "ranking_side": None,
                    "rank": None,
                    "would_be_rank": None,
                    "relative_strength": None,
                    "trade_score": None,
                    "confidence_grade": None,
                    "kavach_state": None,
                    "current_price": None,
                    "volume_ratio": None,
                    "volume_label": None,
                    "rank_cutoff_persist": PERSIST_TOP_N,
                    "rank_cutoff_top_n": TOP_N,
                    "cutoff_rs_persist": None,
                    "cutoff_rs_top_n": None,
                    "cutoffs": cutoff,
                    "pass_top_n": False,
                    "pass_persist": False,
                    "detail": (
                        "No exclusion-log row for this scan. "
                        "Prospective exclusion logging may not have been active yet."
                        if not has_exclusion_coverage
                        else "Symbol not in snapshot or exclusion log for this scan."
                    ),
                    "scan_trigger": None,
                }
            )

        lock_rows = db.execute(
            text(
                """
                SELECT snapshot_date, symbol, direction, rank, rs_score, locked_at
                FROM daily_snapshot
                WHERE snapshot_date = CAST(:d AS date) AND UPPER(TRIM(symbol)) = :s
                """
            ),
            {"d": session_date, "s": sym},
        ).fetchall()
        lock_out = []
        for r in lock_rows:
            lock_out.append(
                {
                    "snapshot_date": str(r.snapshot_date) if r.snapshot_date else None,
                    "symbol": r.symbol,
                    "direction": r.direction,
                    "rank": r.rank,
                    "rs_score": _f(r.rs_score),
                    "locked_at": _iso(r.locked_at),
                    "locked_at_ist": _ist_hm(r.locked_at),
                }
            )

        audit_rows = db.execute(
            text(
                """
                SELECT event_at, event_type, rule, direction, rank, detail
                FROM rs_lock_membership_audit
                WHERE session_date = CAST(:d AS date) AND UPPER(TRIM(symbol)) = :s
                ORDER BY event_at
                """
            ),
            {"d": session_date, "s": sym},
        ).fetchall()
        lock_audit = [
            {
                "event_at": _iso(r.event_at),
                "event_at_ist": _ist_hm(r.event_at),
                "event_type": r.event_type,
                "rule": r.rule,
                "direction": r.direction,
                "rank": r.rank,
                "detail": r.detail,
            }
            for r in audit_rows
        ]

        try:
            anchor_rows = db.execute(
                text(
                    """
                    SELECT capture_label, capture_time, rank_position, direction,
                           relative_strength, trade_score, confidence_grade
                    FROM rs_anchor_snapshot
                    WHERE session_date = CAST(:d AS date) AND UPPER(TRIM(symbol)) = :s
                    ORDER BY capture_time
                    """
                ),
                {"d": session_date, "s": sym},
            ).fetchall()
            anchors = [
                {
                    "capture_label": r.capture_label,
                    "capture_time": _iso(r.capture_time),
                    "capture_time_ist": _ist_hm(r.capture_time),
                    "rank_position": r.rank_position,
                    "direction": r.direction,
                    "relative_strength": _f(r.relative_strength),
                    "trade_score": _f(r.trade_score),
                    "confidence_grade": r.confidence_grade,
                }
                for r in anchor_rows
            ]
        except Exception:
            anchors = []

        try:
            arch = db.execute(
                text(
                    """
                    SELECT symbol, relative_strength, trade_score, confidence_grade,
                           ranking_side, would_be_rank_bull, would_be_rank_bear, kavach_state
                    FROM rs_universe_kavach_archive
                    WHERE session_date = CAST(:d AS date) AND UPPER(TRIM(symbol)) = :s
                    LIMIT 1
                    """
                ),
                {"d": session_date, "s": sym},
            ).fetchone()
            eod_archive = dict(arch._mapping) if arch else None
            if eod_archive:
                for k in ("relative_strength", "trade_score"):
                    if eod_archive.get(k) is not None:
                        eod_archive[k] = _f(eod_archive[k])
        except Exception:
            eod_archive = None

        first_persist = next(
            (c for c in checkpoints if c["status"] == "persisted"), None
        )
        ever_top5 = any(c.get("pass_top_n") is True for c in checkpoints)
        ever_persist = any(c.get("pass_persist") is True for c in checkpoints)
        data_gap = any(c["status"] == "absent_unknown" for c in checkpoints)

        return {
            "ok": True,
            "date": session_date,
            "symbol_resolution": resolved,
            "symbol": sym,
            "universe": universe,
            "cutoffs_policy": {"top_n": TOP_N, "persist_top_n": PERSIST_TOP_N},
            "exclusion_log_coverage": has_exclusion_coverage,
            "data_completeness": {
                "full_exclusion_reasons": has_exclusion_coverage,
                "pre_log_era_gaps": data_gap and not has_exclusion_coverage,
                "note": (
                    "Exclusion reasons available for scans after rs_scan_exclusion_log began."
                    if has_exclusion_coverage
                    else (
                        "No exclusion-log rows for this symbol-day. "
                        "For scans before logging started, only Top-10 presence/absence "
                        "is knowable."
                    )
                ),
            },
            "summary": {
                "in_universe": in_universe,
                "ever_persisted_top10": ever_persist,
                "ever_top5": ever_top5,
                "first_persisted_at_ist": (
                    first_persist.get("scan_time_ist") if first_persist else None
                ),
                "first_persisted_rank": (
                    first_persist.get("rank") if first_persist else None
                ),
                "locked": bool(lock_out),
                "lock": lock_out,
            },
            "checkpoints": checkpoints,
            "lock_audit": lock_audit,
            "anchors": anchors,
            "eod_archive": eod_archive,
        }
    finally:
        db.close()

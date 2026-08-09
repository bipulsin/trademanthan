"""Future Screener — live full-universe filter over materialized RS snapshot.

Reads latest ``rs_universe_score_snapshot`` only (no grade/score recompute).
Pine readiness text is derived from already-stored snapshot fields via the
same ``classify_kavach_readiness`` helper used by checklist cards.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Set

import pytz
from fastapi import APIRouter
from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.kavach_readiness import classify_kavach_readiness

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

router = APIRouter(prefix="/api/dashboard/future-screener", tags=["future-screener"])

# Canonical sets (UI seeds). Live distincts from the latest scan are also returned.
KNOWN_GRADES = ["A+", "A", "A!", "B", "B!", "C", "C*", "C!", "D", "D!"]
KNOWN_READINESS = [
    "READY TO LONG",
    "READY TO SHORT",
    "WATCHING",
    "NOT READY",
]


def _f(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _ist_clock(dt: Any) -> Optional[str]:
    if dt is None:
        return None
    if isinstance(dt, str):
        return dt
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            dt = pytz.UTC.localize(dt)
        return dt.astimezone(IST).strftime("%Y-%m-%d %H:%M:%S")
    return str(dt)


@router.get("")
@router.get("/")
def future_screener_latest() -> Dict[str, Any]:
    """Return latest full-universe snapshot rows for client-side filtering."""
    db = SessionLocal()
    try:
        latest = db.execute(
            text("SELECT max(scan_time) AS t FROM rs_universe_score_snapshot")
        ).mappings().first()
        scan_time = (latest or {}).get("t")
        if not scan_time:
            return {
                "success": True,
                "scan_time": None,
                "session_date": None,
                "rows": [],
                "distinct_grades": list(KNOWN_GRADES),
                "distinct_readiness": list(KNOWN_READINESS),
                "meta": {
                    "source": "rs_universe_score_snapshot",
                    "pct_basis": "previous_close",
                    "pullback_note": "not materialized on universe snapshot",
                    "readiness_note": (
                        "Derived from stored grade/score/kavach_state/volume via "
                        "classify_kavach_readiness; pullback assumed 0 (no candle recompute)."
                    ),
                },
            }

        rows_db = db.execute(
            text(
                """
                SELECT
                    symbol,
                    confidence_grade,
                    trade_score,
                    adx,
                    stock_percent,
                    kavach_state,
                    volume_ratio,
                    volume_tod_ratio,
                    ranking_type,
                    scan_time,
                    session_date
                FROM rs_universe_score_snapshot
                WHERE scan_time = :t
                ORDER BY trade_score DESC NULLS LAST, symbol ASC
                """
            ),
            {"t": scan_time},
        ).mappings().all()

        out_rows: List[Dict[str, Any]] = []
        grades: Set[str] = set()
        readiness_set: Set[str] = set()
        session_date = None

        for r in rows_db:
            grade = (r.get("confidence_grade") or "").strip() or None
            score = _f(r.get("trade_score"))
            kav = (r.get("kavach_state") or "").strip() or None
            vol = _f(r.get("volume_tod_ratio"))
            if vol is None:
                vol = _f(r.get("volume_ratio"))
            pct = _f(r.get("stock_percent"))

            readiness = "NOT READY"
            try:
                classified = classify_kavach_readiness(
                    confidence_display=grade or "",
                    trade_score=float(score or 0),
                    panel_trend=None,
                    kavach_state=kav,
                    pct_from_open=pct,
                    pullback_long=0,
                    pullback_short=0,
                    volume_ratio_for_enter=vol,
                    vol_decel_3=False,
                )
                readiness = str(classified.get("readiness") or "NOT READY")
            except Exception:
                readiness = "NOT READY"

            if grade:
                grades.add(grade)
            readiness_set.add(readiness)
            if session_date is None and r.get("session_date") is not None:
                session_date = str(r["session_date"])

            out_rows.append(
                {
                    "symbol": r.get("symbol"),
                    "confidence_grade": grade,
                    "readiness": readiness,
                    "trade_score": score,
                    "adx": _f(r.get("adx")),
                    "pct_from_open": pct,  # snapshot stock_percent (vs previous close)
                    "pullback_number": None,
                    "candle_ts": _ist_clock(r.get("scan_time")),
                    "kavach_state": kav,
                    "ranking_type": r.get("ranking_type"),
                }
            )

        # Prefer known order, then any extras from live data
        def _ordered(known: List[str], live: Set[str]) -> List[str]:
            seen = set()
            ordered: List[str] = []
            for g in known:
                if g in live and g not in seen:
                    ordered.append(g)
                    seen.add(g)
            for g in sorted(live):
                if g not in seen:
                    ordered.append(g)
                    seen.add(g)
            return ordered

        return {
            "success": True,
            "scan_time": _ist_clock(scan_time),
            "session_date": session_date,
            "total_symbols": len(out_rows),
            "rows": out_rows,
            "distinct_grades": _ordered(KNOWN_GRADES, grades),
            "distinct_readiness": _ordered(KNOWN_READINESS, readiness_set),
            "meta": {
                "source": "rs_universe_score_snapshot",
                "pct_basis": "previous_close",
                "pct_column_label": "% from P.Close",
                "pullback_note": "not on universe snapshot — column shows —",
                "readiness_note": (
                    "Pine READY TO LONG/SHORT/WATCHING/NOT READY derived from stored "
                    "snapshot fields (pullback assumed 0; no candle fetch)."
                ),
                "filter_logic": (
                    "Client applies optional AND across categories; OR within multi-select."
                ),
            },
        }
    except Exception as exc:
        logger.warning("future-screener failed: %s", exc)
        return {"success": False, "error": str(exc), "rows": []}
    finally:
        db.close()

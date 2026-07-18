"""Read-only RS–confidence divergence lookup API."""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

import pytz
from fastapi import APIRouter, Query

from backend.database import SessionLocal
from backend.services.rs_confidence_divergence_lookup import lookup_symbol_day

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

router = APIRouter(prefix="/api/dashboard/rs-divergence", tags=["rs-divergence"])


@router.get("/lookup")
def lookup(
    symbol: str = Query(..., min_length=1, description="Stock symbol e.g. TRENT"),
    date: str = Query(..., min_length=10, max_length=10, description="Session date YYYY-MM-DD"),
    close_miss_threshold: int = Query(8, ge=1, le=50),
    include_upstox: bool = Query(
        False,
        description="Force Upstox 5m OHLC fallback even when Kavach rows exist",
    ),
):
    """Read-only: lock / RS scans / 10m audit / Fast Watch / GO Board for one symbol-day."""
    try:
        return lookup_symbol_day(
            symbol,
            date,
            close_miss_threshold=close_miss_threshold,
            include_upstox=include_upstox,
        )
    except Exception as exc:
        logger.warning("rs-divergence lookup failed: %s", exc)
        return {"ok": False, "error": str(exc), "symbol": symbol, "date": date}


@router.get("/stretch-session")
def stretch_session(
    date: Optional[str] = Query(
        None,
        min_length=10,
        max_length=10,
        description="Session date YYYY-MM-DD (default: today IST)",
    ),
):
    """First stretch-penalty shadow row per symbol for the session (Ready Now review)."""
    session_date = date or datetime.now(IST).strftime("%Y-%m-%d")
    db = SessionLocal()
    try:
        from backend.services.kavach_confidence import (
            hard_stretch_pct,
            soft_stretch_pct,
            stretch_penalty_live_enabled,
        )
        from backend.services.kavach_stretch_penalty_log import first_ready_stretch_rows

        rows = first_ready_stretch_rows(db, session_date)
        penalized = [r for r in rows if int(r.get("stretch_score_penalty") or 0) > 0]
        hard_n = sum(1 for r in penalized if int(r.get("stretch_letter_penalty") or 0) >= 99)
        soft_n = len(penalized) - hard_n
        would_suppress = sum(1 for r in rows if r.get("would_suppress_ready"))
        return {
            "ok": True,
            "date": session_date,
            "stretch_penalty_live": stretch_penalty_live_enabled(),
            "soft_stretch_pct": soft_stretch_pct(),
            "hard_stretch_pct": hard_stretch_pct(),
            "count": len(rows),
            "penalized_count": len(penalized),
            "soft_count": soft_n,
            "hard_count": hard_n,
            "would_suppress_count": would_suppress,
            "rows": rows,
        }
    except Exception as exc:
        logger.warning("stretch-session failed: %s", exc)
        return {"ok": False, "error": str(exc), "date": session_date, "rows": []}
    finally:
        db.close()

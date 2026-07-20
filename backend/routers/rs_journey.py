"""Read-only RS journey / exclusion audit API."""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

import pytz
from fastapi import APIRouter, Query

from backend.services.rs_journey_lookup import lookup_rs_journey

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

router = APIRouter(prefix="/api/dashboard/rs-journey", tags=["rs-journey"])


@router.get("/lookup")
def journey_lookup(
    symbol: str = Query(..., min_length=1, description="Stock symbol e.g. UNIONBANK"),
    date: Optional[str] = Query(
        None,
        min_length=10,
        max_length=10,
        description="Session date YYYY-MM-DD (default: today IST)",
    ),
):
    """Chronological RS eligibility / rank / cutoff / lock trace for one symbol-day."""
    session_date = date or datetime.now(IST).strftime("%Y-%m-%d")
    try:
        return lookup_rs_journey(symbol, session_date)
    except Exception as exc:
        logger.warning("rs-journey lookup failed: %s", exc)
        return {
            "ok": False,
            "error": str(exc),
            "symbol": symbol,
            "date": session_date,
        }

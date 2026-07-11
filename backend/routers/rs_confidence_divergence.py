"""Read-only RS–confidence divergence lookup API."""
from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, Query

from backend.services.rs_confidence_divergence_lookup import lookup_symbol_day

logger = logging.getLogger(__name__)

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

"""Relative Strength Scanner dashboard API.

Read-only endpoint that serves the latest persisted scan snapshot. It never
recalculates indicators — the 5-minute scheduler owns all computation.
"""
import logging

from fastapi import APIRouter

from backend.services.relative_strength_scanner import get_latest_snapshot

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/dashboard", tags=["relative-strength"])


@router.get("/relative-strength")
def relative_strength():
    """Return ``{last_updated, bullish, bearish}`` from the latest scan."""
    try:
        return get_latest_snapshot()
    except Exception as exc:
        logger.warning("relative-strength endpoint failed: %s", exc)
        return {"last_updated": "", "bullish": [], "bearish": [], "error": str(exc)}

"""Garuda screener API — shadow Top-6 for dailyRSchecklist UI (no gating)."""
from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/dashboard/garuda", tags=["garuda-screener"])


@router.get("/latest")
def latest(date: Optional[str] = None):
    """Latest Garuda Top-6 + Part1/Part2 components from shadow log."""
    try:
        from backend.services.garuda_screener.job import get_latest_top6

        return get_latest_top6(date)
    except Exception as exc:
        logger.warning("garuda latest failed: %s", exc)
        return {
            "session_date": date,
            "bar_end": None,
            "top_n": [],
            "empty": True,
            "error": str(exc),
            "warning": (
                "TESTING IN PROGRESS — Garuda is unvalidated. "
                "Forward-performance testing has not been completed. "
                "Do not use for trade decisions."
            ),
        }

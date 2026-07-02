"""Relative Strength Scanner dashboard API.

Read endpoint serves the latest persisted scan snapshot. The 5-minute scheduler
and the EOD (15:32 IST) job own scheduled computation; ``POST …/run`` triggers
an on-demand full-universe recalculation.
"""
import logging
from typing import Optional

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from backend.services.relative_strength_scanner import (
    get_latest_snapshot,
    run_relative_strength_scan,
)

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


@router.post("/relative-strength/run")
def relative_strength_run_now(
    cache_only: Optional[bool] = Query(
        None,
        description=(
            "When true, use only in-process candle cache (market-hours default). "
            "When false, allow direct Upstox fetches. Omit to auto-detect from session."
        ),
    ),
):
    """On-demand RS scan over the full arbitrage_master universe (same job as scheduler).

    Scans all current-month futures, ranks Top 5 Bullish / Bearish, and persists
    a new snapshot. Can take several minutes off-hours when ``cache_only`` is false.
    """
    logger.info("relative_strength_run_now: cache_only=%s", cache_only)
    try:
        kwargs = {}
        if cache_only is not None:
            kwargs["cache_only"] = cache_only
        result = run_relative_strength_scan(scan_trigger="manual_api", **kwargs)
        status = 200 if result.get("ok") else 503
        return JSONResponse(status_code=status, content={"success": result.get("ok", False), **result})
    except Exception as exc:
        logger.exception("relative_strength_run_now failed: %s", exc)
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})


@router.get("/relative-strength/anchors")
def relative_strength_anchors(
    date: Optional[str] = None,
    capture_label: Optional[str] = None,
    symbol: Optional[str] = None,
    limit: int = 500,
):
    """Query archived Top-5 RS snapshots at fixed IST decision times."""
    try:
        from backend.services.rs_scanner_anchors import query_anchor_snapshots

        return {
            "rows": query_anchor_snapshots(
                session_date=date,
                capture_label=capture_label,
                symbol=symbol,
                limit=limit,
            )
        }
    except Exception as exc:
        logger.warning("relative-strength anchors failed: %s", exc)
        return {"rows": [], "error": str(exc)}

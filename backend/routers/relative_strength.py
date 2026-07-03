"""Relative Strength Scanner dashboard API.

Read endpoint serves the latest persisted scan snapshot. The 5-minute scheduler
and the EOD (15:32 IST) job own scheduled computation; ``POST …/run`` triggers
an on-demand full-universe recalculation.
"""
import logging
from typing import Optional

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse, PlainTextResponse

from backend.services.relative_strength_scanner import (
    get_latest_snapshot,
    run_relative_strength_scan,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/dashboard", tags=["relative-strength"])


@router.get("/relative-strength")
def relative_strength():
    """Return conviction board payload (Core 5+5) with setup radar; falls back to raw RS."""
    try:
        from backend.services.rs_conviction_board import get_conviction_board_payload

        payload = get_conviction_board_payload()
        if payload.get("bullish_core") or payload.get("bearish_core"):
            return payload
        return get_latest_snapshot()
    except Exception as exc:
        logger.warning("relative-strength endpoint failed: %s", exc)
        try:
            return get_latest_snapshot()
        except Exception:
            return {"last_updated": "", "bullish": [], "bearish": [], "error": str(exc)}


@router.get("/relative-strength/conviction-board")
def conviction_board():
    try:
        from backend.services.rs_conviction_board import get_conviction_board_payload

        return get_conviction_board_payload()
    except Exception as exc:
        logger.warning("conviction-board failed: %s", exc)
        return {"error": str(exc), "bullish_core": [], "bearish_core": []}


@router.get("/relative-strength/live-setups")
def live_setups():
    try:
        from backend.services.rs_setup_radar import get_live_setups

        return {"live_setups": get_live_setups()}
    except Exception as exc:
        return {"live_setups": [], "error": str(exc)}


@router.get("/relative-strength/config")
def conviction_config_get():
    from backend.services.rs_conviction_config import get_config

    return get_config()


@router.post("/relative-strength/config")
def conviction_config_save(body: dict):
    from backend.services.rs_conviction_config import save_config

    return save_config(body or {})


@router.post("/relative-strength/config/reset")
def conviction_config_reset():
    from backend.services.rs_conviction_config import reset_config

    return reset_config()


@router.get("/relative-strength/export/scoring")
def export_scoring(date: Optional[str] = None):
    from backend.services.rs_conviction_export import export_scoring_csv

    sd = date or ""
    csv_text = export_scoring_csv(date)
    fname = f"rs_conviction_scoring_{sd or 'today'}.csv"
    return PlainTextResponse(
        csv_text,
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )


@router.get("/relative-strength/export/promotions")
def export_promotions(date: Optional[str] = None):
    from backend.services.rs_conviction_export import export_promotions_csv

    sd = date or ""
    csv_text = export_promotions_csv(date)
    fname = f"rs_conviction_promotions_{sd or 'today'}.csv"
    return PlainTextResponse(
        csv_text,
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )


@router.get("/relative-strength/export/radar-log")
def export_radar_log(date: Optional[str] = None):
    from backend.services.rs_conviction_export import export_radar_log_csv

    sd = date or ""
    csv_text = export_radar_log_csv(date)
    fname = f"rs_setup_radar_log_{sd or 'today'}.csv"
    return PlainTextResponse(
        csv_text,
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )


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

"""Market data health API."""
from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from fastapi.responses import JSONResponse

from backend.models.user import User
from backend.routers.auth import get_current_user
from backend.services.market_data.health import get_market_data_health
from backend.services.market_data.scheduler import run_market_data_refresh_job
from backend.services.market_data.warm_cycle_log import latest_warm_cycle, recent_warm_cycles
from backend.services.rs_score_cycle_log import latest_rs_score_cycle, recent_rs_score_cycles

router = APIRouter(prefix="/market-data", tags=["market-data"])


@router.get("/health")
def market_data_health(user: User = Depends(get_current_user)):
    """Last refresh, stale counts, websocket status."""
    try:
        return JSONResponse(
            status_code=200,
            content={"success": True, **get_market_data_health()},
        )
    except Exception as e:
        return JSONResponse(status_code=500, content={"success": False, "message": str(e)})


@router.get("/candle-warm-cycles")
def candle_warm_cycles(
    limit: int = Query(36, ge=1, le=72),
    user: User = Depends(get_current_user),
):
    """Rolling candle-warm cycle deny rates + symbols missing candles."""
    try:
        cycles = recent_warm_cycles(limit=limit)
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "count": len(cycles),
                "latest": latest_warm_cycle(),
                "cycles": cycles,
            },
        )
    except Exception as e:
        return JSONResponse(status_code=500, content={"success": False, "message": str(e)})


@router.get("/rs-score-cycles")
def rs_score_cycles(
    limit: int = Query(36, ge=1, le=72),
    user: User = Depends(get_current_user),
):
    """Rolling RS score cycles: duration + symbols skipped (cache miss / unscored)."""
    try:
        cycles = recent_rs_score_cycles(limit=limit)
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "count": len(cycles),
                "latest": latest_rs_score_cycle(),
                "cycles": cycles,
            },
        )
    except Exception as e:
        return JSONResponse(status_code=500, content={"success": False, "message": str(e)})


@router.post("/refresh")
def market_data_refresh_now(user: User = Depends(get_current_user)):
    """On-demand centralized refresh (admin)."""
    try:
        out = run_market_data_refresh_job()
        return JSONResponse(status_code=200, content={"success": True, **out})
    except Exception as e:
        return JSONResponse(status_code=500, content={"success": False, "message": str(e)})

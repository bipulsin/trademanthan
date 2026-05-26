"""Market data health API."""
from __future__ import annotations

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse

from backend.models.user import User
from backend.routers.auth import get_current_user
from backend.services.market_data.health import get_market_data_health
from backend.services.market_data.scheduler import run_market_data_refresh_job

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


@router.post("/refresh")
def market_data_refresh_now(user: User = Depends(get_current_user)):
    """On-demand centralized refresh (admin)."""
    try:
        out = run_market_data_refresh_job()
        return JSONResponse(status_code=200, content={"success": True, **out})
    except Exception as e:
        return JSONResponse(status_code=500, content={"success": False, "message": str(e)})

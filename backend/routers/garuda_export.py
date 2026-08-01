"""Authenticated Garuda shadow data export API (read-only)."""
from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status

from backend.dependencies import get_current_user
from backend.models import User
from backend.services.garuda_screener.export import (
    DEFAULT_PAGE_LIMIT,
    MAX_PAGE_LIMIT,
    export_garuda_shadow,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/export", tags=["garuda-export"])


@router.get("/garuda-shadow")
def garuda_shadow_export(
    start_date: Optional[str] = Query(
        None, description="YYYY-MM-DD inclusive; defaults to earliest Top-6 shadow date"
    ),
    end_date: Optional[str] = Query(
        None, description="YYYY-MM-DD inclusive; defaults to latest Top-6 shadow date"
    ),
    symbol: Optional[str] = Query(None, description="Optional equity/FO underlying filter"),
    limit: int = Query(DEFAULT_PAGE_LIMIT, ge=1, le=MAX_PAGE_LIMIT),
    offset: int = Query(0, ge=0),
    include_cache_ohlc: bool = Query(
        True,
        description=(
            "If true, best-effort fill open/high/low/volume from in-process "
            "candle_cache (never refetches Upstox)"
        ),
    ),
    _user: User = Depends(get_current_user),
):
    """Export Garuda Top-6 qualification events with RS/grade/forward joins.

    Auth: ``Authorization: Bearer <JWT>`` (same dashboard session token as
    other authenticated tradewithcto.com APIs).
    """
    data = export_garuda_shadow(
        start_date=start_date,
        end_date=end_date,
        symbol=symbol,
        limit=limit,
        offset=offset,
        include_cache_ohlc=include_cache_ohlc,
    )
    if not data.get("ok"):
        err = str(data.get("error") or "export_failed")
        code = (
            status.HTTP_400_BAD_REQUEST
            if err.startswith("invalid_date") or "start_date" in err
            else status.HTTP_500_INTERNAL_SERVER_ERROR
        )
        raise HTTPException(status_code=code, detail=err)
    return data

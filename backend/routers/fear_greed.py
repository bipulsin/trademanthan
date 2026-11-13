"""Fear & Greed dial API — reusable by dashboard and other pages."""
from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from backend.services.fear_greed.service import get_fear_greed

logger = logging.getLogger(__name__)

router = APIRouter(tags=["fear-greed"])


@router.get("/fear-greed")
async def fear_greed_endpoint(
    instrument: str = Query(..., min_length=1, description="Index, stock, crypto, or commodity name"),
    lookback: int = Query(252, ge=60, le=400, description="Trading days of history"),
    timeframe: str = Query("1d", description="Candle timeframe (daily supported)"),
    momentum_w: Optional[float] = Query(None, alias="w_momentum"),
    volatility_w: Optional[float] = Query(None, alias="w_volatility"),
    volume_w: Optional[float] = Query(None, alias="w_volume"),
    oi_w: Optional[float] = Query(None, alias="w_open_interest"),
    premium_w: Optional[float] = Query(None, alias="w_futures_premium"),
    rs_w: Optional[float] = Query(None, alias="w_relative_strength"),
):
    """
    Fear & Greed score (0–100) for one instrument.

    Context/filter only — not an entry or exit signal.
    Example: GET /api/fear-greed?instrument=NIFTY
    """
    try:
        weights = {}
        mapping = {
            "momentum": momentum_w,
            "volatility": volatility_w,
            "volume": volume_w,
            "open_interest": oi_w,
            "futures_premium": premium_w,
            "relative_strength": rs_w,
        }
        for k, v in mapping.items():
            if v is not None:
                weights[k] = float(v)
        payload = get_fear_greed(
            instrument,
            lookback=lookback,
            timeframe=timeframe,
            weights=weights or None,
        )
        status = 200 if payload.get("success") else 404
        return JSONResponse(
            status_code=status,
            content=payload,
            headers={"Cache-Control": "public, max-age=60"},
        )
    except ValueError as e:
        return JSONResponse(
            status_code=400,
            content={"success": False, "message": str(e), "score": None, "zone": None},
        )
    except Exception as e:
        logger.exception("fear_greed endpoint: %s", e)
        return JSONResponse(
            status_code=500,
            content={"success": False, "message": str(e), "score": None, "zone": None},
        )

"""Generic security chart API — read-only, isolated from scanner pipelines."""
from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, Query
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from backend.database import get_db
from backend.models.user import User
from backend.routers.auth import get_user_from_token, oauth2_scheme
from backend.services.chart_candles import DEFAULT_CHART_TF, fetch_chart_candles
from backend.services.chart_feed_manager import (
    chart_feed_status,
    chart_subscribe,
    chart_unsubscribe,
    get_chart_live_quote,
)
from backend.services.chart_instrument_resolver import resolve_chart_instrument

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/chart", tags=["security-chart"])


def _require_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)) -> User:
    return get_user_from_token(token, db)


@router.get("/resolve")
def chart_resolve(
    symbol: str = Query(..., description="Underlying or display symbol"),
    instrument_type: str = Query("FUT"),
    instrument_key: Optional[str] = Query(None),
    exchange: Optional[str] = Query("NSE"),
    user: User = Depends(_require_user),
):
    del user
    try:
        meta = resolve_chart_instrument(
            symbol,
            instrument_type,
            instrument_key=instrument_key,
            exchange=exchange,
        )
        return JSONResponse({"success": True, **meta})
    except ValueError as e:
        return JSONResponse(status_code=400, content={"success": False, "error": str(e)})
    except Exception as e:
        logger.exception("chart_resolve: %s", e)
        return JSONResponse(status_code=500, content={"success": False, "error": str(e)})


@router.get("/candles")
def chart_candles(
    symbol: str = Query(...),
    instrument_type: str = Query("FUT"),
    instrument_key: Optional[str] = Query(None),
    timeframe: str = Query(DEFAULT_CHART_TF),
    exchange: Optional[str] = Query("NSE"),
    user: User = Depends(_require_user),
):
    del user
    try:
        meta = resolve_chart_instrument(
            symbol,
            instrument_type,
            instrument_key=instrument_key,
            exchange=exchange,
        )
        ik = meta["instrument_key"]
        payload = fetch_chart_candles(ik, timeframe)
        return JSONResponse(
            {
                "success": True,
                **meta,
                **payload,
            },
            headers={"Cache-Control": "no-store"},
        )
    except ValueError as e:
        return JSONResponse(status_code=400, content={"success": False, "error": str(e)})
    except Exception as e:
        logger.exception("chart_candles: %s", e)
        return JSONResponse(status_code=500, content={"success": False, "error": str(e)})


@router.post("/subscribe")
def chart_subscribe_route(
    instrument_key: str = Query(...),
    user: User = Depends(_require_user),
):
    del user
    try:
        out = chart_subscribe(instrument_key)
        return JSONResponse({"success": True, **out})
    except ValueError as e:
        return JSONResponse(status_code=400, content={"success": False, "error": str(e)})
    except Exception as e:
        logger.exception("chart_subscribe: %s", e)
        return JSONResponse(status_code=500, content={"success": False, "error": str(e)})


@router.post("/unsubscribe")
def chart_unsubscribe_route(
    instrument_key: str = Query(...),
    user: User = Depends(_require_user),
):
    del user
    try:
        out = chart_unsubscribe(instrument_key)
        return JSONResponse({"success": True, **out})
    except Exception as e:
        logger.exception("chart_unsubscribe: %s", e)
        return JSONResponse(status_code=500, content={"success": False, "error": str(e)})


@router.get("/live")
def chart_live(
    instrument_key: str = Query(...),
    user: User = Depends(_require_user),
):
    del user
    try:
        q = get_chart_live_quote(instrument_key)
        if not q:
            return JSONResponse(
                {"success": True, "instrument_key": instrument_key, "ltp": None, "stale": True}
            )
        return JSONResponse({"success": True, **q, "stale": False})
    except Exception as e:
        logger.exception("chart_live: %s", e)
        return JSONResponse(status_code=500, content={"success": False, "error": str(e)})


@router.get("/status")
def chart_status(user: User = Depends(_require_user)):
    del user
    return JSONResponse({"success": True, **chart_feed_status()})

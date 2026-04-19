"""
Daily Futures — ChartInk webhook + authenticated workspace (Today's pick / Running / Closed).
"""
from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, Body, Depends, Header, HTTPException, Request
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from backend.database import get_db
from backend.models.user import User
from backend.routers.auth import get_user_from_token, oauth2_scheme
from backend.services.daily_futures_service import (
    confirm_buy,
    confirm_sell,
    get_workspace,
    normalize_symbols_from_payload,
    process_chartink_webhook,
    webhook_secret_ok,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/daily-futures", tags=["daily-futures"])


def _auth_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)) -> User:
    return get_user_from_token(token, db)


class BuyBody(BaseModel):
    screening_id: int = Field(..., ge=1)
    entry_time: str = Field(..., min_length=3, max_length=16)
    entry_price: float = Field(..., gt=0)


class SellBody(BaseModel):
    trade_id: int = Field(..., ge=1)
    exit_time: str = Field(..., min_length=3, max_length=16)
    exit_price: float = Field(..., gt=0)


@router.get("/workspace")
def daily_futures_workspace(
    user: User = Depends(_auth_user),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Today's picks, running orders, closed trades + PnL summary for the logged-in user."""
    return get_workspace(db, user.id)


@router.post("/order/buy")
def daily_futures_buy(
    body: BuyBody,
    user: User = Depends(_auth_user),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    try:
        return confirm_buy(db, user.id, body.screening_id, body.entry_time, body.entry_price)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/order/sell")
def daily_futures_sell(
    body: SellBody,
    user: User = Depends(_auth_user),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    try:
        return confirm_sell(db, user.id, body.trade_id, body.exit_time, body.exit_price)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/webhook/chartink")
async def chartink_webhook(
    request: Request,
    secret: Optional[str] = None,
    x_daily_futures_secret: Optional[str] = Header(None, alias="X-Daily-Futures-Secret"),
) -> Dict[str, Any]:
    """
    ChartInk (or any caller) sends symbols every ~15 minutes.
    Query: ?secret=... or header X-Daily-Futures-Secret (must match CHARTINK_DAILY_FUTURES_SECRET).
    Body: JSON object with symbols / symbol / stocks list, or a plain JSON array, or text/plain symbols.
    """
    prov = secret or x_daily_futures_secret or ""
    if not webhook_secret_ok(prov):
        raise HTTPException(status_code=401, detail="Invalid or missing webhook secret")

    payload: Any = None
    ct = (request.headers.get("content-type") or "").lower()
    try:
        if "application/json" in ct:
            payload = await request.json()
        else:
            raw = (await request.body()).decode("utf-8", errors="replace").strip()
            if raw.startswith("{") or raw.startswith("["):
                import json

                payload = json.loads(raw)
            else:
                payload = raw
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Could not parse body: {e}")

    symbols = normalize_symbols_from_payload(payload)
    if not symbols:
        raise HTTPException(status_code=400, detail="No symbols found in payload")

    try:
        summary = process_chartink_webhook(symbols)
        summary["symbols_received"] = len(symbols)
        return {"success": True, **summary}
    except Exception as e:
        logger.exception("daily_futures webhook failed")
        raise HTTPException(status_code=500, detail=str(e))

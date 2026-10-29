"""MomentX API — Ready-to-Trade list + manual scan/EMA triggers."""
from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, BackgroundTasks, Depends
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from backend.database import get_db
from backend.models.user import User
from backend.routers.auth import get_user_from_token, oauth2_scheme
from backend.services.momentx.service import get_ready_payload, take_trade_log

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/momentx", tags=["momentx"])


def _auth_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)) -> User:
    return get_user_from_token(token, db)


class TakeTradeBody(BaseModel):
    stock: str = Field(..., min_length=1)
    future_symbol: Optional[str] = None
    future_instrument_key: Optional[str] = None
    ltp: Optional[float] = None
    ema5: Optional[float] = None
    ema15: Optional[float] = None
    ema50: Optional[float] = None


@router.get("/ready")
def ready(_user: User = Depends(_auth_user)) -> Dict[str, Any]:
    """Current Ready-to-Trade cards with LTP, EMAs, trigger flag, Stage-1 values."""
    return get_ready_payload(ensure=True)


@router.post("/scan")
def trigger_scan(
    background_tasks: BackgroundTasks,
    _user: User = Depends(_auth_user),
) -> Dict[str, Any]:
    """Kick a Stage-1 scan in the background (market-hours only unless forced by job)."""

    def _run() -> None:
        from backend.services.momentx.scanner import run_stage1_scan

        run_stage1_scan(force=False)

    background_tasks.add_task(_run)
    return {"ok": True, "queued": True}


@router.post("/refresh-ema")
def trigger_ema(
    background_tasks: BackgroundTasks,
    _user: User = Depends(_auth_user),
) -> Dict[str, Any]:
    def _run() -> None:
        from backend.services.momentx.tracker import refresh_emas

        refresh_emas(force=True)

    background_tasks.add_task(_run)
    return {"ok": True, "queued": True}


@router.post("/take-trade")
def take_trade(body: TakeTradeBody, _user: User = Depends(_auth_user)) -> Dict[str, Any]:
    """Confirm Take Trade — logs only; does not place an order."""
    return take_trade_log(body.model_dump())

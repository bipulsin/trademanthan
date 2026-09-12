"""Authenticated Commodities Div UI APIs."""
from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from backend.database import get_db
from backend.models.user import User
from backend.routers.auth import get_user_from_token, oauth2_scheme
from backend.services.commodities_div.actions import (
    exit_submit,
    get_active_signal,
    list_history,
    take_trade,
    workspace_payload,
)
from backend.services.commodities_div.ltp_sidecar import refresh_in_trade_ltp
from backend.services.commodities_div.mapping import delete_mapping, list_mappings, upsert_mapping
from backend.services.commodities_div.schema import ensure_commodities_div_tables
from backend.services.commodities_div.webhook import now_ist_second

logger = logging.getLogger(__name__)

router = APIRouter(tags=["commodities-div"])


def _auth_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)) -> User:
    return get_user_from_token(token, db)


class TakeTradeBody(BaseModel):
    signal_id: int
    entry_price: float = Field(..., gt=0)


class ExitSubmitBody(BaseModel):
    signal_id: int
    exit_price: float = Field(..., gt=0)
    exit_at: str = Field(..., min_length=8, description="YYYY-MM-DD HH:MM:SS IST")


class MappingBody(BaseModel):
    tv_ticker: str = Field(..., min_length=1)
    upstox_symbol: str = Field(..., min_length=1)
    exchange: str = Field(default="MCX")
    notes: Optional[str] = None


@router.get("/health")
def commodities_div_health() -> Dict[str, Any]:
    ensure_commodities_div_tables()
    return {
        "ok": True,
        "service": "commodities_div",
        "server_time_ist": now_ist_second().strftime("%Y-%m-%d %H:%M:%S"),
    }


@router.get("/workspace")
def commodities_div_workspace(user: User = Depends(_auth_user)) -> Dict[str, Any]:
    ensure_commodities_div_tables()
    payload = workspace_payload()
    payload["mappings"] = list_mappings()
    payload["user"] = getattr(user, "email", None) or getattr(user, "id", None)
    return payload


@router.get("/active")
def commodities_div_active(user: User = Depends(_auth_user)) -> Dict[str, Any]:
    return {"ok": True, "active": get_active_signal()}


@router.get("/history")
def commodities_div_history(user: User = Depends(_auth_user)) -> Dict[str, Any]:
    return {"ok": True, "history": list_history(100)}


@router.post("/take-trade")
def commodities_div_take_trade(
    body: TakeTradeBody,
    user: User = Depends(_auth_user),
) -> Dict[str, Any]:
    try:
        row = take_trade(signal_id=body.signal_id, entry_price=body.entry_price)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return {"ok": True, "signal": row}


@router.post("/exit-submit")
def commodities_div_exit_submit(
    body: ExitSubmitBody,
    user: User = Depends(_auth_user),
) -> Dict[str, Any]:
    try:
        row = exit_submit(
            signal_id=body.signal_id,
            exit_price=body.exit_price,
            exit_at=body.exit_at,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return {"ok": True, "signal": row}


@router.get("/mappings")
def commodities_div_list_mappings(user: User = Depends(_auth_user)) -> Dict[str, Any]:
    return {"ok": True, "mappings": list_mappings()}


@router.put("/mappings")
@router.post("/mappings")
def commodities_div_upsert_mapping(
    body: MappingBody,
    user: User = Depends(_auth_user),
) -> Dict[str, Any]:
    try:
        row = upsert_mapping(
            tv_ticker=body.tv_ticker,
            upstox_symbol=body.upstox_symbol,
            exchange=body.exchange or "MCX",
            notes=body.notes,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return {"ok": True, "mapping": row}


@router.delete("/mappings/{map_id}")
def commodities_div_delete_mapping(
    map_id: int,
    user: User = Depends(_auth_user),
) -> Dict[str, Any]:
    ok = delete_mapping(map_id)
    if not ok:
        raise HTTPException(status_code=404, detail="mapping not found")
    return {"ok": True, "deleted": map_id}


@router.post("/ltp-refresh")
def commodities_div_ltp_refresh(user: User = Depends(_auth_user)) -> Dict[str, Any]:
    return refresh_in_trade_ltp()

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
    create_manual_history,
    delete_signal,
    exit_submit,
    list_active_tab_signals,
    list_history,
    list_in_trade_signals,
    take_trade,
    update_signal,
    workspace_payload,
)
from backend.services.commodities_div.ltp_sidecar import refresh_in_trade_ltp
from backend.services.commodities_div.mapping import (
    ALLOWED_UNDERLYINGS,
    attach_instrument_fields,
    parse_free_text_commodity,
)
from backend.services.commodities_div.schema import ensure_commodities_div_tables
from backend.services.commodities_div.webhook import now_ist_second

logger = logging.getLogger(__name__)

router = APIRouter(tags=["commodities-div"])


def _auth_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)) -> User:
    return get_user_from_token(token, db)


class TakeTradeBody(BaseModel):
    signal_id: int
    entry_price: float = Field(..., gt=0)
    trade_mode: Optional[str] = Field(None, description="PAPER | LIVE")


class ExitSubmitBody(BaseModel):
    signal_id: int
    exit_price: float = Field(..., gt=0)
    exit_at: str = Field(..., min_length=8, description="YYYY-MM-DD HH:MM:SS IST")
    entry_price: Optional[float] = Field(None, gt=0)
    trade_taken_at: Optional[str] = Field(None, description="YYYY-MM-DD HH:MM:SS IST")
    direction: Optional[str] = Field(None, description="BULL | BEAR")
    trade_mode: Optional[str] = Field(None, description="PAPER | LIVE")


class UpdateSignalBody(BaseModel):
    signal_id: int
    entry_price: Optional[float] = Field(None, gt=0)
    trade_taken_at: Optional[str] = Field(None, description="YYYY-MM-DD HH:MM:SS IST")
    direction: Optional[str] = Field(None, description="BULL | BEAR")
    exit_price: Optional[float] = Field(None, gt=0)
    exit_at: Optional[str] = Field(None, description="YYYY-MM-DD HH:MM:SS IST")
    trade_mode: Optional[str] = Field(None, description="PAPER | LIVE")


class DeleteSignalBody(BaseModel):
    signal_id: int


class ManualEntryBody(BaseModel):
    commodity: str = Field(..., min_length=1, description="Commodity / TV symbol text")
    direction: str = Field(..., description="BULL | BEAR")
    entry_price: float = Field(..., gt=0)
    trade_taken_at: str = Field(..., min_length=8, description="YYYY-MM-DD HH:MM:SS IST")
    exit_price: Optional[float] = Field(
        None, gt=0, description="Optional; omit with exit_at for In-Trade open row"
    )
    exit_at: Optional[str] = Field(
        None, description="Optional YYYY-MM-DD HH:MM:SS IST; omit with exit_price for In-Trade"
    )
    trade_mode: Optional[str] = Field("PAPER", description="PAPER | LIVE")
    qty: Optional[int] = Field(None, gt=0, description="Optional lot qty override")


@router.get("/health")
def commodities_div_health() -> Dict[str, Any]:
    ensure_commodities_div_tables()
    return {
        "ok": True,
        "service": "commodities_div",
        "server_time_ist": now_ist_second().strftime("%Y-%m-%d %H:%M:%S"),
        "underlyings": list(ALLOWED_UNDERLYINGS),
    }


@router.get("/workspace")
def commodities_div_workspace(user: User = Depends(_auth_user)) -> Dict[str, Any]:
    ensure_commodities_div_tables()
    payload = workspace_payload()
    payload["underlyings"] = list(ALLOWED_UNDERLYINGS)
    payload["user"] = getattr(user, "email", None) or getattr(user, "id", None)
    return payload


@router.get("/active")
def commodities_div_active(user: User = Depends(_auth_user)) -> Dict[str, Any]:
    actives = list_active_tab_signals()
    return {"ok": True, "active": actives, "actives": actives}


@router.get("/in-trade")
def commodities_div_in_trade(user: User = Depends(_auth_user)) -> Dict[str, Any]:
    rows = list_in_trade_signals()
    return {"ok": True, "in_trade": rows}


@router.get("/history")
def commodities_div_history(user: User = Depends(_auth_user)) -> Dict[str, Any]:
    return {"ok": True, "history": list_history(100)}


@router.post("/take-trade")
def commodities_div_take_trade(
    body: TakeTradeBody,
    user: User = Depends(_auth_user),
) -> Dict[str, Any]:
    try:
        row = take_trade(
            signal_id=body.signal_id,
            entry_price=body.entry_price,
            trade_mode=body.trade_mode,
        )
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
            entry_price=body.entry_price,
            trade_taken_at=body.trade_taken_at,
            direction=body.direction,
            trade_mode=body.trade_mode,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return {"ok": True, "signal": row}


@router.get("/resolve-commodity")
def commodities_div_resolve_commodity(
    q: str,
    user: User = Depends(_auth_user),
) -> Dict[str, Any]:
    """
    Preview free-text / TV commodity → MCX contract + lot size (manual entry blur).
    Does not write History / trade_log.
    """
    text = str(q or "").strip()
    if not text:
        raise HTTPException(status_code=400, detail="q (commodity text) is required")
    parsed = parse_free_text_commodity(text)
    inst = attach_instrument_fields(text, resolve_contract=True)
    return {
        "ok": True,
        "input": text,
        "underlying": inst.get("symbol_mapped") or parsed.get("underlying"),
        "underlying_matched": bool(inst.get("underlying_matched")),
        "contract": inst.get("contract") or inst.get("trading_symbol"),
        "trading_symbol": inst.get("trading_symbol"),
        "instrument_key": inst.get("instrument_key"),
        "lot_size": inst.get("lot_size"),
        "display_symbol": inst.get("display_symbol"),
        "contract_month": inst.get("contract_month"),
        "contract_year": inst.get("contract_year"),
        "match_mode": inst.get("match_mode"),
        "parse_mode": inst.get("parse_mode") or parsed.get("parse_mode"),
        "expiry_fallback": bool(inst.get("expiry_fallback")),
    }


@router.post("/manual-entry")
def commodities_div_manual_entry(
    body: ManualEntryBody,
    user: User = Depends(_auth_user),
) -> Dict[str, Any]:
    """
    Manual trade entry.

    - Exit omitted → In-Trade open row (no trade_log until exit_submit).
    - Exit complete → History (+ LIVE trade_log upsert).
    """
    try:
        row = create_manual_history(
            commodity=body.commodity,
            direction=body.direction,
            entry_price=body.entry_price,
            trade_taken_at=body.trade_taken_at,
            exit_price=body.exit_price,
            exit_at=body.exit_at,
            trade_mode=body.trade_mode,
            qty=body.qty,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return {"ok": True, "signal": row}


@router.patch("/signal")
@router.post("/signal/update")
def commodities_div_update_signal(
    body: UpdateSignalBody,
    user: User = Depends(_auth_user),
) -> Dict[str, Any]:
    try:
        row = update_signal(
            signal_id=body.signal_id,
            entry_price=body.entry_price,
            trade_taken_at=body.trade_taken_at,
            direction=body.direction,
            exit_price=body.exit_price,
            exit_at=body.exit_at,
            trade_mode=body.trade_mode,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return {"ok": True, "signal": row}


@router.delete("/signal/{signal_id}")
def commodities_div_delete_signal(
    signal_id: int,
    user: User = Depends(_auth_user),
) -> Dict[str, Any]:
    try:
        return delete_signal(signal_id=int(signal_id))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.post("/signal/delete")
def commodities_div_delete_signal_post(
    body: DeleteSignalBody,
    user: User = Depends(_auth_user),
) -> Dict[str, Any]:
    try:
        return delete_signal(signal_id=int(body.signal_id))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.post("/ltp-refresh")
def commodities_div_ltp_refresh(user: User = Depends(_auth_user)) -> Dict[str, Any]:
    out = refresh_in_trade_ltp()
    try:
        from backend.services.commodities_div.ws_ltp import sync_in_trade_subscriptions

        out["ws_sync"] = sync_in_trade_subscriptions(force=True)
    except Exception as e:
        out["ws_sync"] = {"ok": False, "error": str(e)[:200]}
    return out

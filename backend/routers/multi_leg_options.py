"""Multi-leg options journal API. Login required, same token as the other pages."""
from __future__ import annotations

import logging
from datetime import date
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from backend.database import get_db
from backend.models.user import User
from backend.routers.auth import get_user_from_token, oauth2_scheme
from backend.services.multi_leg_options import (
    MultiLegNotFound,
    MultiLegValidationError,
    close_trade,
    create_trade,
    delete_trade,
    get_quote,
    get_trade,
    list_active,
    list_instruments,
    list_report,
    refresh_active_quotes,
    suggested_expiry,
    update_trade,
)
from backend.services.multi_leg_upstox_sync import (
    assign_orphan,
    journal_orphan_state,
    sync_from_upstox,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/multi-leg-options", tags=["multi-leg-options"])


def _auth_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)) -> User:
    return get_user_from_token(token, db)


class LegBody(BaseModel):
    id: Optional[str] = None
    side: str
    option_type: str
    strike_price: float = Field(..., gt=0)
    leg_expiry_date: Optional[str] = None
    entry_price: float = Field(..., ge=0)
    entry_time: str
    exit_price: Optional[float] = Field(None, ge=0)
    exit_time: Optional[str] = None
    ltp: Optional[float] = None
    delta: Optional[float] = None


class TradeBody(BaseModel):
    trade_type: str
    instrument: str
    spot_price_entry: float = Field(..., gt=0)
    entry_date: Optional[str] = None
    expiry_date: Optional[str] = None
    legs: List[LegBody] = Field(default_factory=list)


class CloseBody(BaseModel):
    legs: Optional[List[LegBody]] = None


class AssignBody(BaseModel):
    trade_id: str


class SyncBody(BaseModel):
    trade_date: Optional[str] = None


def _call(fn, *args, **kwargs):
    try:
        return fn(*args, **kwargs)
    except MultiLegNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except MultiLegValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/instruments")
def instruments(_user: User = Depends(_auth_user)) -> Dict[str, Any]:
    return {"ok": True, **list_instruments()}


@router.get("/expiry")
def expiry_default(
    instrument: str = Query(..., min_length=1),
    entry_date: Optional[str] = Query(None),
    _user: User = Depends(_auth_user),
) -> Dict[str, Any]:
    raw = (entry_date or "")[:10]
    try:
        entry = date.fromisoformat(raw) if raw else date.today()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="entry_date must be YYYY-MM-DD") from exc
    try:
        exp = suggested_expiry(entry, instrument.strip().upper())
    except MultiLegValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {
        "ok": True,
        "instrument": instrument.strip().upper(),
        "entry_date": entry.isoformat(),
        "expiry_date": exp.isoformat(),
    }


@router.get("/quote")
def quote_one(
    instrument: str = Query(...),
    strike: float = Query(..., gt=0),
    option_type: str = Query(...),
    expiry: str = Query(...),
    _user: User = Depends(_auth_user),
) -> Dict[str, Any]:
    return {"ok": True, "quote": get_quote(instrument, strike, option_type, expiry)}


@router.get("/quotes")
def quotes_active(_user: User = Depends(_auth_user)) -> Dict[str, Any]:
    payload = refresh_active_quotes()
    payload.update(journal_orphan_state())
    return payload


@router.get("/trades/active")
def active(_user: User = Depends(_auth_user)) -> Dict[str, Any]:
    trades = list_active()
    return {"ok": True, "count": len(trades), "trades": trades, **journal_orphan_state()}


@router.post("/sync/upstox")
def sync_upstox(
    body: Optional[SyncBody] = None,
    _user: User = Depends(_auth_user),
) -> Dict[str, Any]:
    """Import executed NIFTY, BANKNIFTY, and SENSEX option fills for one IST day."""
    raw = body.trade_date if body is not None else None
    return _call(sync_from_upstox, raw)


@router.post("/orphans/{leg_id}/assign")
def assign_orphan_leg(
    leg_id: str,
    body: AssignBody,
    _user: User = Depends(_auth_user),
) -> Dict[str, Any]:
    return _call(assign_orphan, leg_id, body.trade_id)


@router.get("/trades/report")
def report(
    status: Optional[str] = Query(None),
    instrument: Optional[str] = Query(None),
    trade_type: Optional[str] = Query(None),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    _user: User = Depends(_auth_user),
) -> Dict[str, Any]:
    trades = _call(
        list_report,
        status=status,
        instrument=instrument,
        trade_type=trade_type,
        date_from=date_from,
        date_to=date_to,
    )
    return {"ok": True, "count": len(trades), "trades": trades}


@router.post("/trades")
def post_trade(body: TradeBody, _user: User = Depends(_auth_user)) -> Dict[str, Any]:
    trade = _call(create_trade, body.model_dump())
    return {"ok": True, "trade": trade}


@router.put("/trades/{trade_id}")
def put_trade(trade_id: str, body: TradeBody, _user: User = Depends(_auth_user)) -> Dict[str, Any]:
    trade = _call(update_trade, trade_id, body.model_dump())
    return {"ok": True, "trade": trade}


@router.post("/trades/{trade_id}/close")
def post_close(
    trade_id: str,
    body: Optional[CloseBody] = None,
    _user: User = Depends(_auth_user),
) -> Dict[str, Any]:
    payload = body.model_dump() if body is not None else {}
    trade = _call(close_trade, trade_id, payload)
    return {"ok": True, "trade": trade}


@router.delete("/trades/{trade_id}")
def remove_trade(trade_id: str, _user: User = Depends(_auth_user)) -> Dict[str, Any]:
    _call(delete_trade, trade_id)
    return {"ok": True, "deleted": trade_id}


@router.get("/trades/{trade_id}")
def one_trade(trade_id: str, _user: User = Depends(_auth_user)) -> Dict[str, Any]:
    trade = get_trade(trade_id)
    if not trade:
        raise HTTPException(status_code=404, detail="trade not found")
    return {"ok": True, "trade": trade}

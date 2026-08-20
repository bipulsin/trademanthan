"""Trade journal UI API — paste / form / edit trade_log rows."""
from __future__ import annotations

from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from backend.database import get_db
from backend.models.user import User
from backend.routers.auth import get_user_from_token, oauth2_scheme
from backend.services.rule27_trade_log import (
    ensure_trade_log_table,
    get_trade,
    list_trades,
    update_trade_fields,
    upsert_trade,
)
from backend.services.trade_log_journal import (
    enrich_from_master,
    parse_journal_text,
    payload_from_enriched,
    refresh_session_log,
)

router = APIRouter(prefix="/api/trade-log", tags=["trade-log"])
SOURCE = "tradelog_ui"


def _auth_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)) -> User:
    return get_user_from_token(token, db)


class ParseBody(BaseModel):
    text: str = Field(..., min_length=10)


class TradeBody(BaseModel):
    session_date: str
    symbol: str
    direction: str
    entry_time: str
    entry_price: float
    exit_time: Optional[str] = None
    exit_price: Optional[float] = None
    qty: Optional[int] = None
    slippage_pts: Optional[float] = None
    exit_price_intended: Optional[float] = None
    exit_trigger_type: Optional[str] = None
    exit_trigger: Optional[str] = None
    notes: Optional[str] = None
    journal_text: Optional[str] = None


class PatchBody(BaseModel):
    session_date: Optional[str] = None
    symbol: Optional[str] = None
    direction: Optional[str] = None
    qty: Optional[int] = None
    entry_time: Optional[str] = None
    entry_price: Optional[float] = None
    exit_time: Optional[str] = None
    exit_price: Optional[float] = None
    slippage_pts: Optional[float] = None
    exit_price_intended: Optional[float] = None
    exit_trigger_type: Optional[str] = None
    exit_trigger: Optional[str] = None
    notes: Optional[str] = None


def _row_after(db: Session, trade_id: int) -> Dict[str, Any]:
    row = get_trade(db, trade_id)
    if not row:
        raise HTTPException(status_code=404, detail="trade not found")
    return row


@router.get("")
def list_journal(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    user: User = Depends(_auth_user),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    ensure_trade_log_table()
    rows = list_trades(db, start_date, end_date)
    return {"ok": True, "count": len(rows), "trades": rows}


@router.post("/parse")
def parse_journal(
    body: ParseBody,
    user: User = Depends(_auth_user),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    parsed = parse_journal_text(body.text)
    enriched = enrich_from_master(db, parsed)
    return {"ok": True, "parsed": enriched}


@router.post("")
def create_journal(
    body: TradeBody,
    user: User = Depends(_auth_user),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    ensure_trade_log_table()
    notes = body.notes or body.journal_text or ""
    seed = {
        "session_date": body.session_date,
        "symbol": body.symbol,
        "direction": body.direction,
        "entry_time": body.entry_time,
        "entry_price": body.entry_price,
        "exit_time": body.exit_time,
        "exit_price": body.exit_price,
        "qty": body.qty,
        "slippage_pts": body.slippage_pts,
        "exit_price_intended": body.exit_price_intended,
        "exit_trigger_type": body.exit_trigger_type,
        "exit_trigger": body.exit_trigger,
        "notes": notes,
        "parse_warnings": [],
    }
    try:
        enriched = enrich_from_master(db, seed)
        payload = payload_from_enriched(enriched, source=SOURCE)
        trade_id = upsert_trade(db, payload)
        refresh_session_log(db, payload["session_date"], payload.get("exit_time"), SOURCE)
        db.commit()
    except ValueError as exc:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except IntegrityError as exc:
        db.rollback()
        raise HTTPException(
            status_code=409,
            detail="A trade with this date, symbol, side, and entry time already exists",
        ) from exc
    return {"ok": True, "id": trade_id, "trade": _row_after(db, trade_id), "enriched": enriched}


@router.patch("/{trade_id}")
def patch_journal(
    trade_id: int,
    body: PatchBody,
    user: User = Depends(_auth_user),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    ensure_trade_log_table()
    patch = body.model_dump(exclude_unset=True)
    try:
        if "symbol" in patch and patch["symbol"]:
            enriched = enrich_from_master(
                db,
                {
                    "symbol": patch["symbol"],
                    "direction": patch.get("direction") or "LONG",
                    "session_date": patch.get("session_date") or "2000-01-01",
                    "entry_time": patch.get("entry_time") or "09:15:00",
                    "entry_price": patch.get("entry_price") or 1.0,
                    "qty": patch.get("qty"),
                    "parse_warnings": [],
                },
            )
            if not enriched.get("master_ok"):
                raise ValueError("symbol not found in arbitrage_master")
            patch["symbol"] = enriched["symbol"]
            if enriched.get("contract"):
                patch["contract"] = enriched["contract"]
            if patch.get("qty") is None and enriched.get("qty") is not None:
                patch["qty"] = enriched["qty"]
        update_trade_fields(db, trade_id, patch)
        row = get_trade(db, trade_id)
        if row:
            refresh_session_log(
                db,
                str(row.get("session_date") or "")[:10],
                row.get("exit_time"),
                SOURCE,
            )
        db.commit()
    except ValueError as exc:
        db.rollback()
        # Keep 404 for missing rows; other validation → 400
        code = 404 if "not found" in str(exc).lower() and "symbol" not in str(exc).lower() else 400
        if str(exc) == "trade not found":
            code = 404
        raise HTTPException(status_code=code, detail=str(exc)) from exc
    except IntegrityError as exc:
        db.rollback()
        raise HTTPException(
            status_code=409,
            detail="Entry time conflicts with another trade for this symbol/side/date",
        ) from exc
    return {"ok": True, "id": trade_id, "trade": _row_after(db, trade_id)}

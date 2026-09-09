"""Stock Options webhook + workspace. POST /webhook/stockOption (and stockoption)."""
from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from backend.database import get_db
from backend.models.user import User
from backend.routers.auth import get_user_from_token, oauth2_scheme
from backend.services.stock_option_signals import (
    decode_raw_payload,
    insert_webhook_and_signals,
    list_workspace,
    now_ist_second,
    set_hard_stop_placed,
    submit_trade,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["stock-option"])


def _auth_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)) -> User:
    return get_user_from_token(token, db)


def _source_ip(request: Request) -> Optional[str]:
    xff = (request.headers.get("x-forwarded-for") or "").strip()
    if xff:
        return xff.split(",")[0].strip()[:64]
    xri = (request.headers.get("x-real-ip") or "").strip()
    if xri:
        return xri[:64]
    if request.client and request.client.host:
        return str(request.client.host).strip()[:64]
    return None


class SubmitBody(BaseModel):
    date_traded: str = Field(..., min_length=8, max_length=10)
    buy_strike: float = Field(..., gt=0)
    buy_cost: float = Field(..., ge=0)
    sell_strike: float = Field(..., gt=0)
    sell_cost: float = Field(..., ge=0)


class HardStopBody(BaseModel):
    placed: bool = True


async def _ingest(request: Request) -> JSONResponse:
    received_at = now_ist_second()
    source_ip = _source_ip(request)
    body = await request.body()
    parsed, raw_payload = decode_raw_payload(body)
    logger.info(
        "stock_option webhook received_at=%s source_ip=%s body_len=%d",
        received_at.isoformat(),
        source_ip,
        len(body or b""),
    )
    try:
        result = insert_webhook_and_signals(
            received_at=received_at,
            source_ip=source_ip,
            parsed=parsed,
            raw_payload=raw_payload,
        )
    except Exception as e:
        logger.exception("stock_option webhook persist failed: %s", e)
        return JSONResponse(
            status_code=503,
            content={
                "ok": False,
                "received_at": received_at.strftime("%Y-%m-%d %H:%M:%S"),
                "message": "Could not store webhook; retry",
            },
        )
    return JSONResponse(
        status_code=200,
        content={
            "ok": True,
            "received_at": received_at.strftime("%Y-%m-%d %H:%M:%S"),
            "stored": True,
            **result,
        },
    )


@router.post("/webhook/stockOption")
@router.post("/webhook/stockoption")
async def stock_option_webhook(request: Request) -> JSONResponse:
    return await _ingest(request)


@router.get("/webhook/stockOption")
@router.get("/webhook/stockoption")
async def stock_option_webhook_ping() -> JSONResponse:
    return JSONResponse(
        status_code=200,
        content={
            "ok": True,
            "webhook": "https://www.tradewithcto.com/webhook/stockOption",
            "also": ["/webhook/stockoption"],
            "json_fields": [
                "stocks",
                "trigger_prices",
                "triggered_at",
                "scan_name",
                "scan_url",
                "alert_name",
                "webhook_url",
                "columns[].symbol",
                "columns[].williamsr",
            ],
        },
    )


@router.get("/stock-options/workspace")
async def stock_option_workspace(_user: User = Depends(_auth_user)) -> JSONResponse:
    try:
        data = list_workspace()
    except Exception as e:
        logger.exception("stock_option workspace failed: %s", e)
        raise HTTPException(status_code=503, detail="workspace unavailable") from e
    return JSONResponse(status_code=200, content={"ok": True, **data})


@router.post("/stock-options/signals/{signal_id}/submit")
async def stock_option_submit(
    signal_id: int,
    body: SubmitBody,
    _user: User = Depends(_auth_user),
) -> JSONResponse:
    try:
        out = submit_trade(
            signal_id,
            date_traded=body.date_traded,
            buy_strike=body.buy_strike,
            buy_cost=body.buy_cost,
            sell_strike=body.sell_strike,
            sell_cost=body.sell_cost,
        )
    except LookupError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return JSONResponse(status_code=200, content=out)


@router.post("/stock-options/signals/{signal_id}/hard-stop")
async def stock_option_hard_stop(
    signal_id: int,
    body: HardStopBody,
    _user: User = Depends(_auth_user),
) -> JSONResponse:
    try:
        out = set_hard_stop_placed(signal_id, body.placed)
    except LookupError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    return JSONResponse(status_code=200, content=out)

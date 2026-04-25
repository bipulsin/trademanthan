"""
Daily Futures — ChartInk webhook + authenticated workspace (Today's pick / Running / Closed).
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import APIRouter, BackgroundTasks, Body, Depends, Header, HTTPException, Request
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session
from starlette.concurrency import run_in_threadpool

from backend.database import get_db
from backend.models.user import User
from backend.routers.auth import get_user_from_token, oauth2_scheme
from backend.services.daily_futures_service import (
    confirm_buy,
    confirm_sell,
    get_conviction_breakdown_debug,
    get_workspace_running_enriched,
    get_workspace_trade_if_could,
    get_workspace,
    normalize_symbols_from_payload,
    persist_chartink_bearish_webhook_raw_body,
    persist_chartink_webhook_raw_body,
    process_chartink_webhook,
    process_chartink_webhook_bearish,
    webhook_bearish_secret_ok,
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
    lite: bool = False,
    user: User = Depends(_auth_user),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Today's picks, running orders, closed trades + PnL summary for the logged-in user."""
    return get_workspace(db, user.id, lite_mode=bool(lite))


@router.get("/workspace/running-enriched")
def daily_futures_workspace_running_enriched(
    user: User = Depends(_auth_user),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Running + 15m strip enrichments fetched independently from heavy sections."""
    return get_workspace_running_enriched(db, user.id)


@router.get("/workspace/trade-if-could")
def daily_futures_workspace_trade_if_could(
    user: User = Depends(_auth_user),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Trade-if-could rows fetched independently to avoid full workspace timeouts."""
    return get_workspace_trade_if_could(db, user.id)


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


@router.get("/webhook/chartink/ping")
def chartink_webhook_path_ping() -> Dict[str, Any]:
    """
    Public health check: confirms Nginx + FastAPI route the Daily Futures webhook.
    ChartInk must use POST to the *post_paths* below (this GET is only for humans / monitors).
    """
    return {
        "ok": True,
        "message": "Route is live. ChartInk alerts must use HTTP POST, not GET.",
        "post_paths": {
            "option_a_api_prefix": "/api/daily-futures/webhook/chartink",
            "option_b_no_prefix": "/daily-futures/webhook/chartink",
        },
        "base_url_example": "https://www.tradewithcto.com/api/daily-futures/webhook/chartink",
        "query_or_header": "Add ?secret=YOUR_SECRET or header X-Daily-Futures-Secret (env CHARTINK_DAILY_FUTURES_SECRET on server if set).",
    }


@router.get("/debug/conviction-breakdown")
def daily_futures_conviction_breakdown(
    future_symbol: str,
    trade_date: Optional[str] = None,
    _user: User = Depends(_auth_user),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """
    Debug helper: fetch latest conviction/candle breakdown row for one future symbol on a date.
    Defaults to today's IST date when trade_date is omitted.
    """
    try:
        return get_conviction_breakdown_debug(db, future_symbol=future_symbol, trade_date=trade_date)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


async def _chartink_webhook_background(symbols: list, raw_saved_name: str) -> None:
    """Upstox + DB ingestion can take 60s+; run off the event loop so ChartInk gets HTTP 200 fast."""
    try:
        out = await run_in_threadpool(process_chartink_webhook, symbols)
        logger.info(
            "daily_futures chartink background done processed=%s trade_date=%s raw_file=%s",
            (out or {}).get("processed"),
            (out or {}).get("trade_date"),
            raw_saved_name,
        )
    except Exception:
        logger.exception("daily_futures chartink background failed raw_file=%s", raw_saved_name)


async def _chartink_bearish_webhook_background(symbols: list, raw_saved_name: str) -> None:
    try:
        out = await run_in_threadpool(process_chartink_webhook_bearish, symbols)
        logger.info(
            "daily_futures_bearish chartink done processed=%s trade_date=%s raw_file=%s",
            (out or {}).get("processed"),
            (out or {}).get("trade_date"),
            raw_saved_name,
        )
    except Exception:
        logger.exception("daily_futures_bearish chartink failed raw_file=%s", raw_saved_name)


@router.post("/webhook/chartink")
async def chartink_webhook(
    request: Request,
    background_tasks: BackgroundTasks,
    secret: Optional[str] = None,
    x_daily_futures_secret: Optional[str] = Header(None, alias="X-Daily-Futures-Secret"),
) -> Dict[str, Any]:
    """
    ChartInk (or any caller) sends symbols every ~15 minutes.
    Query: ?secret=tradewithctodailyfuture (or header X-Daily-Futures-Secret). If env
    CHARTINK_DAILY_FUTURES_SECRET is set, it overrides the default secret.
    Body supports the same ChartInk shape used by /scan/chartink-webhook-bullish
    (e.g. stocks, trigger_prices, scan_name, alert_name), plus plain symbol lists.

    Returns **immediately** with HTTP 200; ingestion runs in a background thread so short
    HTTP client timeouts (e.g. Guzzle) do not cause **499** / aborted processing at 9:15 / 9:30.
    """
    prov = secret or x_daily_futures_secret or ""
    if not webhook_secret_ok(prov):
        raise HTTPException(status_code=401, detail="Invalid or missing webhook secret")

    raw = await request.body()
    if not raw:
        raise HTTPException(status_code=400, detail="Empty body")

    try:
        inbox_path = persist_chartink_webhook_raw_body(raw)
    except OSError as e:
        logger.exception("chartink daily_futures raw persist failed: %s", e)
        raise HTTPException(
            status_code=503,
            detail="Server could not store webhook body; will not drop data — retry in a few seconds",
        ) from e

    receipt_name = Path(inbox_path).name

    payload: Any = None
    ct = (request.headers.get("content-type") or "").lower()
    try:
        if "application/json" in ct:
            payload = json.loads(raw.decode("utf-8", errors="replace"))
        else:
            s = raw.decode("utf-8", errors="replace").strip()
            if s.startswith("{") or s.startswith("["):
                payload = json.loads(s)
            else:
                payload = s
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Could not parse body: {e}")

    symbols = normalize_symbols_from_payload(payload)
    if not symbols:
        raise HTTPException(status_code=400, detail="No symbols found in payload")

    logger.info(
        "daily_futures chartink POST accepted symbol_count=%d raw_receipt=%s (queue background)",
        len(symbols),
        receipt_name,
    )
    sym_copy = list(symbols)
    background_tasks.add_task(_chartink_webhook_background, sym_copy, receipt_name)
    return {
        "success": True,
        "symbols_received": len(sym_copy),
        "queued": True,
        "inbox_receipt": receipt_name,
        "message": "Payload stored; ingestion in background. Refresh Daily Futures workspace in ~30–90s.",
    }


bearish_router = APIRouter(prefix="/daily-futures-bearish", tags=["daily-futures-bearish"])


@bearish_router.post("/webhook/chartink")
async def chartink_bearish_webhook(
    request: Request,
    background_tasks: BackgroundTasks,
    secret: Optional[str] = None,
    x_df_bearish_secret: Optional[str] = Header(None, alias="X-Daily-Futures-Bearish-Secret"),
) -> Dict[str, Any]:
    prov = secret or x_df_bearish_secret or ""
    if not webhook_bearish_secret_ok(prov):
        raise HTTPException(status_code=401, detail="Invalid or missing bearish webhook secret")
    raw = await request.body()
    if not raw:
        raise HTTPException(status_code=400, detail="Empty body")
    try:
        inbox_path = persist_chartink_bearish_webhook_raw_body(raw)
    except OSError as e:
        logger.exception("chartink bearish raw persist failed: %s", e)
        raise HTTPException(
            status_code=503,
            detail="Server could not store webhook body; retry shortly",
        ) from e
    receipt_name = Path(inbox_path).name
    ct = (request.headers.get("content-type") or "").lower()
    try:
        if "application/json" in ct:
            payload: Any = json.loads(raw.decode("utf-8", errors="replace"))
        else:
            s = raw.decode("utf-8", errors="replace").strip()
            if s.startswith("{") or s.startswith("["):
                payload = json.loads(s)
            else:
                payload = s
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Could not parse body: {e}")
    symbols = normalize_symbols_from_payload(payload)
    if not symbols:
        raise HTTPException(status_code=400, detail="No symbols found in payload")
    sym_copy = list(symbols)
    background_tasks.add_task(_chartink_bearish_webhook_background, sym_copy, receipt_name)
    return {
        "success": True,
        "direction": "SHORT",
        "symbols_received": len(sym_copy),
        "queued": True,
        "inbox_receipt": receipt_name,
    }

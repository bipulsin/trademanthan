"""
Daily Futures — ChartInk webhook + authenticated workspace (Today's pick / Running / Closed).
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

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
    df_chartink_accum_finalize_payload,
    df_chartink_accum_extend_from_starlette_form,
    df_chartink_accum_init_from_query,
    df_chartink_audit_json_bytes,
    df_chartink_ingest_parsed_into_accum,
    get_conviction_breakdown_debug,
    get_workspace_running_enriched,
    get_workspace_trade_if_could,
    get_workspace,
    manual_update_conviction_vwap,
    normalize_symbols_from_payload,
    parse_daily_futures_chartink_webhook_body,
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


class ManualConvictionVwapBody(BaseModel):
    screening_id: int = Field(..., ge=1)
    mode: str = Field(..., description="live or entry")
    session_vwap: float = Field(..., gt=0)
    vwap_leg_reason: str | None = Field(
        default=None,
        max_length=500,
        description="Optional; if set, stored as the VWAP leg reason instead of the auto text.",
    )


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


@router.post("/conviction/manual-vwap")
def daily_futures_manual_conviction_vwap(
    body: ManualConvictionVwapBody,
    user: User = Depends(_auth_user),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    mode = str(body.mode or "").strip().lower()
    if mode not in ("live", "entry"):
        raise HTTPException(status_code=400, detail="mode must be 'live' or 'entry'")
    try:
        return manual_update_conviction_vwap(
            db=db,
            user_id=user.id,
            screening_id=body.screening_id,
            mode=mode,  # type: ignore[arg-type]
            session_vwap=body.session_vwap,
            vwap_leg_reason=body.vwap_leg_reason,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/webhook/chartink/ping")
def chartink_webhook_path_ping() -> Dict[str, Any]:
    """
    Public health check: confirms Nginx + FastAPI route the Daily Futures webhook.
    The main webhook also accepts GET (query-only fallback) and multipart/form-data besides JSON.
    """
    return {
        "ok": True,
        "message": "Webhook route live. Prefer POST JSON or form; GET with ?stocks=...&secret=... if POST fails.",
        "methods": ["GET", "POST", "PUT", "PATCH"],
        "urls": {
            "option_a_api_prefix": "/api/daily-futures/webhook/chartink",
            "option_b_no_prefix": "/daily-futures/webhook/chartink",
        },
        "base_url_example": "https://www.tradewithcto.com/api/daily-futures/webhook/chartink",
        "query_or_header": "secret via ?secret= or X-Daily-Futures-Secret (bearish: X-Daily-Futures-Bearish-Secret).",
        "multipart": "multipart/form-data text fields merged like URL-encoded (file parts ignored).",
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


async def _resolve_daily_futures_chartink_symbols(
    request: Request,
    *,
    persist_body: Callable[[bytes], str],
) -> Tuple[List[str], str]:
    """
    Collect symbols from query string + body (JSON, urlencoded, multipart text fields, or GET-only).
    Persists either raw POST bytes or a JSON audit record (GET / multipart).
    """
    qp_items = list(request.query_params.multi_items())
    acc = df_chartink_accum_init_from_query(qp_items)
    method = (request.method or "GET").upper()
    ct = (request.headers.get("content-type") or "").lower()

    if method == "GET":
        audit = df_chartink_audit_json_bytes(
            method=method,
            content_type=ct,
            query_multi_items=qp_items,
            raw_body_note="GET — symbols from query only",
        )
        try:
            inbox_path = persist_body(audit)
        except OSError as e:
            logger.exception("chartink daily_futures audit persist failed: %s", e)
            raise HTTPException(
                status_code=503,
                detail="Server could not store webhook receipt; retry shortly",
            ) from e
        symbols = normalize_symbols_from_payload(df_chartink_accum_finalize_payload(acc))
        return symbols, Path(inbox_path).name

    if "multipart/form-data" in ct:
        form = await request.form()
        mf_summary = df_chartink_accum_extend_from_starlette_form(acc, form)
        audit = df_chartink_audit_json_bytes(
            method=method,
            content_type=ct,
            query_multi_items=qp_items,
            multipart_field_summary=mf_summary,
        )
        try:
            inbox_path = persist_body(audit)
        except OSError as e:
            logger.exception("chartink daily_futures multipart audit persist failed: %s", e)
            raise HTTPException(
                status_code=503,
                detail="Server could not store webhook receipt; retry shortly",
            ) from e
        symbols = normalize_symbols_from_payload(df_chartink_accum_finalize_payload(acc))
        return symbols, Path(inbox_path).name

    raw = await request.body()
    if not raw.strip():
        audit = df_chartink_audit_json_bytes(
            method=method,
            content_type=ct,
            query_multi_items=qp_items,
            raw_body_note="empty_body — symbols from query only",
        )
        try:
            inbox_path = persist_body(audit)
        except OSError as e:
            logger.exception("chartink daily_futures audit persist failed: %s", e)
            raise HTTPException(
                status_code=503,
                detail="Server could not store webhook receipt; retry shortly",
            ) from e
        symbols = normalize_symbols_from_payload(df_chartink_accum_finalize_payload(acc))
        if not symbols:
            raise HTTPException(
                status_code=400,
                detail="Empty body and no symbols in query string (use ?stocks=... or POST a body)",
            )
        return symbols, Path(inbox_path).name

    try:
        inbox_path = persist_body(raw)
    except OSError as e:
        logger.exception("chartink daily_futures raw persist failed: %s", e)
        raise HTTPException(
            status_code=503,
            detail="Server could not store webhook body; will not drop data — retry in a few seconds",
        ) from e

    try:
        inner = parse_daily_futures_chartink_webhook_body(raw, ct)
    except (json.JSONDecodeError, UnicodeDecodeError, ValueError) as e:
        raise HTTPException(status_code=400, detail=f"Could not parse body: {e}") from e

    df_chartink_ingest_parsed_into_accum(acc, inner)
    symbols = normalize_symbols_from_payload(df_chartink_accum_finalize_payload(acc))
    return symbols, Path(inbox_path).name


@router.api_route("/webhook/chartink", methods=["GET", "POST", "PUT", "PATCH"])
async def chartink_webhook(
    request: Request,
    background_tasks: BackgroundTasks,
    secret: Optional[str] = None,
    x_daily_futures_secret: Optional[str] = Header(None, alias="X-Daily-Futures-Secret"),
) -> Dict[str, Any]:
    """
    ChartInk (or any caller) sends symbols every ~15 minutes.
    Query: ?secret=... (or header X-Daily-Futures-Secret). Supports GET (query-only fallback),
    POST JSON, application/x-www-form-urlencoded, and multipart/form-data (text fields; file parts ignored).

    Returns **immediately** with HTTP 200; ingestion runs in a background thread so short
    HTTP client timeouts (e.g. Guzzle) do not cause **499** / aborted processing at 9:15 / 9:30.
    """
    prov = (
        (secret or "").strip()
        or (x_daily_futures_secret or "").strip()
        or (request.query_params.get("secret") or "").strip()
    )
    if not webhook_secret_ok(prov):
        raise HTTPException(status_code=401, detail="Invalid or missing webhook secret")

    symbols, receipt_name = await _resolve_daily_futures_chartink_symbols(
        request, persist_body=persist_chartink_webhook_raw_body
    )
    if not symbols:
        raise HTTPException(status_code=400, detail="No symbols found in payload")

    logger.info(
        "daily_futures chartink %s accepted symbol_count=%d raw_receipt=%s (queue background)",
        (request.method or "?").upper(),
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


@bearish_router.api_route("/webhook/chartink", methods=["GET", "POST", "PUT", "PATCH"])
async def chartink_bearish_webhook(
    request: Request,
    background_tasks: BackgroundTasks,
    secret: Optional[str] = None,
    x_df_bearish_secret: Optional[str] = Header(None, alias="X-Daily-Futures-Bearish-Secret"),
) -> Dict[str, Any]:
    prov = (
        (secret or "").strip()
        or (x_df_bearish_secret or "").strip()
        or (request.query_params.get("secret") or "").strip()
    )
    if not webhook_bearish_secret_ok(prov):
        raise HTTPException(status_code=401, detail="Invalid or missing bearish webhook secret")

    symbols, receipt_name = await _resolve_daily_futures_chartink_symbols(
        request, persist_body=persist_chartink_bearish_webhook_raw_body
    )
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

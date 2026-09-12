"""POST /webhook/commDiv — TradingView Commodities Div ingest."""
from __future__ import annotations

import logging
import os
import secrets
from typing import Optional

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from backend.services.commodities_div.webhook import now_ist_second, process_webhook

logger = logging.getLogger(__name__)

router = APIRouter(tags=["commodities-div-webhook"])


def _expected_token() -> str:
    return (os.getenv("COMM_DIV_WEBHOOK_TOKEN") or "").strip()


def _token_ok(request: Request) -> bool:
    expected = _expected_token()
    if not expected:
        # Fail closed when token not configured in production-like envs;
        # allow empty only if ENVIRONMENT explicitly marks debug/dev.
        env = (os.getenv("ENVIRONMENT") or "production").strip().lower()
        if env in ("dev", "development", "test", "local"):
            return True
        logger.error("COMM_DIV_WEBHOOK_TOKEN not set — rejecting webhook")
        return False
    provided = (request.query_params.get("token") or "").strip()
    if not provided:
        # also accept Authorization: Bearer
        auth = (request.headers.get("authorization") or "").strip()
        if auth.lower().startswith("bearer "):
            provided = auth[7:].strip()
    if not provided:
        return False
    return secrets.compare_digest(provided, expected)


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


_DOC = {
    "ok": False,
    "error": "Method not allowed; use POST",
    "urls": [
        "https://www.tradewithcto.com/webhook/commDiv?token=YOUR_TOKEN",
        "https://tradewithcto.com/webhook/commDiv?token=YOUR_TOKEN",
    ],
    "auth": "Query string ?token= matches env COMM_DIV_WEBHOOK_TOKEN (TradingView cannot set custom headers)",
    "body": '{"flag":"BULL-DIV","symbol":"CRUDEOIL1!","time":1757675460000}',
}


@router.get("/webhook/commDiv")
@router.get("/webhook/commdiv")
async def commodities_div_webhook_get() -> JSONResponse:
    return JSONResponse(status_code=405, content=_DOC, headers={"Allow": "POST"})


@router.post("/webhook/commDiv")
@router.post("/webhook/commdiv")
async def commodities_div_webhook(request: Request) -> JSONResponse:
    received_at = now_ist_second()
    if not _token_ok(request):
        return JSONResponse(
            status_code=401,
            content={
                "ok": False,
                "received_at": received_at.strftime("%Y-%m-%d %H:%M:%S"),
                "message": "Unauthorized — pass ?token= matching COMM_DIV_WEBHOOK_TOKEN",
            },
        )
    source_ip = _source_ip(request)
    body = await request.body()
    logger.info(
        "commodities_div webhook received_at=%s source_ip=%s body_len=%d",
        received_at.isoformat(),
        source_ip,
        len(body or b""),
    )
    try:
        result = process_webhook(received_at=received_at, source_ip=source_ip, body=body)
    except Exception as e:
        logger.exception("commodities_div webhook persist failed: %s", e)
        return JSONResponse(
            status_code=503,
            content={
                "ok": False,
                "received_at": received_at.strftime("%Y-%m-%d %H:%M:%S"),
                "message": "Could not store webhook; retry",
            },
        )
    return JSONResponse(status_code=200, content=result)

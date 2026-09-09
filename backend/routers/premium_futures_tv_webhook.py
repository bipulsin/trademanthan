"""POST /webhook/premium_futures — TradingView Premium Futures ingest (no auth)."""
from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from backend.services.premium_futures_tv_webhook import (
    RECOMMENDED_ALERT_JSON,
    decode_raw_payload,
    insert_tv_webhook_row,
    now_ist_second,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["premium-futures-tv-webhook"])

_DOC = {
    "ok": False,
    "error": "Method not allowed; use POST",
    "urls": [
        "https://www.tradewithcto.com/webhook/premium_futures",
        "https://tradewithcto.com/webhook/premium_futures",
        "https://www.tradewithcto.com/webhook/dailyfutures",
        "https://tradewithcto.com/webhook/dailyfutures",
    ],
    "recommended_alert_message": RECOMMENDED_ALERT_JSON,
    "side": "bullish/long/buy vs bearish/short/sell from action, side, or message text",
}


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


@router.get("/webhook/premium_futures")
@router.get("/webhook/dailyfutures")
async def premium_futures_tv_webhook_get() -> JSONResponse:
    return JSONResponse(status_code=405, content=_DOC, headers={"Allow": "POST"})


@router.post("/webhook/premium_futures")
@router.post("/webhook/dailyfutures")
async def premium_futures_tv_webhook(request: Request) -> JSONResponse:
    received_at = now_ist_second()
    source_ip = _source_ip(request)
    body = await request.body()
    parsed, raw_payload, raw_body = decode_raw_payload(body)
    logger.info(
        "premium_futures TV webhook received_at=%s source_ip=%s body_len=%d",
        received_at.isoformat(),
        source_ip,
        len(body or b""),
    )
    try:
        result = insert_tv_webhook_row(
            received_at=received_at,
            source_ip=source_ip,
            parsed=parsed,
            raw_payload=raw_payload,
            raw_body=raw_body,
        )
    except Exception as e:
        logger.exception("premium_futures TV webhook persist failed: %s", e)
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

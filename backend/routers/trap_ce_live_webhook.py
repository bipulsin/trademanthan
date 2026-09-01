"""POST /webhook/trap_intra — Chartink Trap-CE Live ingest (no auth v1)."""
from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from backend.services.trap_ce_live_webhook import (
    decode_raw_payload,
    insert_webhook_rows,
    now_ist_second,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["trap-ce-live-webhook"])


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


@router.post("/webhook/trap_intra")
async def trap_intra_webhook(request: Request) -> JSONResponse:
    received_at = now_ist_second()
    source_ip = _source_ip(request)
    body = await request.body()
    parsed, raw_payload = decode_raw_payload(body)
    logger.info(
        "trap_ce_live webhook received_at=%s source_ip=%s body_len=%d",
        received_at.isoformat(),
        source_ip,
        len(body or b""),
    )
    try:
        status, n = insert_webhook_rows(
            received_at=received_at,
            source_ip=source_ip,
            parsed=parsed,
            raw_payload=raw_payload,
        )
    except Exception as e:
        logger.exception("trap_ce_live webhook persist failed: %s", e)
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
            "parse_status": status,
            "rows": n,
            "stored": True,
        },
    )

"""POST /webhook/momentx — ChartInk → MomentX (store-first, then activate cards)."""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import APIRouter, BackgroundTasks, Request
from fastapi.responses import JSONResponse
from starlette.concurrency import run_in_threadpool

from backend.services.momentx.chartink_webhook import (
    parse_chartink_momentx_payload,
    persist_momentx_chartink_raw_body,
    process_momentx_chartink_payload,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["momentx-webhook"])


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


async def _process_background(inbox_path: str, content_type: Optional[str]) -> None:
    try:
        raw = Path(inbox_path).read_bytes()
        parsed = parse_chartink_momentx_payload(raw, content_type)
        out = await run_in_threadpool(
            process_momentx_chartink_payload, parsed, inbox_path=inbox_path
        )
        logger.info(
            "momentx webhook background done inbox=%s activated=%s created=%s",
            Path(inbox_path).name,
            (out or {}).get("activated"),
            (out or {}).get("created"),
        )
    except Exception:
        logger.exception("momentx webhook background failed inbox=%s", inbox_path)


@router.get("/webhook/momentx")
async def momentx_webhook_ping() -> Dict[str, Any]:
    return {
        "ok": True,
        "message": "MomentX ChartInk webhook live. POST JSON with stocks + alert_name.",
        "url": "https://www.tradewithcto.com/webhook/momentx",
        "methods": ["POST", "GET"],
    }


@router.post("/webhook/momentx")
async def momentx_chartink_webhook(
    request: Request,
    background_tasks: BackgroundTasks,
) -> JSONResponse:
    """Store ChartInk payload immediately, ack 200, then activate cards in background.

    ChartInk retries on slow/failed responses — persist before parse/process.
    """
    source_ip = _source_ip(request)
    body = await request.body()
    ct = (request.headers.get("content-type") or "").strip() or None
    logger.info(
        "momentx webhook received source_ip=%s body_len=%d ct=%s",
        source_ip,
        len(body or b""),
        ct,
    )
    try:
        inbox_path = persist_momentx_chartink_raw_body(body)
    except Exception as e:
        logger.exception("momentx webhook persist failed: %s", e)
        return JSONResponse(
            status_code=503,
            content={
                "ok": False,
                "stored": False,
                "message": "Could not store webhook; retry",
            },
        )

    receipt = Path(inbox_path).name
    # Best-effort peek for response metadata (never block ack on process).
    symbols_peek = 0
    side_peek = None
    try:
        peek = parse_chartink_momentx_payload(body, ct)
        symbols_peek = len(peek.get("stocks") or [])
        side_peek = peek.get("side")
    except Exception as e:
        logger.info("momentx webhook parse peek failed receipt=%s: %s", receipt, e)

    background_tasks.add_task(_process_background, inbox_path, ct)
    return JSONResponse(
        status_code=200,
        content={
            "ok": True,
            "stored": True,
            "queued": True,
            "inbox_receipt": receipt,
            "symbols_received": symbols_peek,
            "side": side_peek,
            "message": "Payload stored; MomentX card activation queued.",
        },
    )

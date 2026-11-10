"""Dhan API-key consent callback and order postback ack.

Redirect URL: https://www.tradewithcto.com/webhook/dhan/callback
Postback URL: https://www.tradewithcto.com/webhook/dhan/postback

The callback stores an access token for trade reads. It does not place orders
and does not log the token.
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse

from backend.services.dhan_service import DhanApiError, DhanAuthError, dhan_service

logger = logging.getLogger(__name__)

router = APIRouter(tags=["dhan-webhook"])

_PAGE = """<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"><title>Dhan</title>
<style>
body {{ font-family: sans-serif; background:#0c1210; color:#e7f0ea; display:flex;
       min-height:100vh; align-items:center; justify-content:center; margin:0; }}
p {{ max-width: 28rem; line-height: 1.45; }}
</style></head><body><p>{message}</p></body></html>
"""


@router.get("/webhook/dhan/callback")
async def dhan_consent_callback(request: Request) -> HTMLResponse:
    token_id = str(request.query_params.get("tokenId") or request.query_params.get("token_id") or "").strip()
    try:
        dhan_service.consume_consent(token_id)
    except (DhanAuthError, DhanApiError) as exc:
        logger.info("dhan consent callback failed")
        return HTMLResponse(_PAGE.format(message=str(exc)), status_code=400)
    except Exception:
        logger.exception("dhan consent callback failed")
        return HTMLResponse(
            _PAGE.format(message="Dhan login did not finish. Return to Commodities Div and try Sync frm Broker again."),
            status_code=400,
        )
    return HTMLResponse(
        _PAGE.format(
            message="Dhan is connected. You can close this tab and use Sync frm Broker on Commodities Div."
        )
    )


@router.api_route("/webhook/dhan/postback", methods=["GET", "POST"])
async def dhan_postback() -> JSONResponse:
    """Ack order-update postbacks. Order placement is not handled here."""
    return JSONResponse({"ok": True})

"""Crypto Options Report API — paired Delta India BTC straddles."""
from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from backend.database import get_db
from backend.models.user import User
from backend.routers.auth import get_user_from_token, oauth2_scheme
from backend.services.crypto_options_report.job import run_crypto_options_refresh
from backend.services.crypto_options_report.store import list_straddles

logger = logging.getLogger(__name__)

router = APIRouter(tags=["crypto-options-report"])


def _auth_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)) -> User:
    return get_user_from_token(token, db)


def _parse_date(value: Optional[str], field: str) -> Optional[date]:
    if value is None or not str(value).strip():
        return None
    try:
        return datetime.strptime(str(value).strip()[:10], "%Y-%m-%d").date()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"{field} must be YYYY-MM-DD") from exc


def _summary(rows: list) -> dict:
    currencies = [r.get("quote_currency") for r in rows if r.get("quote_currency")]
    unique = set(currencies)
    currency = currencies[0] if len(unique) == 1 else None
    pnls = [r["pnl"] for r in rows if r.get("pnl") is not None]
    return {
        "total_trades": len(rows),
        "open_trades": sum(1 for r in rows if r.get("status") == "OPEN"),
        "closed_trades": sum(1 for r in rows if r.get("status") == "CLOSED"),
        "total_pnl": sum(pnls) if pnls else 0.0,
        "quote_currency": currency,
    }


@router.get("/crypto-options/report")
async def crypto_options_report(
    start_date: Optional[str] = Query(None, description="Session date YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="Session date YYYY-MM-DD"),
    _user: User = Depends(_auth_user),
) -> JSONResponse:
    start = _parse_date(start_date, "start_date")
    end = _parse_date(end_date, "end_date")
    try:
        rows = list_straddles(start, end)
    except Exception as exc:
        logger.exception("crypto options report read failed")
        raise HTTPException(status_code=503, detail="crypto options report unavailable") from exc
    summary = _summary(rows)
    return JSONResponse(
        status_code=200,
        content={"success": True, "summary": summary, "data": rows, "quote_currency": summary.get("quote_currency")},
    )


@router.post("/crypto-options/refresh")
async def crypto_options_refresh(_user: User = Depends(_auth_user)) -> JSONResponse:
    """Pull latest fills and marks. Does not place orders."""
    try:
        result = run_crypto_options_refresh(trigger="api")
    except Exception as exc:
        logger.exception("crypto options report refresh failed")
        raise HTTPException(status_code=503, detail="crypto options refresh failed") from exc
    missing = result.get("missing_env") or []
    if missing:
        raise HTTPException(
            status_code=503,
            detail="missing env: " + ", ".join(missing),
        )
    if not result.get("ok"):
        raise HTTPException(status_code=503, detail=str(result.get("error") or "refresh failed"))
    return JSONResponse(status_code=200, content={"success": True, **result})

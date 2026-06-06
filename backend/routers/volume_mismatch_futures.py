"""Volume Mismatch Futures API."""
from __future__ import annotations

import logging
import math
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from backend.database import get_db
from backend.models.user import User
from backend.routers.auth import get_user_from_token, oauth2_scheme
from backend.services.smart_futures_session_date import effective_session_date_ist_for_trend
from backend.services.volume_mismatch.job import (
    run_volume_mismatch_daily_scan_job,
    run_volume_mismatch_monitor_job,
)
from backend.services.volume_mismatch.repository import (
    fetch_scan_meta,
    fetch_signals_for_date,
    mark_triggered,
)
from backend.services.volume_mismatch.scanner import run_volume_mismatch_scan
from backend.services.volume_mismatch.tables import ensure_volume_mismatch_signals_table
from backend.services.volume_mismatch.universe import load_volume_mismatch_universe

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/volume-mismatch-futures", tags=["volume-mismatch-futures"])


def _require_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)) -> User:
    return get_user_from_token(token, db)


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    return str(value)


def _serialize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    out = _json_safe(row)
    out["instrument_key"] = row.get("instrument_token") or row.get("instrument_key")
    out["preferred_entry"] = row.get("preferred_entry") or row.get("entry_price")
    return out


@router.get("/signals")
def get_signals(
    trade_date: Optional[date] = Query(None),
    direction: Optional[str] = Query(None, description="LONG or SHORT"),
    entry_status: Optional[str] = Query(None, description="WAITING|READY|TRIGGERED|EXPIRED"),
    min_score: Optional[float] = Query(None, ge=0, le=100),
    user: User = Depends(_require_user),
    db: Session = Depends(get_db),
):
    del user
    sd = trade_date or effective_session_date_ist_for_trend()
    ensure_volume_mismatch_signals_table(db)
    rows = fetch_signals_for_date(
        db,
        sd,
        direction=direction,
        entry_status=entry_status,
        min_score=min_score,
    )
    meta = fetch_scan_meta(db, sd)
    long_rows = [r for r in rows if str(r.get("direction")).upper() == "LONG"]
    short_rows = [r for r in rows if str(r.get("direction")).upper() == "SHORT"]
    return JSONResponse(
        status_code=200,
        content={
            "success": True,
            "trade_date": sd.isoformat(),
            "universe_count": len(load_volume_mismatch_universe()),
            "signal_count": len(rows),
            "long_count": len(long_rows),
            "short_count": len(short_rows),
            "last_updated": _json_safe(meta.get("last_updated")),
            "rows": [_serialize_row(r) for r in rows],
            "long_rows": [_serialize_row(r) for r in long_rows],
            "short_rows": [_serialize_row(r) for r in short_rows],
        },
    )


@router.get("/status")
def get_status(
    trade_date: Optional[date] = Query(None),
    user: User = Depends(_require_user),
    db: Session = Depends(get_db),
):
    del user
    sd = trade_date or effective_session_date_ist_for_trend()
    ensure_volume_mismatch_signals_table(db)
    meta = fetch_scan_meta(db, sd)
    return JSONResponse(
        status_code=200,
        content={
            "success": True,
            "trade_date": sd.isoformat(),
            "signal_count": meta.get("signal_count"),
            "last_updated": _json_safe(meta.get("last_updated")),
        },
    )


class EnterBody(BaseModel):
    signal_id: int = Field(..., ge=1)


@router.post("/enter")
def confirm_enter(
    body: EnterBody,
    user: User = Depends(_require_user),
    db: Session = Depends(get_db),
):
    del user
    ensure_volume_mismatch_signals_table(db)
    row = mark_triggered(db, body.signal_id)
    if not row:
        return JSONResponse(
            status_code=400,
            content={"success": False, "error": "Signal not READY or not found"},
        )
    db.commit()
    return JSONResponse(
        status_code=200,
        content={"success": True, "signal": _serialize_row(row)},
    )


@router.post("/scan")
def manual_scan(
    user: User = Depends(_require_user),
    db: Session = Depends(get_db),
):
    del user, db
    result = run_volume_mismatch_scan()
    return JSONResponse(status_code=200, content=_json_safe(result))


@router.post("/monitor")
def manual_monitor(
    user: User = Depends(_require_user),
    db: Session = Depends(get_db),
):
    del user, db
    result = run_volume_mismatch_monitor_job()
    return JSONResponse(status_code=200, content=_json_safe(result))

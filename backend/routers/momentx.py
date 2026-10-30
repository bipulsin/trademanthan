"""MomentX API — Ready list, trade log, chart data, scan triggers."""
from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any, Dict, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, Query
from fastapi.responses import Response
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from backend.database import get_db
from backend.models.user import User
from backend.routers.auth import get_user_from_token, oauth2_scheme
from backend.services.momentx.service import get_ready_payload, take_trade_log

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/momentx", tags=["momentx"])


def _auth_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)) -> User:
    return get_user_from_token(token, db)


def _parse_date(s: Optional[str]) -> Optional[date]:
    if not s:
        return None
    try:
        return date.fromisoformat(str(s)[:10])
    except Exception:
        return None


class TakeTradeBody(BaseModel):
    stock: str = Field(..., min_length=1)
    future_symbol: Optional[str] = None
    future_instrument_key: Optional[str] = None
    lot_size: Optional[int] = None
    ltp: Optional[float] = None
    entry_price: Optional[float] = None
    entry_time: Optional[str] = None
    exit_price: Optional[float] = None
    exit_time: Optional[str] = None
    ema5: Optional[float] = None
    ema15: Optional[float] = None
    ema50: Optional[float] = None


class EditTradeBody(BaseModel):
    entry_price: Optional[float] = None
    entry_time: Optional[str] = None
    exit_price: Optional[float] = None
    exit_time: Optional[str] = None


@router.get("/ready")
def ready(_user: User = Depends(_auth_user)) -> Dict[str, Any]:
    """Current Ready-to-Trade cards with LTP, EMAs, trigger flag, Stage-1 values."""
    return get_ready_payload(ensure=True)


@router.post("/scan")
def trigger_scan(
    background_tasks: BackgroundTasks,
    _user: User = Depends(_auth_user),
) -> Dict[str, Any]:
    def _run() -> None:
        from backend.services.momentx.scanner import run_stage1_scan

        run_stage1_scan(force=False)

    background_tasks.add_task(_run)
    return {"ok": True, "queued": True}


@router.post("/refresh-ema")
def trigger_ema(
    background_tasks: BackgroundTasks,
    _user: User = Depends(_auth_user),
) -> Dict[str, Any]:
    def _run() -> None:
        from backend.services.momentx.tracker import refresh_emas

        refresh_emas(force=True)

    background_tasks.add_task(_run)
    return {"ok": True, "queued": True}


@router.post("/take-trade")
def take_trade(body: TakeTradeBody, _user: User = Depends(_auth_user)) -> Dict[str, Any]:
    """Confirm Take Trade — logs entry/exit into momentx day log (no broker order)."""
    return take_trade_log(body.model_dump())


@router.get("/log")
def trade_log(
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    _user: User = Depends(_auth_user),
) -> Dict[str, Any]:
    from backend.services.momentx.persistence import fetch_log

    return fetch_log(date_from=_parse_date(date_from), date_to=_parse_date(date_to))


@router.get("/log.csv")
def trade_log_csv(
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    _user: User = Depends(_auth_user),
) -> Response:
    from backend.services.momentx.persistence import log_csv

    fname, body = log_csv(date_from=_parse_date(date_from), date_to=_parse_date(date_to))
    return Response(
        content=body,
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )


@router.patch("/log/{row_id}")
def edit_trade_log(
    row_id: int,
    body: EditTradeBody,
    _user: User = Depends(_auth_user),
) -> Dict[str, Any]:
    from backend.services.momentx.persistence import update_trade_fields

    return update_trade_fields(row_id, body.model_dump(exclude_unset=True))


@router.get("/chart")
def chart_data(
    instrument_key: str = Query(..., min_length=3),
    interval: str = Query("minutes/5"),
    days_back: int = Query(10, ge=2, le=60),
    _user: User = Depends(_auth_user),
) -> Dict[str, Any]:
    from backend.services.momentx.chart_data import build_chart_payload

    return build_chart_payload(
        instrument_key=instrument_key,
        interval=interval,
        days_back=int(days_back),
    )


class BacktestRunBody(BaseModel):
    date_from: str = Field(..., min_length=8, description="YYYY-MM-DD")
    date_to: str = Field(..., min_length=8, description="YYYY-MM-DD")
    asof_hhmm: str = Field("14:45", description="IST as-of clock for Stage-1 eval")
    replace_range: bool = Field(True, description="Clear overlapping qualified rows first")


@router.post("/backtest/run")
def backtest_run(body: BacktestRunBody, _user: User = Depends(_auth_user)) -> Dict[str, Any]:
    """Queue a Stage-1 backtest for an arbitrary date/month range (background)."""
    d0 = _parse_date(body.date_from)
    d1 = _parse_date(body.date_to)
    if not d0 or not d1:
        return {"ok": False, "error": "invalid date_from/date_to"}
    from backend.services.momentx.backtest_engine import start_backtest_async

    return start_backtest_async(
        date_from=d0,
        date_to=d1,
        asof_hhmm=body.asof_hhmm or "14:45",
        replace_range=bool(body.replace_range),
    )


@router.get("/backtest/status")
def backtest_status(
    run_id: Optional[int] = Query(None),
    _user: User = Depends(_auth_user),
) -> Dict[str, Any]:
    from backend.services.momentx.backtest_engine import active_run_id
    from backend.services.momentx.backtest_store import fetch_run, fetch_runs, latest_done_run_id

    rid = run_id if run_id is not None else active_run_id() or latest_done_run_id()
    run = fetch_run(int(rid)) if rid is not None else None
    return {
        "ok": True,
        "active_run_id": active_run_id(),
        "run": run,
        "runs": fetch_runs(limit=10),
    }


@router.get("/backtest/results")
def backtest_results(
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    run_id: Optional[int] = Query(None),
    _user: User = Depends(_auth_user),
) -> Dict[str, Any]:
    """Qualified stocks grouped by trading day (latest done run unless run_id set)."""
    from backend.services.momentx.backtest_store import fetch_qualified

    return fetch_qualified(
        date_from=_parse_date(date_from),
        date_to=_parse_date(date_to),
        run_id=run_id,
    )

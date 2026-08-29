"""Breakfast Strategy API."""
from __future__ import annotations

import json
import logging
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from backend.services.breakfast_strategy.backtest import (
    apply_cap_variant,
    find_artifact,
    run_backtest,
    run_forward_today,
)
from backend.services.breakfast_strategy.config import DATE_FROM, DATE_TO, OOS_SPOT_ARTIFACT_NAME
from backend.services.breakfast_strategy.history import load_history
from backend.services.breakfast_strategy.live import build_live_state, validate_ws_vs_rest
from backend.services.breakfast_strategy.live_persist import fetch_live_signals, update_manual_capture
from backend.services.breakfast_strategy.persist import fetch_trades

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/breakfast-strategy", tags=["breakfast-strategy"])


class LiveSignalManualPatch(BaseModel):
    direction: str = Field(..., description="LONG or SHORT")
    manual_entry_price: Optional[float] = None
    manual_entry_time: Optional[str] = None
    manual_exit_price: Optional[float] = None
    manual_exit_time: Optional[str] = None
    manual_note: Optional[str] = None


def _load_artifact(basename: Optional[str] = None) -> Dict[str, Any]:
    path = find_artifact(basename)
    if not path:
        raise HTTPException(
            status_code=503,
            detail="Breakfast backtest artifact not found. Run scripts/run_breakfast_backtest.py",
        )
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not read artifact: {e}") from e


@router.get("/data")
def get_breakfast_data(
    pnl_cap_enabled: bool = Query(False),
    dataset: Optional[str] = Query(None, description="primary (default) or oos_spot"),
) -> Dict[str, Any]:
    basename = OOS_SPOT_ARTIFACT_NAME if dataset == "oos_spot" else None
    doc = _load_artifact(basename)
    out = apply_cap_variant(doc, pnl_cap_enabled)
    if dataset == "oos_spot":
        out["dataset"] = "oos_spot"
        out["comparability_caveat"] = (
            "Spot-proxy OOS run: cash prices used where futures history is unavailable; "
            "P&L sized at futures-equivalent lot quantity. Not directly comparable to the "
            "Jul–Aug futures-priced primary dataset (basis + VWAP-on-spot vs VWAP-on-futures)."
        )
    return out


@router.get("/trades")
def list_breakfast_trades(
    mode: Optional[str] = Query(None),
    start_date: Optional[date] = Query(None),
    end_date: Optional[date] = Query(None),
) -> Dict[str, Any]:
    rows = fetch_trades(
        mode=mode,
        start_date=start_date.isoformat() if start_date else None,
        end_date=end_date.isoformat() if end_date else None,
    )
    return {"ok": True, "count": len(rows), "trades": rows}


@router.get("/summary")
def breakfast_summary() -> Dict[str, Any]:
    try:
        doc = _load_artifact()
        return {"ok": True, "summary": doc.get("summary") or {}, "date_from": doc.get("date_from"), "date_to": doc.get("date_to")}
    except HTTPException:
        rows = fetch_trades(mode="backtest", start_date=DATE_FROM.isoformat(), end_date=DATE_TO.isoformat())
        from backend.services.breakfast_strategy.backtest import _summary

        return {"ok": True, "summary": _summary(rows), "source": "db"}


@router.post("/run-backtest")
def run_backtest_now(
    force_fetch: bool = Query(False),
    pnl_cap_enabled: bool = Query(False),
) -> JSONResponse:
    try:
        out = run_backtest(
            force_fetch=force_fetch,
            persist_db=True,
            pnl_cap_enabled=pnl_cap_enabled,
        )
        return JSONResponse(status_code=200, content={"ok": True, **out})
    except Exception as e:
        logger.exception("breakfast backtest: %s", e)
        return JSONResponse(status_code=500, content={"ok": False, "message": str(e)})


@router.post("/run-forward")
def run_forward_now() -> JSONResponse:
    try:
        out = run_forward_today(persist_db=True)
        return JSONResponse(status_code=200, content={"ok": True, **out})
    except Exception as e:
        logger.exception("breakfast forward: %s", e)
        return JSONResponse(status_code=500, content={"ok": False, "message": str(e)})


@router.get("/history")
def get_breakfast_history() -> Dict[str, Any]:
    doc = load_history()
    if not doc:
        raise HTTPException(
            status_code=503,
            detail="History artifact not found. Run scripts/run_breakfast_history_rolling.py",
        )
    # Light payload for tab: omit full trade lists unless ?include_trades=1
    out = dict(doc)
    months_light = []
    for m in doc.get("months") or []:
        ml = {k: v for k, v in m.items() if k != "trades"}
        ml["trade_count"] = len(m.get("trades") or [])
        months_light.append(ml)
    out["months"] = months_light
    return out


@router.get("/history/{period_label}/trades")
def get_history_month_trades(period_label: str) -> Dict[str, Any]:
    doc = load_history()
    for m in doc.get("months") or []:
        if m.get("period_label") == period_label:
            return {
                "ok": True,
                "period_label": period_label,
                "trades": m.get("trades") or [],
                "summary": m.get("summary") or {},
            }
    raise HTTPException(status_code=404, detail=f"Period {period_label} not found in history")


@router.get("/live")
def get_breakfast_live(
    replay_at: Optional[str] = Query(
        None,
        description="ISO timestamp for dry-run replay (IST assumed if no TZ)",
    ),
) -> Dict[str, Any]:
    replay_dt: Optional[datetime] = None
    if replay_at:
        try:
            replay_dt = datetime.fromisoformat(replay_at.replace("Z", "+00:00"))
        except ValueError as e:
            raise HTTPException(status_code=400, detail=f"Invalid replay_at: {e}") from e
    return build_live_state(replay_at=replay_dt)


@router.get("/live/validate")
def breakfast_live_validate(
    instrument_key: str = Query(..., description="Upstox instrument_key"),
    session_date: Optional[date] = Query(None),
) -> Dict[str, Any]:
    sd = session_date or date.today()
    return validate_ws_vs_rest(instrument_key, sd)


@router.get("/live/signals")
def get_live_signals(
    session_date: Optional[date] = Query(None, description="Session date (default today IST)"),
) -> Dict[str, Any]:
    sd = session_date or date.today()
    rows = fetch_live_signals(sd.isoformat())
    return {"ok": True, "session_date": sd.isoformat(), "count": len(rows), "signals": rows}


@router.patch("/live/signals/{session_date}/{symbol}")
def patch_live_signal_manual(
    session_date: date,
    symbol: str,
    body: LiveSignalManualPatch,
) -> Dict[str, Any]:
    direction = str(body.direction or "").strip().upper()
    if direction not in ("LONG", "SHORT"):
        raise HTTPException(status_code=400, detail="direction must be LONG or SHORT")
    fields = body.model_dump(exclude_unset=True, exclude={"direction"})
    if not fields:
        raise HTTPException(status_code=400, detail="No manual fields provided")
    updated = update_manual_capture(session_date.isoformat(), symbol, direction, fields)
    if not updated:
        raise HTTPException(
            status_code=404,
            detail=f"No live signal for {session_date.isoformat()} {symbol} {direction}",
        )
    return {"ok": True, "signal": updated}


@router.get("/health")
def breakfast_health() -> Dict[str, str]:
    path = find_artifact()
    return {"status": "ok" if path else "missing", "artifact": str(path or "")}

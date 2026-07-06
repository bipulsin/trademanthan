"""
Public BTST stock-options backtest API.

No authentication — page at ``/btst-backtest.html``.
"""
from __future__ import annotations

import logging
import threading
from datetime import date
from typing import Any, Dict, Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query
from pydantic import BaseModel, Field

from backend.services.btst_backtest.exit_manager import recalc_pnls
from backend.services.btst_backtest.repository import (
    compute_summary,
    fetch_latest_run,
    fetch_result,
    update_manual_fill,
)
from backend.services.btst_backtest.runner import run_btst_backtest

logger = logging.getLogger(__name__)

router = APIRouter(tags=["btst-backtest"])

_run_lock = threading.Lock()
_run_status: Dict[str, Any] = {"running": False, "run_id": None, "error": None}


def _jsonify_row(r: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(r)
    for k in ("trade_date",):
        v = out.get(k)
        if hasattr(v, "isoformat"):
            out[k] = v.isoformat()
    for k in (
        "entry_time",
        "exit_a_time",
        "exit_b_time",
        "run_date",
        "start_date",
        "end_date",
    ):
        v = out.get(k)
        if v is not None and hasattr(v, "isoformat"):
            out[k] = v.isoformat()
    for k, v in list(out.items()):
        if hasattr(v, "__float__") and not isinstance(v, (bool, int)):
            try:
                out[k] = float(v)
            except (TypeError, ValueError):
                pass
    return out


def _run_job(trading_days: int, end_date: Optional[date], notes: str) -> None:
    global _run_status
    try:
        result = run_btst_backtest(trading_days=trading_days, end_date=end_date, notes=notes)
        if result.get("error"):
            _run_status = {"running": False, "run_id": result.get("run_id"), "error": result["error"]}
        else:
            _run_status = {"running": False, "run_id": result.get("run_id"), "error": None}
    except Exception as exc:
        logger.exception("btst backtest failed")
        _run_status = {"running": False, "run_id": None, "error": str(exc)}


@router.get("/status")
def backtest_status() -> Dict[str, Any]:
    return dict(_run_status)


@router.post("/run")
def start_backtest(
    background_tasks: BackgroundTasks,
    days: int = Query(30, ge=1, le=60),
    end_date: Optional[date] = None,
    notes: str = "",
) -> Dict[str, Any]:
    with _run_lock:
        if _run_status.get("running"):
            raise HTTPException(status_code=409, detail="Backtest already running")
        _run_status.update({"running": True, "run_id": None, "error": None})
    background_tasks.add_task(_run_job, days, end_date, notes)
    return {"started": True, "days": days}


@router.get("/latest")
def latest_results() -> Dict[str, Any]:
    doc = fetch_latest_run()
    if not doc:
        raise HTTPException(status_code=404, detail="No backtest runs yet")
    rows = [_jsonify_row(r) for r in doc["rows"]]
    run = _jsonify_row(doc["run"])
    summary = compute_summary(doc["rows"])
    return {"run": run, "rows": rows, "summary": summary}


class ManualFillBody(BaseModel):
    entry_premium: Optional[float] = None
    exit_a_premium: Optional[float] = None
    exit_b_premium: Optional[float] = None


@router.patch("/results/{result_id}")
def patch_manual_fill(result_id: int, body: ManualFillBody) -> Dict[str, Any]:
    row = fetch_result(result_id)
    if not row:
        raise HTTPException(status_code=404, detail="Result row not found")
    entry = body.entry_premium if body.entry_premium is not None else row.get("entry_premium")
    exit_a = body.exit_a_premium if body.exit_a_premium is not None else row.get("exit_a_premium")
    exit_b = body.exit_b_premium if body.exit_b_premium is not None else row.get("exit_b_premium")
    lot = row.get("lot_size")
    pnls = recalc_pnls(
        float(entry) if entry is not None else None,
        float(exit_a) if exit_a is not None else None,
        float(exit_b) if exit_b is not None else None,
        int(lot) if lot is not None else None,
    )
    updates = {}
    if body.entry_premium is not None:
        updates["entry_premium"] = body.entry_premium
    if body.exit_a_premium is not None:
        updates["exit_a_premium"] = body.exit_a_premium
    if body.exit_b_premium is not None:
        updates["exit_b_premium"] = body.exit_b_premium
    updates.update(pnls)
    updated = update_manual_fill(result_id, updates)
    doc = fetch_latest_run()
    summary = compute_summary(doc["rows"]) if doc else {}
    return {"row": _jsonify_row(updated), "summary": summary}

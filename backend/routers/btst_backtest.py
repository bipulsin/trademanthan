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
from pydantic import BaseModel

from backend.services.btst_backtest.exit_manager import recalc_pnls
from backend.services.btst_backtest.repository import (
    compute_summary,
    fetch_all_results,
    fetch_earliest_trade_date,
    fetch_failed_row_keys,
    fetch_latest_run_meta,
    fetch_result,
    update_manual_fill,
)
from backend.services.btst_backtest.runner import run_btst_backtest, run_btst_retry_failed

logger = logging.getLogger(__name__)

router = APIRouter(tags=["btst-backtest"])

_run_lock = threading.Lock()
_run_status: Dict[str, Any] = {"running": False, "run_id": None, "error": None, "mode": None}


def _jsonify_row(r: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(r)
    for k in ("trade_date", "start_date", "end_date", "run_date"):
        v = out.get(k)
        if v is not None and hasattr(v, "isoformat"):
            out[k] = v.isoformat()
    for k in ("entry_time", "exit_a_time", "exit_b_time"):
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


def _run_job(mode: str, trading_days: int, end_date: Optional[date], notes: str) -> None:
    global _run_status
    try:
        if mode == "retry":
            result = run_btst_retry_failed(notes=notes)
        elif mode == "earlier":
            result = run_btst_backtest(
                trading_days=trading_days, end_date=end_date, notes=notes, mode="earlier"
            )
        else:
            result = run_btst_backtest(
                trading_days=trading_days, end_date=end_date, notes=notes, mode="recent"
            )
        if result.get("error") and not result.get("result_ids"):
            _run_status = {
                "running": False,
                "run_id": result.get("run_id"),
                "error": result["error"],
                "mode": mode,
            }
        else:
            _run_status = {
                "running": False,
                "run_id": result.get("run_id"),
                "error": result.get("error"),
                "mode": mode,
            }
    except Exception as exc:
        logger.exception("btst backtest failed")
        _run_status = {"running": False, "run_id": None, "error": str(exc), "mode": mode}


def _start_job(mode: str, trading_days: int, end_date: Optional[date], notes: str) -> Dict[str, Any]:
    with _run_lock:
        if _run_status.get("running"):
            raise HTTPException(status_code=409, detail="Backtest already running")
        _run_status.update({"running": True, "run_id": None, "error": None, "mode": mode})
    return {"started": True, "days": trading_days, "mode": mode}


@router.get("/status")
def backtest_status() -> Dict[str, Any]:
    out = dict(_run_status)
    out["failed_row_count"] = len(fetch_failed_row_keys())
    out["earliest_trade_date"] = (
        fetch_earliest_trade_date().isoformat() if fetch_earliest_trade_date() else None
    )
    return out


@router.post("/run")
def start_backtest(
    background_tasks: BackgroundTasks,
    days: int = Query(15, ge=1, le=60),
    end_date: Optional[date] = None,
    notes: str = "",
) -> Dict[str, Any]:
    out = _start_job("recent", days, end_date, notes)
    background_tasks.add_task(_run_job, "recent", days, end_date, notes)
    return out


@router.post("/run-earlier")
def start_earlier_backtest(
    background_tasks: BackgroundTasks,
    days: int = Query(15, ge=1, le=60),
    notes: str = "",
) -> Dict[str, Any]:
    if fetch_earliest_trade_date() is None:
        raise HTTPException(status_code=400, detail="No existing rows — run recent days first")
    out = _start_job("earlier", days, None, notes)
    background_tasks.add_task(_run_job, "earlier", days, None, notes)
    return out


@router.post("/retry-failed")
def retry_failed_rows(
    background_tasks: BackgroundTasks,
    notes: str = "retry_failed",
) -> Dict[str, Any]:
    if not fetch_failed_row_keys():
        raise HTTPException(status_code=400, detail="No api_fetch_failed rows to retry")
    out = _start_job("retry", 0, None, notes)
    background_tasks.add_task(_run_job, "retry", 0, None, notes)
    return out


@router.get("/latest")
def latest_results() -> Dict[str, Any]:
    rows = fetch_all_results()
    if not rows:
        raise HTTPException(status_code=404, detail="No backtest results yet")
    run = fetch_latest_run_meta()
    summary = compute_summary(rows)
    return {
        "run": _jsonify_row(run) if run else None,
        "rows": [_jsonify_row(r) for r in rows],
        "summary": summary,
        "earliest_trade_date": (
            fetch_earliest_trade_date().isoformat() if fetch_earliest_trade_date() else None
        ),
    }


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
    updates: Dict[str, Any] = {}
    if body.entry_premium is not None:
        updates["entry_premium"] = body.entry_premium
    if body.exit_a_premium is not None:
        updates["exit_a_premium"] = body.exit_a_premium
    if body.exit_b_premium is not None:
        updates["exit_b_premium"] = body.exit_b_premium
    updates.update(pnls)
    updated = update_manual_fill(result_id, updates)
    all_rows = fetch_all_results()
    summary = compute_summary(all_rows)
    return {"row": _jsonify_row(updated), "summary": summary}

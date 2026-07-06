"""
Public BTST stock-options backtest API (CSV-fed).

No authentication — page at ``/btst-backtest.html``.
"""
from __future__ import annotations

import logging
import threading
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, BackgroundTasks, File, HTTPException, UploadFile
from pydantic import BaseModel

from backend.services.btst_backtest import progress as btst_progress
from backend.services.btst_backtest.csv_import import parse_btst_csv
from backend.services.btst_backtest.exit_manager import recalc_pnls
from backend.services.btst_backtest.repository import (
    compute_summary,
    count_results_for_run,
    fetch_all_results,
    fetch_latest_run_meta,
    fetch_result,
    update_manual_fill,
)
from backend.services.btst_backtest.runner import run_csv_backtest

logger = logging.getLogger(__name__)

router = APIRouter(tags=["btst-backtest"])

_run_lock = threading.Lock()
_run_status: Dict[str, Any] = {"running": False, "run_id": None, "error": None}
_pending_csv: Dict[str, Any] = {"rows": [], "filename": "", "warnings": []}


def _jsonify_row(r: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(r)
    for k in ("trade_date", "run_date"):
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


def _run_job(csv_rows: List[Dict[str, Any]], csv_filename: str, notes: str) -> None:
    global _run_status
    try:
        btst_progress.reset_for_job(len(csv_rows), csv_filename)
        result = run_csv_backtest(csv_rows, csv_filename=csv_filename, notes=notes)
        if result.get("error"):
            _run_status = {"running": False, "run_id": result.get("run_id"), "error": result["error"]}
        else:
            _run_status = {
                "running": False,
                "run_id": result.get("run_id"),
                "error": None,
            }
    except Exception as exc:
        logger.exception("btst csv backtest failed")
        _run_status = {"running": False, "run_id": None, "error": str(exc)}
    finally:
        btst_progress.set_idle()


@router.post("/upload")
async def upload_csv(file: UploadFile = File(...)) -> Dict[str, Any]:
    """Parse CSV and stage rows for the next Run backtest click."""
    global _pending_csv
    raw = await file.read()
    if not raw:
        raise HTTPException(status_code=400, detail="Empty file")
    try:
        rows, warnings = parse_btst_csv(raw)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    with _run_lock:
        _pending_csv = {
            "rows": rows,
            "filename": file.filename or "upload.csv",
            "warnings": warnings,
        }
    return {
        "ok": True,
        "filename": _pending_csv["filename"],
        "row_count": len(rows),
        "warnings": warnings,
        "preview": [
            {
                "trade_date": r["trade_date"].isoformat(),
                "stock_symbol": r["stock_symbol"],
                "sector": r.get("sector"),
            }
            for r in rows[:5]
        ],
    }


@router.get("/pending")
def pending_csv() -> Dict[str, Any]:
    with _run_lock:
        rows = _pending_csv.get("rows") or []
        return {
            "filename": _pending_csv.get("filename") or "",
            "row_count": len(rows),
            "warnings": _pending_csv.get("warnings") or [],
        }


@router.post("/run")
def start_backtest(
    background_tasks: BackgroundTasks,
    notes: str = "",
) -> Dict[str, Any]:
    with _run_lock:
        if _run_status.get("running"):
            raise HTTPException(status_code=409, detail="Backtest already running")
        rows = list(_pending_csv.get("rows") or [])
        if not rows:
            raise HTTPException(status_code=400, detail="Upload a CSV first")
        filename = _pending_csv.get("filename") or "upload.csv"
        _run_status.update({"running": True, "run_id": None, "error": None})
    background_tasks.add_task(_run_job, rows, filename, notes)
    return {"started": True, "row_count": len(rows), "filename": filename}


@router.get("/status")
def backtest_status() -> Dict[str, Any]:
    from backend.services.upstox_rate_limiter import stats as rl_stats

    out = dict(_run_status)
    prog = btst_progress.snapshot()
    out["progress"] = prog
    active_run_id = prog.get("active_run_id")
    out["active_run_id"] = active_run_id
    out["rows_written_this_run"] = (
        count_results_for_run(int(active_run_id)) if active_run_id else 0
    )
    started = prog.get("started_at")
    if started and out.get("running"):
        try:
            t0 = datetime.fromisoformat(str(started).replace("Z", "+00:00"))
            out["elapsed_sec"] = int(
                (datetime.now(timezone.utc) - t0.astimezone(timezone.utc)).total_seconds()
            )
        except (TypeError, ValueError):
            pass
    if out.get("running"):
        out["candle_rate_limiter"] = rl_stats()
    with _run_lock:
        out["pending_csv_rows"] = len(_pending_csv.get("rows") or [])
        out["pending_csv_filename"] = _pending_csv.get("filename") or ""
    return out


@router.get("/latest")
def latest_results() -> Dict[str, Any]:
    rows = fetch_all_results()
    if not rows:
        raise HTTPException(status_code=404, detail="No backtest results yet")
    run = fetch_latest_run_meta()
    return {
        "run": _jsonify_row(run) if run else None,
        "rows": [_jsonify_row(r) for r in rows],
        "summary": compute_summary(rows),
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
    return {"row": _jsonify_row(updated), "summary": compute_summary(all_rows)}

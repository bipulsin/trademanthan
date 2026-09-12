"""FastAPI routes for divtest (MACD divergence backtester)."""
from __future__ import annotations

import asyncio
import json
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from backend.services.divtest.config import get_settings, settings_public, update_settings
from backend.services.divtest.instruments import ensure_instrument_master, resolve_instrument
from backend.services.divtest.results_store import get_results_summary, list_instruments
from backend.services.divtest.runner import get_job, start_backtest

router = APIRouter(tags=["divtest"])


class RunBody(BaseModel):
    instruments: str
    period_months: int = 6
    force_refresh: bool = False
    settings: Optional[Dict[str, Any]] = None


class SettingsBody(BaseModel):
    settings: Dict[str, Any] = Field(default_factory=dict)


@router.get("/health")
def health() -> Dict[str, Any]:
    cfg = get_settings()
    has_token = False
    try:
        from backend.config import settings
        from backend.services.upstox_service import UpstoxService

        ux = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
        if hasattr(ux, "reload_token_from_storage"):
            ux.reload_token_from_storage()
        has_token = bool(getattr(ux, "access_token", None))
    except Exception:
        has_token = False
    return {
        "ok": True,
        "service": "divtest",
        "demoMode": bool(cfg.get("demo_mode")),
        "hasToken": has_token,
    }


@router.get("/settings")
def read_settings() -> Dict[str, Any]:
    return settings_public()


@router.post("/settings")
def write_settings(body: SettingsBody) -> Dict[str, Any]:
    updated = update_settings(body.settings or {})
    return {"ok": True, "settings": settings_public(updated)}


@router.get("/instruments")
def instruments(q: Optional[str] = Query(None)) -> Dict[str, Any]:
    stored = list_instruments()
    resolved: List[Dict[str, Any]] = []
    cfg = get_settings()
    if q and not cfg.get("demo_mode"):
        try:
            ensure_instrument_master()
        except Exception as exc:
            raise HTTPException(status_code=502, detail=f"Instrument master failed: {exc}") from exc
        for name in [s.strip() for s in q.split(",") if s.strip()]:
            try:
                resolved.append(resolve_instrument(name))
            except Exception as exc:
                resolved.append({"symbol": name.upper(), "found": False, "notes": [str(exc)]})
    elif q and cfg.get("demo_mode"):
        for name in [s.strip() for s in q.split(",") if s.strip()]:
            resolved.append({"symbol": name.upper(), "found": True, "demo": True, "lot_size": 1})
    return {"demoMode": bool(cfg.get("demo_mode")), "stored": stored, "resolved": resolved}


@router.post("/run-backtest")
def run_backtest(body: RunBody) -> Dict[str, Any]:
    try:
        job = start_backtest(
            instruments=body.instruments,
            period_months=body.period_months,
            force_refresh=body.force_refresh,
            settings=body.settings,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {
        "ok": True,
        "jobId": job["id"],
        "status": job["status"],
        "months": job["months"],
        "symbols": job["symbols"],
    }


@router.get("/jobs/{job_id}")
def job_status(job_id: str) -> Dict[str, Any]:
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return {
        "id": job["id"],
        "status": job["status"],
        "progress": job["progress"],
        "partial_trades": job.get("partial_trades", 0),
        "error": job.get("error"),
        "started_at": job.get("started_at"),
        "finished_at": job.get("finished_at"),
        "logs": (job.get("logs") or [])[-200:],
    }


@router.get("/jobs/{job_id}/events")
async def job_events(job_id: str) -> StreamingResponse:
    if not get_job(job_id):
        raise HTTPException(status_code=404, detail="Job not found")

    async def gen():
        last_log = 0
        last_done = -1
        while True:
            job = get_job(job_id)
            if not job:
                yield f"event: error\ndata: {json.dumps({'message': 'Job missing'})}\n\n"
                break
            progress = job.get("progress") or {}
            done = int(progress.get("done") or 0)
            if done != last_done:
                last_done = done
                payload = {
                    **progress,
                    "status": job["status"],
                    "partial_trades": job.get("partial_trades", 0),
                }
                yield f"event: progress\ndata: {json.dumps(payload)}\n\n"
            logs = job.get("logs") or []
            if len(logs) > last_log:
                for entry in logs[last_log:]:
                    yield f"event: log\ndata: {json.dumps(entry)}\n\n"
                last_log = len(logs)
            if job["status"] in ("completed", "failed"):
                if job["status"] == "completed":
                    yield f"event: done\ndata: {json.dumps({'status': 'completed'})}\n\n"
                else:
                    yield f"event: error\ndata: {json.dumps({'message': job.get('error') or 'failed'})}\n\n"
                break
            await asyncio.sleep(0.5)

    return StreamingResponse(gen(), media_type="text/event-stream")


@router.get("/results")
def results(
    instrument: Optional[str] = Query(None),
    timeframe: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=10, le=500),
) -> Dict[str, Any]:
    inst = None if not instrument or instrument == "All" else instrument
    tf = None if not timeframe or timeframe == "All" else timeframe
    summary = get_results_summary(instrument=inst, timeframe=tf)
    trades = summary.get("trades") or []
    start = (page - 1) * page_size
    page_trades = trades[start : start + page_size]
    return {
        **summary,
        "trades": page_trades,
        "pagination": {
            "page": page,
            "pageSize": page_size,
            "total": len(trades),
            "pages": max(1, (len(trades) + page_size - 1) // page_size),
        },
    }

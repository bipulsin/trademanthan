"""Breakfast Strategy API."""
from __future__ import annotations

import json
import logging
from datetime import date
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse

from backend.services.breakfast_strategy.backtest import (
    apply_cap_variant,
    find_artifact,
    run_backtest,
    run_forward_today,
)
from backend.services.breakfast_strategy.config import DATE_FROM, DATE_TO
from backend.services.breakfast_strategy.persist import fetch_trades

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/breakfast-strategy", tags=["breakfast-strategy"])


def _load_artifact() -> Dict[str, Any]:
    path = find_artifact()
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
) -> Dict[str, Any]:
    doc = _load_artifact()
    return apply_cap_variant(doc, pnl_cap_enabled)


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


@router.get("/health")
def breakfast_health() -> Dict[str, str]:
    path = find_artifact()
    return {"status": "ok" if path else "missing", "artifact": str(path or "")}

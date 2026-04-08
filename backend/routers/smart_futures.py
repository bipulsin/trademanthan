"""
Smart Futures API — Renko-based NSE F&O dashboard and orders.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import pytz
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from backend.database import get_db
from backend.models.user import User
from backend.routers.auth import get_user_from_token, oauth2_scheme
from backend.services.smart_futures import pipeline, repository
from backend.services.smart_futures.order_manager import audit, place_entry, place_exit
from backend.services.smart_futures.signal_engine import should_exit_position

logger = logging.getLogger(__name__)
# No prefix here — mounted twice in main.py: /api/smart-futures/* (canonical) and /smart-futures/*
# so nginx configs that strip /api/ (proxy_pass .../) still reach these routes.
router = APIRouter(tags=["smart-futures"])
IST = pytz.timezone("Asia/Kolkata")

# Dashboard: top 3 candidates with score >= this value (inclusive), ORDER BY score DESC.
SMART_FUTURES_MIN_SCORE = 4


def _session_date():
    return datetime.now(IST).date()


def _require_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)) -> User:
    return get_user_from_token(token, db)


def _require_admin(user: User = Depends(_require_user)) -> User:
    if (getattr(user, "is_admin", None) or "").strip() != "Yes":
        raise HTTPException(status_code=403, detail="Administrator only")
    return user


class SmartFuturesConfigUpdate(BaseModel):
    live_enabled: Optional[bool] = None
    position_size: Optional[int] = Field(None, ge=1, le=3)
    partial_exit_enabled: Optional[bool] = None
    brick_atr_period: Optional[int] = Field(None, ge=2, le=99)
    brick_atr_override: Optional[float] = None


class OrderBody(BaseModel):
    instrument_key: str
    direction: str  # LONG | SHORT
    symbol: str = ""


class ExitBody(BaseModel):
    position_id: int


@router.get("/config")
def get_sf_config_public(user: User = Depends(_require_user)):
    """Live flag + position size for UI (all authenticated users)."""
    return repository.get_config()


@router.put("/config")
def put_sf_config(body: SmartFuturesConfigUpdate, admin: User = Depends(_require_admin)):
    patch = body.model_dump(exclude_unset=True)
    return repository.merge_config(patch)


@router.get("/dashboard/top")
def get_dashboard_top(user: User = Depends(_require_user)):
    """Fast payload: config + top 3 by score (UI loads this first)."""
    d0 = _session_date()
    ms = SMART_FUTURES_MIN_SCORE
    cfg, candidates = repository.load_dashboard_top(d0, ms)
    return {
        "config": cfg,
        "session_date": str(d0),
        "min_score": ms,
        "candidates": candidates,
    }


@router.get("/dashboard/positions")
def get_dashboard_positions(user: User = Depends(_require_user)):
    """Open positions + exit_ready (UI loads after /dashboard/top)."""
    d0 = _session_date()
    return {"positions": repository.load_open_positions_with_exit(d0)}


@router.post("/order")
def post_order(body: OrderBody, user: User = Depends(_require_user)):
    cfg = repository.get_config()
    if not cfg.get("live_enabled"):
        raise HTTPException(status_code=400, detail="Live trading is disabled (Admin: set Live = Yes)")
    d0 = _session_date()
    cands = repository.get_top_candidates_min_score(d0, SMART_FUTURES_MIN_SCORE, limit=50)
    match = next(
        (
            c
            for c in cands
            if c["instrument_key"] == body.instrument_key
            and c.get("direction") == body.direction
            and c.get("entry_signal")
        ),
        None,
    )
    if not match:
        raise HTTPException(status_code=400, detail="No active entry signal for this instrument/direction")
    qty = int(cfg.get("position_size") or 1)
    mb = float(match.get("main_brick_size") or 0.0)
    hb = mb * 0.5 if mb > 0 else 0.0
    res = place_entry(
        user_id=user.id,
        instrument_key=body.instrument_key,
        direction=body.direction,
        quantity_lots=qty,
    )
    if not res.get("success"):
        raise HTTPException(status_code=502, detail=res.get("error") or "Order failed")
    oid = res.get("order_id")
    sym = body.symbol or match.get("symbol") or ""
    ltp = match.get("ltp")
    pid = repository.insert_position(
        d0,
        user.id,
        sym,
        body.instrument_key,
        body.direction,
        qty,
        ltp,
        mb,
        hb,
        oid,
    )
    audit(user.id, pid, "ENTRY", oid, qty)
    return {"success": True, "order_id": oid, "position_id": pid}


@router.post("/exit")
def post_exit(body: ExitBody, user: User = Depends(_require_user)):
    cfg = repository.get_config()
    if not cfg.get("live_enabled"):
        raise HTTPException(status_code=400, detail="Live trading is disabled")
    d0 = _session_date()
    positions = repository.list_open_positions(d0)
    pos = next((p for p in positions if int(p["id"]) == int(body.position_id)), None)
    if not pos:
        raise HTTPException(status_code=404, detail="Open position not found")
    mb = float(pos["main_brick_size"] or 0.0)
    if not should_exit_position(pos["instrument_key"], pos["direction"], mb):
        raise HTTPException(status_code=400, detail="Exit condition not met (1m Renko)")

    lots = int(pos["lots_open"])
    cfg_ps = int(cfg.get("position_size") or 1)
    partial = bool(cfg.get("partial_exit_enabled")) and cfg_ps > 1 and lots > 1
    exit_lots = 1 if partial else lots

    res = place_exit(
        user_id=user.id,
        instrument_key=pos["instrument_key"],
        direction=pos["direction"],
        quantity_lots=exit_lots,
    )
    if not res.get("success"):
        raise HTTPException(status_code=502, detail=res.get("error") or "Exit failed")
    oid = res.get("order_id")
    new_open = lots - exit_lots
    if new_open <= 0:
        repository.close_position(pos["id"])
    else:
        repository.update_position_lots(pos["id"], new_open, "OPEN")
    audit(user.id, pos["id"], "EXIT_PARTIAL" if partial else "EXIT_FULL", oid, exit_lots)
    return {"success": True, "order_id": oid, "lots_closed": exit_lots}


@router.post("/admin/run-scan")
def admin_run_scan(
    force: bool = Query(False, description="Run outside 9:15–15:30 IST (one-off / backfill)"),
    _: User = Depends(_require_admin),
):
    """Run scanner now. Use POST .../admin/run-scan?force=true for a one-off outside market hours."""
    return pipeline.run_smart_futures_scan_job(force=force)


@router.post("/admin/force-exit")
def admin_force_exit(admin: User = Depends(_require_admin)):
    return pipeline.force_exit_all_smart_futures_positions(user_id=admin.id)

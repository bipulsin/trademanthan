"""Kosmic Tarang API — Phase 2 screener + paper trade ticket (admin-only)."""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from backend.database import get_db
from backend.models.user import User
from backend.routers.auth import get_user_from_token, oauth2_scheme
from backend.services.tarang.chain_builder import ChainBuilder
from backend.services.tarang.config import get_events, get_profiles, get_risk
from backend.services.tarang.health import build_health_payload
from backend.services.tarang.iv_snapshots import capture_iv_snapshots
from backend.services.tarang.schema import ensure_tarang_tables
from backend.services.tarang.screener import (
    get_candidate,
    list_recent_candidates,
    list_rejections,
    run_screener,
)
from backend.services.tarang.sizing import min_max_loss_table
from backend.services.tarang.ticket import (
    build_ticket,
    dismiss_candidate,
    mark_taken_manual,
    take_trade_paper,
)

router = APIRouter(tags=["tarang"])
logger = logging.getLogger(__name__)


def _auth(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)) -> User:
    return get_user_from_token(token, db)


def _require_admin(user: User = Depends(_auth)) -> User:
    if (getattr(user, "is_admin", None) or "").strip() != "Yes":
        raise HTTPException(status_code=403, detail="Administrator only")
    return user


class TakeBody(BaseModel):
    note: str = ""


class ManualBody(BaseModel):
    note: str = ""
    fills: Dict[str, Any] = Field(default_factory=dict)


@router.get("/health")
def tarang_health(_user: User = Depends(_require_admin)) -> Dict[str, Any]:
    ensure_tarang_tables()
    return build_health_payload()


@router.get("/config")
def tarang_config(_user: User = Depends(_require_admin)) -> Dict[str, Any]:
    return {
        "profiles": get_profiles(),
        "risk": get_risk(),
        "events": get_events(),
    }


@router.get("/min-max-loss")
def tarang_min_max_loss(_user: User = Depends(_require_admin)) -> Dict[str, Any]:
    return min_max_loss_table()


@router.get("/chain/{profile_id}")
def tarang_chain(
    profile_id: str,
    expiry: Optional[str] = None,
    _user: User = Depends(_require_admin),
) -> Dict[str, Any]:
    ensure_tarang_tables()
    chain = ChainBuilder().build(profile_id.upper(), expiry=expiry)
    return {
        "profile_id": chain.profile_id,
        "venue": chain.venue,
        "underlying": chain.underlying,
        "expiry": chain.expiry,
        "futures_or_spot": chain.futures_or_spot,
        "lot_size": chain.lot_size,
        "strike_step": chain.strike_step,
        "built_at": chain.built_at,
        "meta": chain.meta,
        "quotes": [q.to_dict() for q in chain.quotes],
    }


@router.post("/iv-snapshots/run")
def tarang_iv_run(_user: User = Depends(_require_admin)) -> Dict[str, Any]:
    """Manual IV snapshot kick (also runs on APScheduler)."""
    return capture_iv_snapshots()


@router.post("/screener/run")
def tarang_screener_run(
    profile_id: Optional[str] = None,
    _user: User = Depends(_require_admin),
) -> Dict[str, Any]:
    ensure_tarang_tables()
    ids = [profile_id.upper()] if profile_id else None
    return run_screener(ids)


@router.get("/screener")
def tarang_screener_latest(_user: User = Depends(_require_admin)) -> Dict[str, Any]:
    """Latest candidates per profile (from recent runs)."""
    ensure_tarang_tables()
    cands = list_recent_candidates(80)
    # keep newest per profile
    by: Dict[str, Any] = {}
    for c in cands:
        pid = c["profile_id"]
        if pid not in by:
            by[pid] = c
    return {
        "product": "Kosmic Tarang",
        "phase": 2,
        "mode": "PAPER",
        "auto": False,
        "by_profile": by,
        "recent": cands[:20],
    }


@router.get("/candidates")
def tarang_candidates(
    limit: int = 40,
    _user: User = Depends(_require_admin),
) -> Dict[str, Any]:
    return {"candidates": list_recent_candidates(min(limit, 100))}


@router.get("/candidates/{candidate_id}")
def tarang_candidate(candidate_id: int, _user: User = Depends(_require_admin)) -> Dict[str, Any]:
    c = get_candidate(candidate_id)
    if not c:
        raise HTTPException(status_code=404, detail="Candidate not found")
    return c


@router.get("/rejections")
def tarang_rejections(
    limit: int = 50,
    profile_id: Optional[str] = None,
    _user: User = Depends(_require_admin),
) -> Dict[str, Any]:
    return {"rejections": list_rejections(min(limit, 200), profile_id=profile_id)}


@router.get("/ticket/{candidate_id}")
def tarang_ticket(candidate_id: int, _user: User = Depends(_require_admin)) -> Dict[str, Any]:
    t = build_ticket(candidate_id)
    if not t.get("ok"):
        raise HTTPException(status_code=404, detail=t.get("error") or "not found")
    return t


@router.post("/ticket/{candidate_id}/take")
def tarang_ticket_take(
    candidate_id: int,
    body: TakeBody = TakeBody(),
    _user: User = Depends(_require_admin),
) -> Dict[str, Any]:
    out = take_trade_paper(candidate_id, note=body.note or "")
    if not out.get("ok"):
        raise HTTPException(status_code=400, detail=out.get("error") or out)
    return out


@router.post("/ticket/{candidate_id}/dismiss")
def tarang_ticket_dismiss(candidate_id: int, _user: User = Depends(_require_admin)) -> Dict[str, Any]:
    out = dismiss_candidate(candidate_id)
    if not out.get("ok"):
        raise HTTPException(status_code=404, detail=out.get("error"))
    return out


@router.post("/ticket/{candidate_id}/mark-manual")
def tarang_ticket_manual(
    candidate_id: int,
    body: ManualBody = ManualBody(),
    _user: User = Depends(_require_admin),
) -> Dict[str, Any]:
    out = mark_taken_manual(candidate_id, fills=body.fills, note=body.note or "")
    if not out.get("ok"):
        raise HTTPException(status_code=400, detail=out.get("error") or out)
    return out

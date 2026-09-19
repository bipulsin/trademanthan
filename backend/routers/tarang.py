"""Kosmic Tarang API — Phase 1 data-health + read-only feeds (no screener / take-trade)."""
from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from backend.database import get_db
from backend.models.user import User
from backend.routers.auth import get_user_from_token, oauth2_scheme
from backend.services.tarang.chain_builder import ChainBuilder
from backend.services.tarang.config import get_events, get_profiles, get_risk
from backend.services.tarang.health import build_health_payload
from backend.services.tarang.iv_snapshots import capture_iv_snapshots
from backend.services.tarang.schema import ensure_tarang_tables
from backend.services.tarang.sizing import min_max_loss_table

router = APIRouter(tags=["tarang"])
logger = logging.getLogger(__name__)


def _auth(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)) -> User:
    return get_user_from_token(token, db)


def _require_admin(user: User = Depends(_auth)) -> User:
    if (getattr(user, "is_admin", None) or "").strip() != "Yes":
        raise HTTPException(status_code=403, detail="Administrator only")
    return user


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

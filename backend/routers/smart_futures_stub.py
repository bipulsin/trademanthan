"""
Placeholder Smart Futures routes: admin config only (JSON file), no screener / orders.

Mounted at /api/smart-futures and /smart-futures for admintwc.js and future UI.
"""
from __future__ import annotations

import json
import logging
import threading
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from backend.database import get_db
from backend.models.user import User
from backend.routers.auth import get_user_from_token, oauth2_scheme

logger = logging.getLogger(__name__)

router = APIRouter(tags=["smart-futures"])

_CONFIG_LOCK = threading.Lock()
_DATA_DIR = Path(__file__).resolve().parents[2] / "data"
_CONFIG_PATH = _DATA_DIR / "smart_futures_admin_stub.json"

_DEFAULT: Dict[str, Any] = {
    "live_enabled": False,
    "position_size": 1,
    "partial_exit_enabled": False,
    "brick_atr_period": 10,
    "brick_atr_override": None,
}


def _require_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)) -> User:
    return get_user_from_token(token, db)


def _require_admin(user: User = Depends(_require_user)) -> User:
    if (getattr(user, "is_admin", None) or "").strip() != "Yes":
        raise HTTPException(status_code=403, detail="Administrator only")
    return user


def _load_unlocked() -> Dict[str, Any]:
    out = dict(_DEFAULT)
    try:
        if _CONFIG_PATH.is_file():
            raw = json.loads(_CONFIG_PATH.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                for k in _DEFAULT:
                    if k in raw:
                        out[k] = raw[k]
    except Exception as e:
        logger.warning("smart_futures_stub: read config failed: %s", e)
    return out


def _read_config() -> Dict[str, Any]:
    with _CONFIG_LOCK:
        return _load_unlocked()


class SmartFuturesConfigUpdate(BaseModel):
    live_enabled: Optional[bool] = None
    position_size: Optional[int] = Field(None, ge=1, le=3)
    partial_exit_enabled: Optional[bool] = None
    brick_atr_period: Optional[int] = Field(None, ge=2, le=99)
    brick_atr_override: Optional[float] = None


@router.get("/config")
def get_sf_config_stub(user: User = Depends(_require_user)):
    """Admin UI + future screener: persisted parameters (file-backed, not DB)."""
    return _read_config()


@router.put("/config")
def put_sf_config_stub(body: SmartFuturesConfigUpdate, admin: User = Depends(_require_admin)):
    patch = body.model_dump(exclude_unset=True)
    with _CONFIG_LOCK:
        cur = _load_unlocked()
        cur.update(patch)
        _DATA_DIR.mkdir(parents=True, exist_ok=True)
        _CONFIG_PATH.write_text(json.dumps(cur, indent=2), encoding="utf-8")
    logger.info("smart_futures_stub: config saved by admin id=%s", getattr(admin, "id", None))
    return cur

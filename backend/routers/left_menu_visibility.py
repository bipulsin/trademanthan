"""
Left-menu visibility: admin enable/disable of shared nav items (file-backed JSON).

Mounted at /api/left-menu and /left-menu for admintwc.js and left-menu.js.
"""
from __future__ import annotations

import json
import logging
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from backend.database import get_db
from backend.models.user import User
from backend.routers.auth import get_user_from_token, oauth2_scheme

logger = logging.getLogger(__name__)

router = APIRouter(tags=["left-menu"])

_CONFIG_LOCK = threading.Lock()
_DATA_DIR = Path(__file__).resolve().parents[2] / "data"
_CONFIG_PATH = _DATA_DIR / "left_menu_visibility.json"

# Canonical menu inventory (order matches left-menu.html / left-menu.js).
# Keys are data-page values. Default: all enabled.
MENU_ITEMS: List[Dict[str, str]] = [
    {"key": "desktop.html", "label": "Dashboard"},
    {"key": "intraoption.html", "label": "Intraday Option"},
    {"key": "stockOptions.html", "label": "Stock Options"},
    {"key": "dailyfutures.html", "label": "Premium Futures"},
    {"key": "volumemismatchfutures.html", "label": "Volume Mismatch Futures"},
    {"key": "iron-condor.html", "label": "Iron Condor"},
    {"key": "pivot-breakout.html", "label": "Pivot Breakout"},
    {"key": "arbitrage.html", "label": "Arbitrage Selection"},
    {"key": "cargpt.html", "label": "Composite Avg"},
    {"key": "broker.html", "label": "Broker Management"},
    {"key": "strategy.html", "label": "Strategy Management"},
    {"key": "reports.html", "label": "Reports"},
    {"key": "tradelog.html", "label": "Trade Log"},
    {"key": "future_screener.html", "label": "Future Screener"},
    {"key": "breakfast.html", "label": "Breakfast Strategy"},
    {"key": "havwap.html", "label": "HA-VWAP Backtest"},
    {"key": "divtest.html", "label": "MACD Div Backtest"},
    {"key": "analysis.html", "label": "Analysis"},
    {"key": "settings.html", "label": "Settings"},
    {"key": "admintwc.html", "label": "Admin"},
    {"key": "rs-journey.html", "label": "RS Journey"},
]

_DEFAULT_ENABLED: Dict[str, bool] = {item["key"]: True for item in MENU_ITEMS}


def _require_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)) -> User:
    return get_user_from_token(token, db)


def _require_admin(user: User = Depends(_require_user)) -> User:
    if (getattr(user, "is_admin", None) or "").strip() != "Yes":
        raise HTTPException(status_code=403, detail="Administrator only")
    return user


def _defaults() -> Dict[str, bool]:
    return dict(_DEFAULT_ENABLED)


def _load_unlocked() -> Dict[str, bool]:
    out = _defaults()
    try:
        if _CONFIG_PATH.is_file():
            raw = json.loads(_CONFIG_PATH.read_text(encoding="utf-8"))
            items = raw.get("items") if isinstance(raw, dict) else None
            if isinstance(items, dict):
                for key in out:
                    if key in items:
                        out[key] = bool(items[key])
    except Exception as e:
        logger.warning("left_menu_visibility: read config failed: %s", e)
    return out


def _read_config() -> Dict[str, bool]:
    with _CONFIG_LOCK:
        return _load_unlocked()


def _payload(enabled: Dict[str, bool]) -> Dict[str, Any]:
    return {
        "items": [
            {
                "key": meta["key"],
                "label": meta["label"],
                "enabled": bool(enabled.get(meta["key"], True)),
            }
            for meta in MENU_ITEMS
        ],
        "enabled": {k: bool(enabled.get(k, True)) for k in _DEFAULT_ENABLED},
    }


class LeftMenuVisibilityUpdate(BaseModel):
    enabled: Optional[Dict[str, bool]] = Field(
        default=None,
        description="Map of data-page key -> enabled. Missing keys keep current/default.",
    )


@router.get("/visibility")
def get_left_menu_visibility(user: User = Depends(_require_user)):
    """Any authenticated user: enabled map for filtering the shared left menu."""
    return _payload(_read_config())


@router.put("/visibility")
def put_left_menu_visibility(
    body: LeftMenuVisibilityUpdate,
    admin: User = Depends(_require_admin),
):
    """Admin only: persist enable/disable preferences for left-menu items."""
    patch = body.enabled or {}
    with _CONFIG_LOCK:
        cur = _load_unlocked()
        for key, val in patch.items():
            if key in cur:
                cur[key] = bool(val)
        _DATA_DIR.mkdir(parents=True, exist_ok=True)
        _CONFIG_PATH.write_text(
            json.dumps({"items": cur}, indent=2),
            encoding="utf-8",
        )
    logger.info(
        "left_menu_visibility: saved by admin id=%s keys=%s",
        getattr(admin, "id", None),
        list(patch.keys()),
    )
    return _payload(cur)

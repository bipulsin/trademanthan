"""
Left-menu visibility: admin enable/disable of shared nav items (DB-backed).

Mounted at /api/left-menu and /left-menu for admintwc.js and left-menu.js.

Persistence: PostgreSQL ``app_settings`` key ``left_menu_visibility`` (survives
container rebuilds). Legacy file ``data/left_menu_visibility.json`` is read once
for migration if the DB row is missing.
"""
from __future__ import annotations

import json
import logging
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.orm import Session

from backend.database import SessionLocal, get_db
from backend.models.user import User
from backend.routers.auth import get_user_from_token, oauth2_scheme

logger = logging.getLogger(__name__)

router = APIRouter(tags=["left-menu"])

_CONFIG_LOCK = threading.Lock()
_SETTINGS_KEY = "left_menu_visibility"
_LEGACY_PATH = Path(__file__).resolve().parents[2] / "data" / "left_menu_visibility.json"

# Canonical menu inventory (order matches left-menu.html / left-menu.js).
# Keys are data-page values. Default: all enabled (only when no saved config).
MENU_ITEMS: List[Dict[str, str]] = [
    {"key": "desktop.html", "label": "Dashboard"},
    {"key": "intraoption.html", "label": "Intraday Option"},
    {"key": "stockOptions.html", "label": "Stock Options"},
    {"key": "commDiv.html", "label": "Commodities Div"},
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


def _merge_items(raw_items: Any) -> Dict[str, bool]:
    out = _defaults()
    if isinstance(raw_items, dict):
        for key in out:
            if key in raw_items:
                out[key] = bool(raw_items[key])
    return out


def _load_legacy_file() -> Optional[Dict[str, bool]]:
    try:
        if not _LEGACY_PATH.is_file():
            return None
        raw = json.loads(_LEGACY_PATH.read_text(encoding="utf-8"))
        items = raw.get("items") if isinstance(raw, dict) else None
        if not isinstance(items, dict):
            return None
        return _merge_items(items)
    except Exception as e:
        logger.warning("left_menu_visibility: legacy file read failed: %s", e)
        return None


def _db_get_raw(db: Session) -> Optional[str]:
    try:
        row = db.execute(
            text("SELECT value FROM app_settings WHERE key = :k"),
            {"k": _SETTINGS_KEY},
        ).fetchone()
        if not row:
            return None
        return row[0] if not hasattr(row, "value") else row.value
    except Exception as e:
        logger.warning("left_menu_visibility: DB read failed: %s", e)
        return None


def _db_upsert(db: Session, enabled: Dict[str, bool]) -> None:
    payload = json.dumps({"items": enabled})
    db.execute(
        text(
            """
            INSERT INTO app_settings (key, value, updated_at)
            VALUES (:k, :v, NOW())
            ON CONFLICT (key) DO UPDATE
            SET value = EXCLUDED.value, updated_at = NOW()
            """
        ),
        {"k": _SETTINGS_KEY, "v": payload},
    )
    db.commit()


def _read_config() -> Dict[str, bool]:
    """Return enabled map. Defaults only when no saved config exists yet."""
    with _CONFIG_LOCK:
        db = SessionLocal()
        try:
            raw = _db_get_raw(db)
            if raw is not None:
                try:
                    parsed = json.loads(raw)
                    items = parsed.get("items") if isinstance(parsed, dict) else None
                    return _merge_items(items)
                except (json.JSONDecodeError, TypeError) as e:
                    logger.warning("left_menu_visibility: bad DB JSON: %s", e)

            legacy = _load_legacy_file()
            if legacy is not None:
                try:
                    _db_upsert(db, legacy)
                    logger.info(
                        "left_menu_visibility: migrated legacy file -> app_settings"
                    )
                except Exception as e:
                    logger.warning(
                        "left_menu_visibility: legacy migrate failed (using file): %s",
                        e,
                    )
                    try:
                        db.rollback()
                    except Exception:
                        pass
                return legacy

            return _defaults()
        finally:
            db.close()


def _write_config(enabled: Dict[str, bool]) -> Dict[str, bool]:
    with _CONFIG_LOCK:
        db = SessionLocal()
        try:
            _db_upsert(db, enabled)
            # Best-effort dual-write for operators inspecting the data dir.
            try:
                _LEGACY_PATH.parent.mkdir(parents=True, exist_ok=True)
                _LEGACY_PATH.write_text(
                    json.dumps({"items": enabled}, indent=2),
                    encoding="utf-8",
                )
            except Exception as e:
                logger.debug("left_menu_visibility: dual-write file skipped: %s", e)
            return dict(enabled)
        except Exception as e:
            try:
                db.rollback()
            except Exception:
                pass
            logger.exception("left_menu_visibility: DB save failed: %s", e)
            raise HTTPException(status_code=500, detail="Failed to save menu visibility") from e
        finally:
            db.close()


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
    cur = _read_config()
    for key, val in patch.items():
        if key in cur:
            cur[key] = bool(val)
    saved = _write_config(cur)
    logger.info(
        "left_menu_visibility: saved by admin id=%s keys=%s",
        getattr(admin, "id", None),
        list(patch.keys()),
    )
    return _payload(saved)

"""Live ticker read + account settings and revocable app tokens."""
from __future__ import annotations

from typing import Any, Dict

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from backend.database import get_db
from backend.models.user import User
from backend.routers.auth import get_user_from_token
from backend.services.ticker_live import (
    ALGOS,
    build_snapshot,
    issue_token,
    revoke_tokens,
    save_settings,
    user_id_for_ticker_token,
)

router = APIRouter(prefix="/api/ticker", tags=["ticker"])


class SettingsBody(BaseModel):
    enabled: bool = True
    algos: Dict[str, bool] = Field(default_factory=dict)


def _user_from_bearer(authorization: str, db: Session) -> User:
    raw = (authorization or "").strip()
    if raw.lower().startswith("bearer "):
        raw = raw[7:].strip()
    if not raw:
        raise HTTPException(status_code=401, detail="Not authenticated")
    uid = user_id_for_ticker_token(db, raw)
    if uid is not None:
        user = db.get(User, uid)
        if not user or not getattr(user, "is_active", True):
            raise HTTPException(status_code=401, detail="Not authenticated")
        return user
    try:
        return get_user_from_token(raw, db)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=401, detail="Not authenticated") from exc


def _session_user(authorization: str = Header(default=""), db: Session = Depends(get_db)) -> User:
    """Settings changes accept the website login token only, not the app token."""
    raw = (authorization or "").strip()
    if raw.lower().startswith("bearer "):
        raw = raw[7:].strip()
    if not raw or raw.startswith("twt_"):
        raise HTTPException(status_code=401, detail="Sign in on the website to change ticker settings")
    return get_user_from_token(raw, db)


@router.get("/live")
def ticker_live(
    authorization: str = Header(default=""),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    user = _user_from_bearer(authorization, db)
    return build_snapshot(db, int(user.id))


@router.get("/settings")
def ticker_settings(
    user: User = Depends(_session_user),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    from backend.services.ticker_live import get_settings

    return {"ok": True, **get_settings(db, int(user.id)), "algo_keys": list(ALGOS)}


@router.put("/settings")
def ticker_settings_put(
    body: SettingsBody,
    user: User = Depends(_session_user),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    saved = save_settings(db, int(user.id), body.enabled, body.algos)
    db.commit()
    return {"ok": True, **saved}


@router.post("/token")
def ticker_token_create(
    user: User = Depends(_session_user),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    issued = issue_token(db, int(user.id))
    db.commit()
    return {"ok": True, **issued}


@router.delete("/token")
def ticker_token_revoke(
    user: User = Depends(_session_user),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    n = revoke_tokens(db, int(user.id))
    db.commit()
    return {"ok": True, "revoked": n}

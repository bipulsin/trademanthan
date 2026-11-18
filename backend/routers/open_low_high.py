"""Open Low High live screener API."""
from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from backend.database import get_db
from backend.models.user import User
from backend.routers.auth import get_user_from_token, oauth2_scheme

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/open-low-high", tags=["open-low-high"])


def _auth_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)) -> User:
    return get_user_from_token(token, db)


class RunBody(BaseModel):
    force: bool = Field(
        False,
        description="Re-run even outside window / when already frozen (manual/test).",
    )


@router.get("/today")
def get_today(_user: User = Depends(_auth_user)) -> Dict[str, Any]:
    """Today's live/frozen screen state. Auto-runs inside 09:25:05–09:27 IST."""
    from backend.services.open_low_high.screener import get_today_state

    return get_today_state(auto_run=True)


@router.get("/log")
def get_log(
    _user: User = Depends(_auth_user),
    limit_days: int = Query(60, ge=1, le=365),
    side: Optional[str] = Query(None, description="BULLISH or BEARISH"),
) -> Dict[str, Any]:
    from backend.services.open_low_high.screener import get_trade_log_payload

    return get_trade_log_payload(limit_days=limit_days, side=side)


@router.post("/run")
def run_now(
    body: RunBody,
    background_tasks: BackgroundTasks,
    _user: User = Depends(_auth_user),
) -> Dict[str, Any]:
    """Trigger screen. Use force=true outside the window or to rebuild a frozen day."""
    from backend.services.open_low_high.screener import run_screen, window_status

    win = window_status()
    if body.force or win == "open":
        # Long-running: queue so the HTTP call returns quickly when force scans full universe.
        def _job() -> None:
            try:
                run_screen(force=bool(body.force))
            except Exception:
                logger.exception("open_low_high background run failed")

        background_tasks.add_task(_job)
        return {
            "ok": True,
            "queued": True,
            "force": bool(body.force),
            "window": win,
            "message": "Screen queued.",
        }
    return {
        "ok": True,
        "queued": False,
        "force": False,
        "window": win,
        "message": "Outside screen window — pass force=true to run anyway.",
    }


@router.get("/health")
def health() -> Dict[str, Any]:
    from backend.services.open_low_high.persistence import ensure_tables, get_day_meta
    from backend.services.open_low_high.screener import _today, window_status

    try:
        ensure_tables()
        meta = get_day_meta(_today())
        return {
            "ok": True,
            "window": window_status(),
            "trade_date": _today().isoformat(),
            "meta": meta,
        }
    except Exception as exc:
        return {"ok": False, "error": str(exc)}

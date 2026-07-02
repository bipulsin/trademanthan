"""Daily RS Trade Checklist API.

Serves and mutates the per-stock pre-trade checklist for ``dailyRSchecklist.html``.
The decision engine lives in ``services.daily_checklist`` (single source of truth);
these endpoints are thin wrappers that always return the full page state so the
browser can re-render without partial-update bugs.
"""
import logging
from typing import Any, Optional

from fastapi import APIRouter
from pydantic import BaseModel

from backend.services import daily_checklist as svc

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/dashboard/daily-checklist", tags=["daily-checklist"])


class UpdateBody(BaseModel):
    symbol: Optional[str] = None
    field: str
    value: Any = None
    session_date: Optional[str] = None


class SymbolBody(BaseModel):
    symbol: str
    session_date: Optional[str] = None


@router.get("/data")
def data(date: Optional[str] = None):
    try:
        return svc.get_state(date)
    except Exception as exc:
        logger.warning("daily-checklist data failed: %s", exc)
        return {"session_date": svc.today_ist(), "stocks": [], "counts": {"go": 0, "watch": 0, "out": 0}, "error": str(exc)}


@router.post("/update")
def update(body: UpdateBody):
    try:
        return svc.update_field(body.symbol or "", body.field, body.value, body.session_date)
    except ValueError as exc:
        return {"error": str(exc)}
    except Exception as exc:
        logger.warning("daily-checklist update failed: %s", exc)
        return {"error": str(exc)}


@router.post("/reset")
def reset(date: Optional[str] = None):
    try:
        return svc.reset_day(date)
    except Exception as exc:
        logger.warning("daily-checklist reset failed: %s", exc)
        return {"error": str(exc)}


@router.post("/populate")
def populate():
    try:
        return svc.populate_from_rs()
    except Exception as exc:
        logger.warning("daily-checklist populate failed: %s", exc)
        return {"error": str(exc)}


@router.post("/sync")
def sync(body: SymbolBody):
    try:
        return svc.sync_symbol_from_rs(body.symbol, body.session_date)
    except ValueError as exc:
        return {"error": str(exc)}
    except Exception as exc:
        logger.warning("daily-checklist sync failed: %s", exc)
        return {"error": str(exc)}


@router.get("/history")
def history(limit: int = 30):
    try:
        return {"days": svc.history(limit)}
    except Exception as exc:
        logger.warning("daily-checklist history failed: %s", exc)
        return {"days": [], "error": str(exc)}


@router.post("/refresh")
def refresh():
    """On-demand checklist refresh from latest RS snapshot (same as 5m job)."""
    try:
        return svc.refresh_checklist_from_rs()
    except Exception as exc:
        logger.warning("daily-checklist refresh failed: %s", exc)
        return {"error": str(exc)}

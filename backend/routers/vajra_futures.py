"""Vajra futures rating API — TWCTO trade qualification for curr-month futures."""
from __future__ import annotations

import logging
from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, Query
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from backend.database import get_db
from backend.models.user import User
from backend.routers.auth import get_user_from_token, oauth2_scheme
from backend.services.vajra.job import fetch_vajra_ratings_for_session, run_vajra_futures_rating_job

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/vajra-futures", tags=["vajra-futures"])


def _require_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)) -> User:
    return get_user_from_token(token, db)


@router.get("/ratings")
def get_vajra_ratings(
    session_date: Optional[date] = Query(None, description="IST session date; default today"),
    user: User = Depends(_require_user),
):
    """Latest Vajra ratings for current-month futures (sorted by trade type, then confidence desc)."""
    del user
    try:
        from backend.services.smart_futures_session_date import effective_session_date_ist_for_trend

        sd = session_date or effective_session_date_ist_for_trend()
        rows = fetch_vajra_ratings_for_session(sd)
        computed_at = rows[0].get("computed_at") if rows else None
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "session_date": sd.isoformat(),
                "count": len(rows),
                "computed_at": computed_at,
                "rows": rows,
            },
        )
    except Exception as e:
        logger.exception("vajra_ratings: %s", e)
        return JSONResponse(
            status_code=500,
            content={"success": False, "message": str(e), "rows": []},
        )


@router.post("/run")
def run_vajra_ratings_now(user: User = Depends(_require_user)):
    """On-demand Vajra rating run (same logic as 15-min scheduler)."""
    del user
    try:
        result = run_vajra_futures_rating_job(scan_trigger="api")
        rows = fetch_vajra_ratings_for_session()
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                **result,
                "count": len(rows),
                "rows": rows,
            },
        )
    except Exception as e:
        logger.exception("vajra_run: %s", e)
        return JSONResponse(status_code=500, content={"success": False, "message": str(e)})

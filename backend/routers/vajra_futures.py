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
from backend.services.vajra.job import compute_vajra_ratings_live, fetch_vajra_ratings_for_session
from backend.services.vajra.timeframes import (
    DEFAULT_HTF,
    DEFAULT_SCAN_TF,
    HTF_IDS,
    SCAN_TF_IDS,
    valid_htf_for_scan,
)
from backend.services.vajra.job import run_vajra_futures_rating_job

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/vajra-futures", tags=["vajra-futures"])


def _require_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)) -> User:
    return get_user_from_token(token, db)


@router.get("/timeframes")
def get_vajra_timeframes(user: User = Depends(_require_user)):
    """Scan / HTF options and valid HTF choices per scan TF."""
    del user
    return JSONResponse(
        status_code=200,
        content={
            "success": True,
            "scan_tf_options": list(SCAN_TF_IDS),
            "htf_options": list(HTF_IDS),
            "default_scan_tf": DEFAULT_SCAN_TF,
            "default_htf": DEFAULT_HTF,
            "valid_htf_by_scan": {s: valid_htf_for_scan(s) for s in SCAN_TF_IDS},
        },
    )


@router.get("/ratings")
def get_vajra_ratings(
    session_date: Optional[date] = Query(None, description="IST session date; default today"),
    scan_tf: str = Query(DEFAULT_SCAN_TF, description="Legacy mode scan TF (ignored when mode=transition)"),
    htf: str = Query(DEFAULT_HTF, description="Legacy mode HTF (ignored when mode=transition)"),
    mode: str = Query(
        "transition",
        description="transition = 30m TPS discovery + 5m shortlist validation; legacy = single TF ECS",
    ),
    user: User = Depends(_require_user),
):
    """Vajra ratings for current-month futures (transition pipeline or legacy ECS)."""
    del user
    try:
        from backend.services.smart_futures_session_date import effective_session_date_ist_for_trend
        from backend.services.vajra.pipeline import DISCOVERY_TF, EXECUTION_TF, HTF_BIAS_TF
        from backend.services.vajra.timeframes import validate_tf_pair

        sd = session_date or effective_session_date_ist_for_trend()
        mode_norm = (mode or "transition").strip().lower()
        if mode_norm == "transition":
            rows = compute_vajra_ratings_live(mode="transition", session_date=sd)
            computed_at = rows[0].get("computed_at") if rows else None
            alerts = [r for r in rows if r.get("alertable")]
            return JSONResponse(
                status_code=200,
                content={
                    "success": True,
                    "session_date": sd.isoformat(),
                    "mode": "transition",
                    "discovery_tf": DISCOVERY_TF,
                    "execution_tf": EXECUTION_TF,
                    "htf_bias_tf": HTF_BIAS_TF,
                    "scan_tf": DISCOVERY_TF,
                    "htf": HTF_BIAS_TF,
                    "source": "live_pipeline",
                    "count": len(rows),
                    "alert_count": len(alerts),
                    "alerts": alerts,
                    "computed_at": computed_at,
                    "rows": rows,
                },
            )

        scan_id, htf_id = validate_tf_pair(scan_tf, htf)
        rows = compute_vajra_ratings_live(scan_id, htf_id, sd, mode="legacy")
        computed_at = rows[0].get("computed_at") if rows else None
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "session_date": sd.isoformat(),
                "mode": "legacy",
                "scan_tf": scan_id,
                "htf": htf_id,
                "source": "live",
                "count": len(rows),
                "computed_at": computed_at,
                "rows": rows,
            },
        )
    except ValueError as e:
        return JSONResponse(
            status_code=400,
            content={"success": False, "message": str(e), "rows": []},
        )
    except Exception as e:
        logger.exception("vajra_ratings: %s", e)
        return JSONResponse(
            status_code=500,
            content={"success": False, "message": str(e), "rows": []},
        )


@router.post("/run")
def run_vajra_ratings_now(user: User = Depends(_require_user)):
    """On-demand Vajra rating run (scheduler stores 15m scan / 1hr HTF)."""
    del user
    try:
        result = run_vajra_futures_rating_job(scan_trigger="api")
        rows = fetch_vajra_ratings_for_session()
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                **result,
                "scan_tf": "15m",
                "htf": "1hr",
                "count": len(rows),
                "rows": rows,
            },
        )
    except Exception as e:
        logger.exception("vajra_run: %s", e)
        return JSONResponse(status_code=500, content={"success": False, "message": str(e)})

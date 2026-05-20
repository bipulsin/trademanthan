"""Vajra futures rating API — TWCTO trade qualification for curr-month futures."""
from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Body, Depends, Query
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from backend.database import get_db
from backend.models.user import User
from backend.routers.auth import get_user_from_token, oauth2_scheme
from backend.services.vajra.job import (
    compute_vajra_ratings_live,
    fetch_vajra_ratings_for_session,
    fetch_vajra_ratings_updated_at,
    resolve_vajra_ratings_for_api,
    sort_vajra_rows_for_display,
)
from backend.services.vajra.ranking import build_screener_display
from backend.services.vajra.timeframes import (
    DEFAULT_HTF,
    DEFAULT_SCAN_TF,
    HTF_IDS,
    SCAN_TF_IDS,
    valid_htf_for_scan,
)
from backend.services.vajra.job import run_vajra_futures_rating_job
from backend.services.vajra import trade_service as vajra_trade_service

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


@router.get("/ratings-status")
def get_vajra_ratings_status(
    session_date: Optional[date] = Query(None, description="IST session date; default today"),
    user: User = Depends(_require_user),
):
    """Lightweight poll target: when computed_at changes, the 5m rating job has finished."""
    del user
    try:
        from backend.services.smart_futures_session_date import effective_session_date_ist_for_trend

        sd = session_date or effective_session_date_ist_for_trend()
        updated = fetch_vajra_ratings_updated_at(sd)
        computed_at = updated.isoformat() if updated else None
        data_age_sec = None
        if updated:
            data_age_sec = max(
                0,
                int((datetime.now(updated.tzinfo) - updated).total_seconds()),
            )
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "session_date": sd.isoformat(),
                "computed_at": computed_at,
                "data_age_sec": data_age_sec,
                "ees_refresh_minutes": 5,
            },
            headers={
                "Cache-Control": "no-store, no-cache, must-revalidate",
                "Pragma": "no-cache",
            },
        )
    except Exception as e:
        logger.exception("vajra_ratings_status: %s", e)
        return JSONResponse(
            status_code=500,
            content={"success": False, "message": str(e), "computed_at": None},
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
            rows, source, stale_reason = resolve_vajra_ratings_for_api(sd, use_cache=True)
            display = build_screener_display(rows)
            rows = display["rows"]
            updated_dt = fetch_vajra_ratings_updated_at(sd)
            computed_at = (
                updated_dt.isoformat()
                if updated_dt
                else (rows[0].get("computed_at") if rows else None)
            )
            data_age_sec = None
            if updated_dt:
                data_age_sec = max(
                    0,
                    int((datetime.now(updated_dt.tzinfo) - updated_dt).total_seconds()),
                )
            alerts = [r for r in rows if r.get("alertable")]
            ees_alert_rows = [
                r for r in rows if (r.get("ees_alerts") or []) and not r.get("alertable")
            ]
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
                    "source": source,
                    "stale_reason": stale_reason,
                    "count": len(rows),
                    "alert_count": len(alerts) + len(ees_alert_rows),
                    "alerts": alerts,
                    "ees_alert_rows": ees_alert_rows,
                    "computed_at": computed_at,
                    "data_age_sec": data_age_sec,
                    "ees_refresh_minutes": 5,
                    "rows": rows,
                    "top_picks": display["top_picks"],
                    "top_sections": display["top_sections"],
                    "groups": {
                        "EXECUTABLE": display["groups"].get("EXECUTABLE", []),
                        "ARMED": display["groups"].get("ARMED", []),
                        "DISCOVERY": display["groups"].get("DISCOVERY", []),
                        "WATCHLIST": display["groups"].get("WATCHLIST", []),
                        "REJECT": display["groups"].get("REJECT", []),
                    },
                    "sections": display.get("sections", {}),
                    "banner": display.get("banner"),
                    "remainder": display["remainder"],
                },
                headers={
                    "Cache-Control": "no-store, no-cache, must-revalidate",
                    "Pragma": "no-cache",
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
                "scan_tf": "30m",
                "htf": "1hr",
                "count": len(rows),
                "rows": rows,
            },
        )
    except Exception as e:
        logger.exception("vajra_run: %s", e)
        return JSONResponse(status_code=500, content={"success": False, "message": str(e)})


@router.get("/trades")
def list_vajra_trades(
    status: str = Query("active", description="active or closed"),
    platform: Optional[str] = Query(None),
    user: User = Depends(_require_user),
):
    rows = vajra_trade_service.list_trades(user.id, status=status, platform=platform)
    return JSONResponse(status_code=200, content={"success": True, "rows": rows, "count": len(rows)})


@router.post("/trades/validate-preview")
def vajra_trade_validate_preview(
    body: Dict[str, Any] = Body(...),
    user: User = Depends(_require_user),
):
    preview = vajra_trade_service.validation_preview(
        user.id,
        stock=str(body.get("stock") or ""),
        direction=str(body.get("direction") or "LONG"),
        instrument_key=str(body.get("instrument_key") or ""),
        discovery_row=body.get("discovery_row") or {},
    )
    return JSONResponse(status_code=200, content={"success": True, **preview})


@router.post("/trades")
def vajra_trade_activate(
    body: Dict[str, Any] = Body(...),
    user: User = Depends(_require_user),
):
    try:
        et_raw = body.get("entry_time")
        if et_raw:
            entry_time = datetime.fromisoformat(str(et_raw).replace("Z", "+00:00"))
        else:
            entry_time = datetime.now()
        trade = vajra_trade_service.activate_trade(
            user.id,
            platform=str(body.get("platform") or "daily_futures"),
            stock=str(body.get("stock") or ""),
            future_symbol=str(body.get("future_symbol") or body.get("security") or ""),
            instrument_key=str(body.get("instrument_key") or ""),
            direction=str(body.get("direction") or "LONG"),
            entry_price=float(body.get("entry_price") or 0),
            lots=int(body.get("lots") or 1),
            entry_time=entry_time,
            discovery_row=body.get("discovery_row") or {},
            checklist=body.get("checklist") or {},
            metrics=body.get("metrics") or {},
            warnings=body.get("warnings") or [],
        )
        refreshed = vajra_trade_service.persist_refresh(user.id, int(trade["id"]))
        return JSONResponse(status_code=200, content={"success": True, "trade": refreshed})
    except Exception as e:
        logger.exception("vajra_trade_activate: %s", e)
        return JSONResponse(status_code=500, content={"success": False, "message": str(e)})


@router.post("/trades/{trade_id}/close")
def vajra_trade_close(
    trade_id: int,
    body: Dict[str, Any] = Body(...),
    user: User = Depends(_require_user),
):
    try:
        xt_raw = body.get("exit_time")
        exit_time = (
            datetime.fromisoformat(str(xt_raw).replace("Z", "+00:00"))
            if xt_raw
            else datetime.now()
        )
        trade = vajra_trade_service.close_trade(
            user.id,
            trade_id,
            exit_price=float(body.get("exit_price") or 0),
            exit_time=exit_time,
            exit_reasons=list(body.get("exit_reasons") or []),
        )
        if not trade:
            return JSONResponse(status_code=404, content={"success": False, "message": "Trade not found"})
        return JSONResponse(status_code=200, content={"success": True, "trade": trade})
    except Exception as e:
        logger.exception("vajra_trade_close: %s", e)
        return JSONResponse(status_code=500, content={"success": False, "message": str(e)})


@router.post("/trades/{trade_id}/refresh")
def vajra_trade_refresh(trade_id: int, user: User = Depends(_require_user)):
    trade = vajra_trade_service.persist_refresh(user.id, trade_id)
    if not trade:
        return JSONResponse(status_code=404, content={"success": False, "message": "Trade not found"})
    return JSONResponse(status_code=200, content={"success": True, "trade": trade})

"""Vajra futures rating API — TWCTO trade qualification for curr-month futures."""
from __future__ import annotations

import logging
import math
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
from backend.services.vajra.ranking import build_screener_display, build_universe_modal_rows
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


def _json_safe(value: Any) -> Any:
    """Ensure Vajra API payloads serialize (datetime, NaN, nested dicts)."""
    if value is None or isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    return str(value)


def _resolve_vajra_transition_bundle(
    user_id: int,
    session_date: Optional[date] = None,
) -> Dict[str, Any]:
    """Shared ratings payload: screener display + stable execution (no full universe build)."""
    from backend.services.smart_futures_session_date import effective_session_date_ist_for_trend
    from backend.services.vajra.job import load_arbitrage_curr_mth_universe
    from backend.services.vajra.pipeline import DISCOVERY_TF, EXECUTION_TF, HTF_BIAS_TF
    from backend.services.vajra.session_window import vajra_session_api_fields
    from backend.services.vajra.stable_execution import apply_stable_execution_overlay

    sd = session_date or effective_session_date_ist_for_trend()
    rows, source, stale_reason = resolve_vajra_ratings_for_api(sd, use_cache=True)
    stable = apply_stable_execution_overlay(rows, user_id, session_date=sd)
    merged_rows = list(stable["rows"])
    row_by_stock = {
        str(r.get("stock") or r.get("security") or "").strip().upper(): r
        for r in merged_rows
    }
    for slot_row in stable.get("sticky_top3") or []:
        sym = str(slot_row.get("stock") or "").strip().upper()
        if sym:
            row_by_stock[sym] = slot_row
    rated_for_screener = list(row_by_stock.values())
    display = build_screener_display(rated_for_screener)
    screener_rows = display["rows"]
    updated_dt = fetch_vajra_ratings_updated_at(sd)
    computed_at = (
        updated_dt.isoformat()
        if updated_dt
        else (screener_rows[0].get("computed_at") if screener_rows else None)
    )
    data_age_sec = None
    if updated_dt:
        data_age_sec = max(
            0,
            int((datetime.now(updated_dt.tzinfo) - updated_dt).total_seconds()),
        )
    alerts = [r for r in screener_rows if r.get("alertable")]
    ees_alert_rows = [
        r for r in screener_rows if (r.get("ees_alerts") or []) and not r.get("alertable")
    ]
    universe_count = len(load_arbitrage_curr_mth_universe())
    return {
        "session_date": sd,
        "source": source,
        "stale_reason": stale_reason,
        "computed_at": computed_at,
        "data_age_sec": data_age_sec,
        "discovery_tf": DISCOVERY_TF,
        "execution_tf": EXECUTION_TF,
        "htf_bias_tf": HTF_BIAS_TF,
        "alerts": alerts,
        "ees_alert_rows": ees_alert_rows,
        "display": display,
        "stable": stable,
        "rated_for_universe": rated_for_screener,
        "universe_count": universe_count,
        "session_fields": vajra_session_api_fields(),
    }


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
        from backend.services.vajra.session_window import vajra_session_api_fields

        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "session_date": sd.isoformat(),
                "computed_at": computed_at,
                "data_age_sec": data_age_sec,
                "ees_refresh_minutes": 5,
                **vajra_session_api_fields(),
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
    try:
        from backend.services.smart_futures_session_date import effective_session_date_ist_for_trend
        from backend.services.vajra.pipeline import DISCOVERY_TF, EXECUTION_TF, HTF_BIAS_TF
        from backend.services.vajra.timeframes import validate_tf_pair

        sd = session_date or effective_session_date_ist_for_trend()
        mode_norm = (mode or "transition").strip().lower()
        if mode_norm == "transition":
            bundle = _resolve_vajra_transition_bundle(user.id, session_date=sd)
            display = bundle["display"]
            stable = bundle["stable"]
            rows = display["rows"]
            sd = bundle["session_date"]
            payload = {
                "success": True,
                "session_date": sd.isoformat(),
                "mode": "transition",
                "discovery_tf": bundle["discovery_tf"],
                "execution_tf": bundle["execution_tf"],
                "htf_bias_tf": bundle["htf_bias_tf"],
                "scan_tf": bundle["discovery_tf"],
                "htf": bundle["htf_bias_tf"],
                "source": bundle["source"],
                "stale_reason": bundle["stale_reason"],
                "count": len(rows),
                "universe_count": bundle["universe_count"],
                "alert_count": len(bundle["alerts"]) + len(bundle["ees_alert_rows"]),
                "alerts": bundle["alerts"],
                "ees_alert_rows": bundle["ees_alert_rows"],
                "computed_at": bundle["computed_at"],
                "data_age_sec": bundle["data_age_sec"],
                "ees_refresh_minutes": 5,
                **bundle["session_fields"],
                "rows": rows,
                "universe_rows": [],
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
                "stable_execution": {
                    "stable_mode_enabled": stable.get("stable_mode_enabled"),
                    "focus_mode_enabled": stable.get("focus_mode_enabled"),
                    "sticky_persist_minutes": stable.get("sticky_persist_minutes"),
                    "sticky_top3": stable.get("sticky_top3"),
                    "momentum_leaders": stable.get("momentum_leaders") or [],
                    "suggested_rotations": stable.get("suggested_rotations"),
                    "freeze_window_open": stable.get("freeze_window_open"),
                    "watchlist_frozen": stable.get("watchlist_frozen"),
                    "frozen_focus_stocks": stable.get("frozen_focus_stocks"),
                    "watchlist_frozen_at": stable.get("watchlist_frozen_at"),
                    "attention_banner": stable.get("attention_banner"),
                    "discovery_window": stable.get("discovery_window"),
                    "execution_window": stable.get("execution_window"),
                    "workflow_phase": stable.get("workflow_phase"),
                    "workflow_notice": stable.get("workflow_notice"),
                    "sector_heatmap": stable.get("sector_heatmap") or [],
                    "co_pilot": stable.get("co_pilot") or {},
                },
                "co_pilot": stable.get("co_pilot") or {},
                "server_telegram_alerts": stable.get("server_telegram_alerts", False),
            }
            return JSONResponse(
                status_code=200,
                content=_json_safe(payload),
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
            content=_json_safe({"success": False, "message": str(e), "rows": []}),
        )


@router.get("/universe-rows")
def get_vajra_universe_rows(
    session_date: Optional[date] = Query(None, description="IST session date; default today"),
    user: User = Depends(_require_user),
):
    """Full arbitrage_master list for the More modal (loaded on demand — keeps /ratings fast)."""
    try:
        from backend.services.vajra.job import load_arbitrage_curr_mth_universe

        bundle = _resolve_vajra_transition_bundle(user.id, session_date=session_date)
        universe = load_arbitrage_curr_mth_universe()
        universe_rows = build_universe_modal_rows(bundle["rated_for_universe"], universe)
        return JSONResponse(
            status_code=200,
            content=_json_safe(
                {
                    "success": True,
                    "session_date": bundle["session_date"].isoformat(),
                    "universe_count": len(universe_rows),
                    "universe_rows": universe_rows,
                    "stable_execution": {
                        "stable_mode_enabled": bundle["stable"].get("stable_mode_enabled"),
                        "sticky_top3": bundle["stable"].get("sticky_top3") or [],
                    },
                }
            ),
            headers={
                "Cache-Control": "no-store, no-cache, must-revalidate",
                "Pragma": "no-cache",
            },
        )
    except Exception as e:
        logger.exception("vajra_universe_rows: %s", e)
        return JSONResponse(
            status_code=500,
            content=_json_safe(
                {"success": False, "message": str(e), "universe_rows": [], "universe_count": 0}
            ),
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


@router.get("/contract-lot-size")
def vajra_contract_lot_size(
    instrument_key: str = Query(..., description="Upstox NSE FUT instrument_key"),
    user: User = Depends(_require_user),
):
    """Read-only exchange lot size for open-position P&L display (no trade logic)."""
    del user
    try:
        from backend.services.smart_futures_picker.position_sizing import (
            get_futures_lot_size_by_instrument_key,
        )

        ik = str(instrument_key or "").strip()
        if not ik:
            return JSONResponse(status_code=400, content={"success": False, "message": "instrument_key required"})
        ls = int(get_futures_lot_size_by_instrument_key(ik))
        return JSONResponse(
            status_code=200,
            content={"success": True, "instrument_key": ik, "lot_size": ls if ls > 0 else None},
        )
    except Exception as e:
        logger.exception("vajra_contract_lot_size: %s", e)
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
    except ValueError as e:
        return JSONResponse(status_code=400, content={"success": False, "message": str(e)})
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


@router.get("/stable-execution/state")
def get_stable_execution_state(
    session_date: Optional[date] = Query(None),
    user: User = Depends(_require_user),
):
    from backend.services.smart_futures_session_date import effective_session_date_ist_for_trend
    from backend.services.vajra.stable_execution import (
        ALLOWED_STICKY_MINUTES,
        is_freeze_watchlist_window_ist,
        load_user_state,
    )

    sd = session_date or effective_session_date_ist_for_trend()
    cfg = load_user_state(user.id, sd)
    return JSONResponse(
        status_code=200,
        content={
            "success": True,
            "session_date": sd.isoformat(),
            "stable_mode_enabled": cfg.stable_mode_enabled,
            "focus_mode_enabled": cfg.focus_mode_enabled,
            "sticky_persist_minutes": cfg.sticky_persist_minutes,
            "allowed_sticky_minutes": list(ALLOWED_STICKY_MINUTES),
            "frozen_focus_stocks": cfg.frozen_focus_stocks,
            "watchlist_frozen_at": (
                cfg.watchlist_frozen_at.isoformat() if cfg.watchlist_frozen_at else None
            ),
            "freeze_window_open": is_freeze_watchlist_window_ist(),
            "sticky_slots": cfg.sticky_slots,
        },
    )


@router.put("/stable-execution/state")
def put_stable_execution_state(
    body: Dict[str, Any] = Body(...),
    user: User = Depends(_require_user),
):
    from backend.services.vajra.stable_execution import load_user_state, save_user_state

    cfg = load_user_state(user.id)
    if "stable_mode_enabled" in body:
        cfg.stable_mode_enabled = bool(body["stable_mode_enabled"])
    if "focus_mode_enabled" in body:
        cfg.focus_mode_enabled = bool(body["focus_mode_enabled"])
    if "sticky_persist_minutes" in body:
        cfg.sticky_persist_minutes = int(body["sticky_persist_minutes"])
    save_user_state(user.id, cfg)
    return JSONResponse(status_code=200, content={"success": True})


@router.post("/stable-execution/freeze-focus")
def post_freeze_watchlist_focus(
    body: Dict[str, Any] = Body(...),
    user: User = Depends(_require_user),
):
    from backend.services.vajra.stable_execution import freeze_watchlist_focus

    stocks = list(body.get("stocks") or body.get("frozen_focus_stocks") or [])
    out = freeze_watchlist_focus(user.id, stocks)
    return JSONResponse(status_code=200, content=out)


@router.get("/sector-persistence")
def get_sector_persistence_heatmap(
    session_date: Optional[date] = Query(None),
    user: User = Depends(_require_user),
):
    """Sector persistence heatmap for dashboard / Vajra stable execution."""
    del user  # auth only
    try:
        from backend.services.smart_futures_session_date import effective_session_date_ist_for_trend
        from backend.services.vajra.sector_intelligence import build_sector_persistence_heatmap
        from backend.services.vajra.session_window import vajra_workflow_phase_fields

        sd = session_date or effective_session_date_ist_for_trend()
        rows = build_sector_persistence_heatmap(session_date=sd)
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "session_date": sd.isoformat(),
                "sectors": rows,
                **vajra_workflow_phase_fields(),
            },
        )
    except Exception as e:
        logger.exception("vajra_sector_persistence: %s", e)
        return JSONResponse(status_code=500, content={"success": False, "message": str(e)})

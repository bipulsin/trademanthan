"""
Iron Condor advisory API — checklist, analysis, entry confirm, polling, alerts, journal.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.orm import Session

from backend.database import get_db
from backend.models.user import User
from backend.routers.auth import get_user_from_token, oauth2_scheme
from backend.services.iron_condor_universe import sector_for_symbol
from backend.services import iron_condor_service as ic
from backend.services import iron_condor_extended as ice
from backend.config import settings
from backend.services import iron_condor_checklist as chk
from backend.services import market_holiday as mh
from backend.services.iron_condor_snapshot_cache import (
    ensure_iron_condor_snapshot_tables,
    run_iron_condor_daily_snapshot_job,
)

router = APIRouter(prefix="/iron-condor", tags=["iron-condor"])
logger = logging.getLogger(__name__)


def _auth(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)) -> User:
    return get_user_from_token(token, db)


def _ser(d: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(d)
    for k in list(out.keys()):
        v = out[k]
        if hasattr(v, "isoformat") and not isinstance(v, str):
            try:
                out[k] = v.isoformat()
            except Exception:
                pass
    return out


class SettingsBody(BaseModel):
    trading_capital: Optional[float] = Field(None, ge=0)
    max_simultaneous_positions: Optional[int] = Field(None, ge=1, le=12)
    target_position_slots: Optional[int] = Field(None, ge=1, le=10)
    profit_target_pct_of_credit: Optional[float] = None
    stop_loss_pct_of_credit: Optional[float] = None


class ChecklistBody(BaseModel):
    underlying: str = Field(..., min_length=1, max_length=32)
    new_capital_estimate: float = Field(0.0, ge=0)
    declared_next_earnings_iso: Optional[str] = Field(None, max_length=12)


class AnalyzeBody(BaseModel):
    underlying: str = Field(..., min_length=1, max_length=32)


class AnalyzeDetailedBody(BaseModel):
    underlying: str = Field(..., min_length=1, max_length=32)
    strike_overrides: Optional[Dict[str, Any]] = None


class ConfirmEntryBody(BaseModel):
    analysis: Dict[str, Any]
    fills: Dict[str, float]
    lot_size: Optional[int] = None
    num_lots: Optional[int] = Field(1, ge=1)
    declared_next_earnings_iso: Optional[str] = Field(None, max_length=12)
    placed_orders_confirmed: bool = False


class LogAdjustmentBody(BaseModel):
    strikes: Dict[str, float]
    fills: Dict[str, float]
    notes: Optional[str] = Field(None, max_length=512)


class CloseJournalBody(BaseModel):
    position_id: int = Field(..., ge=1)
    squaring_confirmed: bool = False
    exit_reason: str = Field(..., min_length=2, max_length=64)
    emotion: str = Field(..., min_length=2, max_length=32)
    followed_rules: bool
    deviation_notes: Optional[str] = None
    lesson_learned: Optional[str] = None
    exit_fills: Dict[str, float] = Field(default_factory=dict)


class SaveBody(BaseModel):
    analysis: Dict[str, Any]


def _feed_status(db: Session, user_id: int) -> Dict[str, Any]:
    try:
        ice.iron_condor_migrations_v2()
    except Exception as e:
        logger.warning("iron_condor _feed_status migrations_v2: %s", e)
    st = ic.get_or_create_settings(db, user_id)
    streak = int(st.get("ic_poll_fail_streak") or 0)
    lo = st.get("ic_last_quote_success_at")
    return {
        "poll_fail_streak": streak,
        "last_quote_success_at": lo.isoformat() if hasattr(lo, "isoformat") else (str(lo) if lo else None),
        "data_feed_lost": streak >= 3,
    }


@router.post("/refresh-daily-cache")
def iron_condor_refresh_daily_cache(_user: User = Depends(_auth)) -> Dict[str, Any]:
    """
    Rebuild today's Iron Condor pre-market cache (India VIX + per-underlying monthly ATR + daily closes).
    Same workload as the 08:33 IST scheduler — use when pre-market job was missed.
    """
    ensure_iron_condor_snapshot_tables()
    return run_iron_condor_daily_snapshot_job()


@router.get("/session")
def session_state(db: Session = Depends(get_db), user: User = Depends(_auth)) -> Dict[str, Any]:
    ic.ensure_iron_condor_tables()
    now = mh._normalize_ist(None)
    poll = ice.is_iron_condor_poll_window_ist(now)
    qf = _feed_status(db, int(user.id))
    banner = None if poll else "Market closed — live leg polling paused. Last LTPs are stale until next session."
    if poll and qf.get("data_feed_lost"):
        banner = "Data feed lost after {} failed poll cycle(s). Last quote update: {} — do not trust stop/adjust triggers until fresh quotes return.".format(
            qf.get("poll_fail_streak"),
            qf.get("last_quote_success_at") or "unknown",
        )
    return {
        "market_poll_active": poll,
        "banner": banner,
        "quote_feed": qf,
        "position_verify_prompt": ice.session_position_verify_needed(db, int(user.id)),
    }


@router.post("/session/verify-positions-held")
def verify_positions_held(db: Session = Depends(get_db), user: User = Depends(_auth)) -> Dict[str, Any]:
    ice.mark_positions_verified_for_session(db, int(user.id))
    return {"success": True}


@router.get("/approved-underlyings")
def iron_condor_approved_underlyings() -> Dict[str, Any]:
    """
    Approved universe rows: symbol, sector, equity instrument_key (cached server-side).
    No auth — used to populate the picker when JWT/session is missing or slow; quotes still require broker login.
    """
    return {"symbols": ic.get_iron_condor_universe_master_rows()}


@router.get("/universe")
def iron_condor_universe(_user: User = Depends(_auth)) -> Dict[str, Any]:
    return {"symbols": ic.get_iron_condor_universe_master_rows()}


@router.get("/universe-with-quotes")
def universe_with_quotes(
    db: Session = Depends(get_db),
    user: User = Depends(_auth),
) -> Dict[str, Any]:
    """Picker: LTP, day %change, sector — one batched Upstox request + short TTL cache."""
    try:
        ic.ensure_iron_condor_tables()
        active_rows = db.execute(
            text(
                """
                SELECT UPPER(TRIM(underlying)) AS u FROM iron_condor_position
                WHERE user_id = :uid AND UPPER(status) IN ('ACTIVE', 'OPEN', 'ADJUSTED')
                """
            ),
            {"uid": int(user.id)},
        ).fetchall()
        active_set = {str(r[0]) for r in active_rows if r and r[0]}
        base_rows, quotes_error = ic.build_universe_picker_rows_with_quotes_cached()
        out = []
        for r in base_rows:
            item = dict(r)
            item["active_position"] = str(item.get("symbol") or "").upper() in active_set
            out.append(item)
        return {"symbols": out, "quotes_error": quotes_error}
    except Exception as e:
        logger.exception("universe_with_quotes failed: %s", e)
        return {
            "symbols": [],
            "quotes_error": "Universe quotes temporarily unavailable — try again shortly.",
        }


@router.get("/universe-symbol-snapshot-row")
def universe_symbol_snapshot_row(
    underlying: str = Query(..., min_length=1, max_length=32),
    db: Session = Depends(get_db),
    user: User = Depends(_auth),
) -> Dict[str, Any]:
    """
    Fast DB-only row for step-1 picker (today's cached daily closes). No IC DDL migrations, no Upstox.
    Lets the UI render before the slower /universe-symbol-quote finishes.
    """
    try:
        row, quotes_error = ic.universe_picker_snapshot_row_only(db, int(user.id), underlying.strip())
        if not row:
            raise HTTPException(
                status_code=400,
                detail=quotes_error or "Unknown or disallowed underlying",
            )
        return {"row": row, "quotes_error": quotes_error}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("universe-symbol-snapshot-row failed: %s", e)
        raise HTTPException(
            status_code=503,
            detail="Could not load snapshot row — try again shortly.",
        ) from e


@router.get("/universe-symbol-quote")
def universe_symbol_quote(
    underlying: str = Query(..., min_length=1, max_length=32),
    db: Session = Depends(get_db),
    user: User = Depends(_auth),
) -> Dict[str, Any]:
    """Live quote for one approved underlying after user selection (no full-universe prefetch)."""
    try:
        row, quotes_error = ic.universe_picker_row_for_symbol(db, int(user.id), underlying.strip())
        if not row:
            raise HTTPException(
                status_code=400,
                detail=quotes_error or "Unknown or disallowed underlying",
            )
        return {"row": row, "quotes_error": quotes_error}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("universe-symbol-quote failed: %s", e)
        raise HTTPException(
            status_code=503,
            detail="Quote request failed — try again shortly.",
        ) from e


@router.post("/checklist")
def post_checklist(
    body: ChecklistBody,
    db: Session = Depends(get_db),
    user: User = Depends(_auth),
) -> Dict[str, Any]:
    ic.ensure_iron_condor_tables()
    und = body.underlying.strip().upper()
    sec = sector_for_symbol(und)
    if not sec:
        raise HTTPException(status_code=400, detail="Symbol not in approved universe.")
    _tc = float(getattr(settings, "IRON_CONDOR_TRADING_CAPITAL_DEFAULT", 500_000.0))
    _slots = int(getattr(settings, "IRON_CONDOR_TARGET_POSITION_SLOTS", 5))
    _pct = 3.0 if _slots >= 5 else 5.0
    capital_estimate = (_tc * _pct) / 100.0 if _tc > 0 else 0.0
    chk_out = chk.run_pre_entry_checklist(
        db,
        int(user.id),
        und,
        sec,
        capital_estimate,
        body.declared_next_earnings_iso,
    )
    return {"success": True, **chk_out}


@router.get("/workspace")
def workspace(
    db: Session = Depends(get_db),
    user: User = Depends(_auth),
) -> Dict[str, Any]:
    ic.ensure_iron_condor_tables()
    st = ic.get_or_create_settings(db, int(user.id))
    raw_positions = ic.list_positions(db, int(user.id))
    raw_alerts = ic.recent_alerts(db, int(user.id), 80)
    positions = [_ser(r) for r in ice.merge_positions_peak_alert_severity(raw_positions, raw_alerts)]
    alerts = [_ser(r) for r in raw_alerts]
    dash = ice.dashboard_summary(db, int(user.id))
    sess = mh._normalize_ist(None)
    return {
        "settings": _ser(dict(st)),
        "positions": positions,
        "alerts": alerts,
        "dashboard": _ser(dash),
        "market_poll_active": ice.is_iron_condor_poll_window_ist(sess),
    }


@router.put("/settings")
def put_settings(
    body: SettingsBody,
    db: Session = Depends(get_db),
    user: User = Depends(_auth),
) -> Dict[str, Any]:
    patch = body.model_dump(exclude_unset=True)
    row = ic.update_settings(db, int(user.id), patch)
    return {"settings": _ser(dict(row))}


@router.post("/analyze")
def analyze(
    body: AnalyzeBody,
    db: Session = Depends(get_db),
    user: User = Depends(_auth),
) -> Dict[str, Any]:
    try:
        out = ic.analyze_iron_condor(body.underlying.strip(), db, int(user.id))
        return {"success": True, "analysis": out}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=502, detail=str(e))


@router.post("/analyze-detailed")
def analyze_detailed(
    body: AnalyzeDetailedBody,
    db: Session = Depends(get_db),
    user: User = Depends(_auth),
) -> Dict[str, Any]:
    try:
        out = ice.analyze_iron_condor_detailed(
            body.underlying.strip(), db, int(user.id), strike_overrides=body.strike_overrides
        )
        return {"success": True, "analysis": out}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=502, detail=str(e))


@router.post("/confirm-entry")
def confirm_entry(
    body: ConfirmEntryBody,
    db: Session = Depends(get_db),
    user: User = Depends(_auth),
) -> Dict[str, Any]:
    req = {"sell_call_fill", "buy_call_fill", "sell_put_fill", "buy_put_fill"}
    if not req.issubset(set(body.fills.keys())):
        raise HTTPException(status_code=400, detail="fills must include all 4 legs.")
    if not body.placed_orders_confirmed:
        raise HTTPException(
            status_code=400,
            detail='Confirm checkbox: "I placed all four legs in Upstox."',
        )
    try:
        ana = dict(body.analysis)
        if body.declared_next_earnings_iso:
            ana["declared_next_earnings_iso"] = body.declared_next_earnings_iso.strip()
        pack: Dict[str, Any] = {
            "analysis": ana,
            "fills": body.fills,
            "lot_size": body.lot_size,
            "num_lots": body.num_lots or 1,
            "declared_next_earnings_iso": body.declared_next_earnings_iso.strip() if body.declared_next_earnings_iso else None,
        }
        pid = ice.confirm_entry_iron_condor(db, int(user.id), pack)
        if not pid:
            raise HTTPException(status_code=500, detail="Could not persist position.")
        return {"success": True, "position_id": pid}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/positions")
def save_position(
    body: SaveBody,
    db: Session = Depends(get_db),
    user: User = Depends(_auth),
) -> Dict[str, Any]:
    try:
        pid = ic.persist_position_from_analysis(db, int(user.id), body.analysis)
        if not pid:
            raise HTTPException(status_code=500, detail="Failed to persist position")
        return {"success": True, "position_id": pid}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/poll")
def poll_workspace(
    db: Session = Depends(get_db),
    user: User = Depends(_auth),
) -> Dict[str, Any]:
    """Call every ~5 minutes when market open."""
    ic.ensure_iron_condor_tables()
    out = ice.poll_user_iron_condors(db, int(user.id))
    return {"success": True, **_ser(out)}


@router.post("/positions/{position_id}/close")
def close_position_route(
    position_id: int,
    db: Session = Depends(get_db),
    user: User = Depends(_auth),
) -> Dict[str, Any]:
    ok = ic.close_position(db, int(user.id), position_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Open/active position not found")
    return {"success": True}


@router.post("/positions/{position_id}/evaluate-alerts")
def evaluate_route(
    position_id: int,
    db: Session = Depends(get_db),
    user: User = Depends(_auth),
) -> Dict[str, Any]:
    fires = ic.evaluate_position_alerts(db, int(user.id), position_id)
    return {"success": True, "new_alerts": fires}


@router.post("/close-with-journal")
def close_with_journal_route(
    body: CloseJournalBody,
    db: Session = Depends(get_db),
    user: User = Depends(_auth),
) -> Dict[str, Any]:
    if not body.squaring_confirmed:
        raise HTTPException(status_code=400, detail='Confirm that you squared off in Upstox ("I exited in broker").')
    ok = ice.close_with_journal(db, int(user.id), body.model_dump())
    if not ok:
        raise HTTPException(status_code=404, detail="Position not found")
    return {"success": True}


@router.post("/alerts/{alert_id}/acknowledge")
def ack_alert(
    alert_id: int,
    db: Session = Depends(get_db),
    user: User = Depends(_auth),
) -> Dict[str, Any]:
    ok = ice.acknowledge_alert(db, int(user.id), alert_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Alert not found or already acknowledged")
    return {"success": True}


@router.get("/trade-journal")
def trade_journal(limit: int = Query(80, ge=1, le=500), db: Session = Depends(get_db), user: User = Depends(_auth)) -> Dict[str, Any]:
    ice.iron_condor_migrations_v2()
    rows = db.execute(
        text(
            """
            SELECT j.* FROM iron_condor_trade_journal j
            WHERE j.user_id = :uid
            ORDER BY j.created_at DESC
            LIMIT :lim
            """
        ),
        {"uid": int(user.id), "lim": limit},
    ).mappings().all()
    return {"rows": [_ser(dict(r)) for r in rows]}


@router.get("/dashboard-summary")
def dashboard_summary(db: Session = Depends(get_db), user: User = Depends(_auth)) -> Dict[str, Any]:
    return {"success": True, **_ser(ice.dashboard_summary(db, int(user.id)))}


@router.get("/equity-curve")
def equity_curve(months: int = Query(36, ge=3, le=120), db: Session = Depends(get_db), user: User = Depends(_auth)) -> Dict[str, Any]:
    return {"success": True, "points": ice.equity_curve_realized(db, int(user.id), limit_months=months)}


@router.post("/positions/{position_id}/log-adjustment")
def log_adjustment(
    position_id: int,
    body: LogAdjustmentBody,
    db: Session = Depends(get_db),
    user: User = Depends(_auth),
) -> Dict[str, Any]:
    try:
        payload = {"strikes": body.strikes, "fills": body.fills, "notes": body.notes}
        ok = ice.log_adjustment_iron_condor(db, int(user.id), position_id, payload)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    if not ok:
        raise HTTPException(status_code=404, detail="Position not found or not active")
    return {"success": True}


@router.get("/price-history/{position_id}")
def price_history(
    position_id: int,
    limit: int = Query(200, ge=1, le=2000),
    db: Session = Depends(get_db),
    user: User = Depends(_auth),
) -> Dict[str, Any]:
    ice.iron_condor_migrations_v2()
    owns = db.execute(
        text("SELECT 1 FROM iron_condor_position WHERE id=:p AND user_id=:u LIMIT 1"),
        {"p": position_id, "u": int(user.id)},
    ).scalar()
    if not owns:
        raise HTTPException(status_code=404, detail="Position not found")
    rows = db.execute(
        text(
            """
            SELECT * FROM iron_condor_price_history
            WHERE position_id=:p AND user_id=:u ORDER BY ts DESC LIMIT :lim
            """
        ),
        {"p": position_id, "u": int(user.id), "lim": limit},
    ).mappings().all()
    return {"points": [_ser(dict(r)) for r in reversed(rows)]}


@router.get("/health-public")
def health_public() -> Dict[str, str]:
    return {"module": "iron-condor", "mode": "advisory"}

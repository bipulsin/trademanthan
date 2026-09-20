"""Kosmic Tarang API — Phase 3 paper lifecycle, In-Trade, ExitEngine, Report (admin-only)."""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, File, Header, HTTPException, Query, Request, Response, UploadFile
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from backend.database import SessionLocal, get_db
from backend.models.user import User
from backend.routers.auth import get_user_from_token, oauth2_scheme
from backend.services.tarang.backtest import run_backtest
from backend.services.tarang.backup import run_backup
from backend.services.tarang.bhavcopy import checksum_from_html, import_bhavcopy_text, scan_datewise_dir
from backend.services.tarang.delta_history import archive_expired_options, load_underlying_1h
from backend.services.tarang.mcx_download import run_mcx_bhavcopy_download
from backend.services.tarang.pipeline_health import pipeline_health
from backend.services.tarang.chain_builder import ChainBuilder
from backend.services.tarang.chain_snapshots import capture_chain_snapshots
from backend.services.tarang.config import get_events, get_profiles, get_risk
from backend.services.tarang.eod_reconstruct import liquidity_report, persist_reconstruction
from backend.services.tarang.premise_test import load_cached_premise, run_premise_test
from backend.services.tarang.events import list_alerts
from backend.services.tarang.expiry_eligibility import expiry_eligibility
from backend.services.tarang.health import build_health_payload
from backend.services.tarang.iv_snapshots import capture_iv_snapshots
from backend.services.tarang.broker_verify import verify_trade_against_broker
from backend.services.tarang.labels import auto_lock_label, mode_badge
from backend.services.tarang.lifecycle import (
    add_trade_note,
    build_in_trade_view,
    dismiss_candidate,
    exclude_forward_test,
    exit_all_open,
    exit_trade,
    get_trade,
    list_open_trades,
    mark_taken_manual,
    record_live_user_trade,
    run_auto_paper_once,
    run_exit_engine_once,
    set_paper_auto,
    take_trade_paper,
)
from backend.services.tarang.live_broker import place_or_shadow
from backend.services.tarang.live_control import LivePlacementDisabled, tarang_live_enabled
from backend.services.tarang.report import build_report, report_csv
from backend.services.tarang.schema import ensure_tarang_tables
from backend.services.tarang.screener import (
    get_candidate,
    list_recent_candidates,
    list_rejections,
    run_screener,
)
from backend.services.tarang.sizing import min_max_loss_table
from backend.services.tarang.ticket import build_ticket

router = APIRouter(tags=["tarang"])
logger = logging.getLogger(__name__)


def _auth(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)) -> User:
    return get_user_from_token(token, db)


def _require_admin(user: User = Depends(_auth)) -> User:
    if (getattr(user, "is_admin", None) or "").strip() != "Yes":
        raise HTTPException(status_code=403, detail="Administrator only")
    return user


class AutoBody(BaseModel):
    enabled: bool = False


class ExcludeBody(BaseModel):
    reason: str


class TelegramSettingsBody(BaseModel):
    telegram_public_signals: Optional[bool] = None
    telegram_private_chat_id: Optional[str] = None


class BhavcopyBackfillBody(BaseModel):
    start: str
    include_full: bool = False


class TakeBody(BaseModel):
    note: str = ""
    holding_mode: str = "INTRADAY"


class ManualBody(BaseModel):
    note: str = ""
    fills: Dict[str, Any] = Field(default_factory=dict)


class ExitBody(BaseModel):
    reason: str = "MANUAL"
    note: str = ""


class NoteBody(BaseModel):
    note: str


class LiveFillBody(BaseModel):
    note: str = ""
    holding_mode: str = "INTRADAY"
    fills: List[Dict[str, Any]] = Field(default_factory=list)


class LivePlaceBody(BaseModel):
    payload: Dict[str, Any] = Field(default_factory=dict)


@router.get("/health")
def tarang_health(_user: User = Depends(_require_admin)) -> Dict[str, Any]:
    ensure_tarang_tables()
    return build_health_payload()


@router.get("/config")
def tarang_config(_user: User = Depends(_require_admin)) -> Dict[str, Any]:
    return {
        "profiles": get_profiles(),
        "risk": get_risk(),
        "events": get_events(),
    }


@router.get("/min-max-loss")
def tarang_min_max_loss(_user: User = Depends(_require_admin)) -> Dict[str, Any]:
    return min_max_loss_table()


@router.get("/chain/{profile_id}")
def tarang_chain(
    profile_id: str,
    expiry: Optional[str] = None,
    _user: User = Depends(_require_admin),
) -> Dict[str, Any]:
    ensure_tarang_tables()
    chain = ChainBuilder().build(profile_id.upper(), expiry=expiry)
    return {
        "profile_id": chain.profile_id,
        "venue": chain.venue,
        "underlying": chain.underlying,
        "expiry": chain.expiry,
        "futures_or_spot": chain.futures_or_spot,
        "lot_size": chain.lot_size,
        "strike_step": chain.strike_step,
        "built_at": chain.built_at,
        "meta": chain.meta,
        "quotes": [q.to_dict() for q in chain.quotes],
    }


@router.post("/iv-snapshots/run")
def tarang_iv_run(_user: User = Depends(_require_admin)) -> Dict[str, Any]:
    return capture_iv_snapshots()


@router.post("/chain-snapshots/run")
def tarang_chain_snap_run(
    venue: Optional[str] = None,
    _user: User = Depends(_require_admin),
) -> Dict[str, Any]:
    venues = [venue] if venue else None
    return capture_chain_snapshots(venues=venues, respect_mcx_session=True)


@router.get("/eligibility")
def tarang_eligibility(_user: User = Depends(_require_admin)) -> Dict[str, Any]:
    return expiry_eligibility()


@router.get("/backtest")
def tarang_backtest_get(
    source: str = "snapshots",
    fill_mode: str = "base",
    _user: User = Depends(_require_admin),
) -> Dict[str, Any]:
    return run_backtest(source=source, fill_mode=fill_mode)


@router.post("/backtest/run")
def tarang_backtest_run(
    source: str = "snapshots",
    fill_mode: str = "base",
    _user: User = Depends(_require_admin),
) -> Dict[str, Any]:
    return run_backtest(source=source, fill_mode=fill_mode)


@router.post("/bhavcopy/import")
async def tarang_bhavcopy_import(
    file: UploadFile = File(...),
    _user: User = Depends(_require_admin),
) -> Dict[str, Any]:
    ensure_tarang_tables()
    raw = await file.read()
    text = raw.decode("utf-8", errors="replace")
    return import_bhavcopy_text(text, filename=file.filename or "upload.csv")


@router.post("/bhavcopy/checksum")
async def tarang_bhavcopy_checksum(
    file: Optional[UploadFile] = File(None),
    _user: User = Depends(_require_admin),
) -> Dict[str, Any]:
    ensure_tarang_tables()
    if file is None:
        return scan_datewise_dir()
    raw = await file.read()
    html = raw.decode("utf-8", errors="replace")
    return checksum_from_html(html, filename=file.filename or "")


@router.get("/bhavcopy/checksum/scan")
def tarang_bhavcopy_checksum_scan(_user: User = Depends(_require_admin)) -> Dict[str, Any]:
    ensure_tarang_tables()
    return scan_datewise_dir()


@router.post("/bhavcopy/download")
def tarang_bhavcopy_download(_user: User = Depends(_require_admin)) -> Dict[str, Any]:
    return run_mcx_bhavcopy_download()


@router.post("/bhavcopy/backfill")
def tarang_bhavcopy_backfill(body: BhavcopyBackfillBody, _user: User = Depends(_require_admin)) -> Dict[str, Any]:
    from datetime import date as date_cls

    try:
        start = date_cls.fromisoformat(body.start)
    except ValueError:
        raise HTTPException(status_code=400, detail="start must be YYYY-MM-DD")
    return run_mcx_bhavcopy_download(backfill_start=start, include_full=body.include_full)


@router.post("/bhavcopy/reconstruct")
def tarang_bhavcopy_reconstruct(_user: User = Depends(_require_admin)) -> Dict[str, Any]:
    return persist_reconstruction()


@router.get("/premise-test")
def tarang_premise_get(_user: User = Depends(_require_admin)) -> Dict[str, Any]:
    return load_cached_premise()


@router.post("/premise-test/run")
def tarang_premise_run(_user: User = Depends(_require_admin)) -> Dict[str, Any]:
    return run_premise_test()


@router.get("/bhavcopy/liquidity")
def tarang_bhavcopy_liquidity(_user: User = Depends(_require_admin)) -> Dict[str, Any]:
    return liquidity_report()


@router.post("/settings/auto")
def tarang_set_auto(body: AutoBody, _user: User = Depends(_require_admin)) -> Dict[str, Any]:
    out = set_paper_auto(bool(body.enabled))
    if not out.get("ok"):
        raise HTTPException(status_code=400, detail=out.get("error") or out)
    return out


@router.post("/auto/run")
def tarang_auto_run(_user: User = Depends(_require_admin)) -> Dict[str, Any]:
    out = run_auto_paper_once()
    if not out.get("ok") and out.get("error") not in ("auto_paper_only",):
        # auto_off is ok True; kill_switch returns ok False
        if out.get("error") == "kill_switch":
            raise HTTPException(status_code=409, detail="kill_switch")
    return out


@router.post("/screener/run")
def tarang_screener_run(
    profile_id: Optional[str] = None,
    _user: User = Depends(_require_admin),
) -> Dict[str, Any]:
    ensure_tarang_tables()
    ids = [profile_id.upper()] if profile_id else None
    return run_screener(ids)


@router.get("/screener")
def tarang_screener_latest(_user: User = Depends(_require_admin)) -> Dict[str, Any]:
    ensure_tarang_tables()
    cands = list_recent_candidates(80)
    by: Dict[str, Any] = {}
    for c in cands:
        pid = c["profile_id"]
        if pid not in by:
            by[pid] = c
    from backend.services.tarang.lifecycle import get_mode_auto

    ma = get_mode_auto()
    return {
        "product": "Kosmic Tarang",
        "phase": 3,
        "mode": ma.get("mode") or "PAPER",
        "display_mode": mode_badge(ma.get("mode")),
        "display_auto": auto_lock_label(),
        "auto": False,
        "by_profile": by,
        "recent": cands[:20],
    }


@router.get("/candidates")
def tarang_candidates(
    limit: int = 40,
    _user: User = Depends(_require_admin),
) -> Dict[str, Any]:
    return {"candidates": list_recent_candidates(min(limit, 100))}


@router.get("/candidates/{candidate_id}")
def tarang_candidate(candidate_id: int, _user: User = Depends(_require_admin)) -> Dict[str, Any]:
    c = get_candidate(candidate_id)
    if not c:
        raise HTTPException(status_code=404, detail="Candidate not found")
    return c


@router.get("/rejections")
def tarang_rejections(
    limit: int = 50,
    profile_id: Optional[str] = None,
    _user: User = Depends(_require_admin),
) -> Dict[str, Any]:
    return {"rejections": list_rejections(min(limit, 200), profile_id=profile_id)}


@router.get("/ticket/{candidate_id}")
def tarang_ticket(candidate_id: int, _user: User = Depends(_require_admin)) -> Dict[str, Any]:
    t = build_ticket(candidate_id)
    if not t.get("ok"):
        raise HTTPException(status_code=404, detail=t.get("error") or "not found")
    return t


@router.post("/ticket/{candidate_id}/take")
def tarang_ticket_take(
    candidate_id: int,
    body: TakeBody = TakeBody(),
    _user: User = Depends(_require_admin),
) -> Dict[str, Any]:
    out = take_trade_paper(candidate_id, note=body.note or "", holding_mode=body.holding_mode or "INTRADAY")
    if not out.get("ok"):
        raise HTTPException(status_code=400, detail=out.get("error") or out)
    return out


@router.post("/ticket/{candidate_id}/dismiss")
def tarang_ticket_dismiss(candidate_id: int, _user: User = Depends(_require_admin)) -> Dict[str, Any]:
    out = dismiss_candidate(candidate_id)
    if not out.get("ok"):
        raise HTTPException(status_code=404, detail=out.get("error"))
    return out


@router.post("/ticket/{candidate_id}/mark-manual")
def tarang_ticket_manual(
    candidate_id: int,
    body: ManualBody = ManualBody(),
    _user: User = Depends(_require_admin),
) -> Dict[str, Any]:
    out = mark_taken_manual(candidate_id, fills=body.fills, note=body.note or "")
    if not out.get("ok"):
        raise HTTPException(status_code=400, detail=out.get("error") or out)
    return out


@router.post("/ticket/{candidate_id}/record-live")
def tarang_ticket_record_live(
    candidate_id: int,
    body: LiveFillBody = LiveFillBody(),
    _user: User = Depends(_require_admin),
) -> Dict[str, Any]:
    out = record_live_user_trade(
        candidate_id,
        fills=list(body.fills or []),
        note=body.note or "",
        holding_mode=body.holding_mode or "INTRADAY",
    )
    if not out.get("ok"):
        raise HTTPException(status_code=400, detail=out.get("error") or out)
    return out


@router.post("/trades/{trade_id}/verify")
def tarang_trade_verify(trade_id: int, _user: User = Depends(_require_admin)) -> Dict[str, Any]:
    return verify_trade_against_broker(trade_id)


@router.post("/live/place")
def tarang_live_place(body: LivePlaceBody = LivePlaceBody(), _user: User = Depends(_require_admin)) -> Dict[str, Any]:
    """Fail-closed. Shadow-logs payload; never transmits when TARANG_LIVE_ENABLED is false."""
    out = place_or_shadow(dict(body.payload or {}))
    return {
        "ok": False,
        "error": "live_placement_disabled",
        "sent": False,
        "shadow": out,
        "live_enabled": tarang_live_enabled(),
    }


# --- Phase 3: In-Trade / Exit / Report ---


@router.get("/trades/open")
def tarang_trades_open(_user: User = Depends(_require_admin)) -> Dict[str, Any]:
    ensure_tarang_tables()
    return {"display_mode": mode_badge("PAPER"), "mode": "PAPER", "trades": list_open_trades()}


@router.get("/trades/{trade_id}")
def tarang_trade_get(trade_id: int, _user: User = Depends(_require_admin)) -> Dict[str, Any]:
    t = get_trade(trade_id)
    if not t:
        raise HTTPException(status_code=404, detail="Trade not found")
    return t


@router.get("/in-trade")
def tarang_in_trade_list(_user: User = Depends(_require_admin)) -> Dict[str, Any]:
    ensure_tarang_tables()
    opens = list_open_trades()
    views = []
    for t in opens:
        v = build_in_trade_view(int(t["id"]), refresh_quotes=True)
        if v.get("ok"):
            views.append(v)
    db = SessionLocal()
    try:
        alerts = list_alerts(db, limit=30)
    finally:
        db.close()
    return {
        "product": "Kosmic Tarang",
        "phase": 3,
        "mode": "PAPER",
        "display_mode": mode_badge("PAPER"),
        "display_auto": auto_lock_label(),
        "trades": views,
        "alerts": alerts,
    }


@router.get("/in-trade/{trade_id}")
def tarang_in_trade_one(trade_id: int, _user: User = Depends(_require_admin)) -> Dict[str, Any]:
    v = build_in_trade_view(trade_id, refresh_quotes=True)
    if not v.get("ok"):
        raise HTTPException(status_code=404, detail=v.get("error") or "not found")
    return v


@router.post("/trades/{trade_id}/exit")
def tarang_trade_exit(
    trade_id: int,
    body: ExitBody = ExitBody(),
    _user: User = Depends(_require_admin),
) -> Dict[str, Any]:
    out = exit_trade(trade_id, reason=body.reason or "MANUAL", actor="USER", note=body.note or "")
    if not out.get("ok"):
        raise HTTPException(status_code=400, detail=out.get("error") or out)
    return out


@router.post("/trades/exit-all")
def tarang_exit_all(
    body: ExitBody = ExitBody(),
    _user: User = Depends(_require_admin),
) -> Dict[str, Any]:
    return exit_all_open(reason=body.reason or "MANUAL", actor="USER")


@router.post("/trades/{trade_id}/note")
def tarang_trade_note(
    trade_id: int,
    body: NoteBody,
    _user: User = Depends(_require_admin),
) -> Dict[str, Any]:
    out = add_trade_note(trade_id, body.note)
    if not out.get("ok"):
        raise HTTPException(status_code=404, detail=out.get("error"))
    return out


@router.post("/exit-engine/run")
def tarang_exit_engine_run(_user: User = Depends(_require_admin)) -> Dict[str, Any]:
    return run_exit_engine_once()


@router.get("/alerts")
def tarang_alerts(limit: int = 40, _user: User = Depends(_require_admin)) -> Dict[str, Any]:
    ensure_tarang_tables()
    db = SessionLocal()
    try:
        return {"alerts": list_alerts(db, limit=min(limit, 100))}
    finally:
        db.close()


@router.get("/report")
def tarang_report(
    mode: str = "FORWARD_TEST",
    book: Optional[str] = None,
    profile_id: Optional[str] = None,
    limit: int = 100,
    _user: User = Depends(_require_admin),
) -> Dict[str, Any]:
    ensure_tarang_tables()
    book_u = (book or mode or "FORWARD_TEST").upper()
    if book_u in ("PAPER",):
        book_u = "FORWARD_TEST"
    if book_u not in ("FORWARD_TEST", "LIVE", "ALL", "PAPER"):
        raise HTTPException(status_code=400, detail="book must be FORWARD_TEST, LIVE, or ALL")
    return build_report(mode=book_u, profile_id=profile_id, limit=min(limit, 500), book=book_u)


@router.get("/report.csv")
def tarang_report_csv(
    mode: str = "FORWARD_TEST",
    book: Optional[str] = None,
    _user: User = Depends(_require_admin),
) -> Response:
    ensure_tarang_tables()
    book_u = (book or mode or "FORWARD_TEST").upper()
    csv_text = report_csv(mode=book_u, book=book_u)
    return Response(
        content=csv_text,
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="tarang_report_{book_u}.csv"'},
    )


@router.get("/pipeline")
def tarang_pipeline(_user: User = Depends(_require_admin)) -> Dict[str, Any]:
    return pipeline_health()


@router.post("/trades/{trade_id}/exclude")
def tarang_trade_exclude(
    trade_id: int,
    body: ExcludeBody,
    _user: User = Depends(_require_admin),
) -> Dict[str, Any]:
    out = exclude_forward_test(trade_id, body.reason or "", actor=getattr(_user, "username", None) or "admin")
    if not out.get("ok"):
        raise HTTPException(status_code=400, detail=out.get("error") or out)
    return out


@router.post("/delta/candles/run")
def tarang_delta_candles(_user: User = Depends(_require_admin)) -> Dict[str, Any]:
    return load_underlying_1h()


@router.post("/delta/archive/run")
def tarang_delta_archive(_user: User = Depends(_require_admin)) -> Dict[str, Any]:
    return archive_expired_options()


@router.get("/rv-compare")
def tarang_rv_compare(_user: User = Depends(_require_admin)) -> Dict[str, Any]:
    from backend.services.tarang.rv import rv_compare, rv_for_profile

    return {
        "gate_uses": "daily_close_to_close",
        "gate_rv": {p: rv_for_profile(p) for p in ("BTC", "ETH", "CL", "NG")},
        "btc": rv_compare("BTCUSD"),
        "eth": rv_compare("ETHUSD"),
        "crudeoilm": rv_compare("CRUDEOILM"),
        "natgasmini": rv_compare("NATGASMINI"),
    }


@router.get("/spreads/delta-scan")
def tarang_delta_spread_scan(live: bool = False, _user: User = Depends(_require_admin)) -> Dict[str, Any]:
    from backend.services.tarang.spread_scan import scan_latest_delta_window

    return scan_latest_delta_window(live=live)


@router.post("/mcx/futures-candles/run")
def tarang_mcx_futures_candles(_user: User = Depends(_require_admin)) -> Dict[str, Any]:
    from backend.services.tarang.mcx_futures_candles import load_mcx_futures_daily

    return load_mcx_futures_daily()


@router.post("/backup/run")
def tarang_backup_run(dry_run: bool = True, _user: User = Depends(_require_admin)) -> Dict[str, Any]:
    return run_backup(dry_run=dry_run)


@router.get("/telegram/settings")
def tarang_telegram_settings(_user: User = Depends(_require_admin)) -> Dict[str, Any]:
    from backend.services.tarang.alerts_telegram import private_chat_id, public_signals_enabled, start_link

    db = SessionLocal()
    try:
        return {
            "start_link": start_link(),
            "private_chat_id": private_chat_id(db),
            "telegram_public_signals": public_signals_enabled(db),
            "how_to": "Open start_link, press Start on the bot, then Poll link. Bot cannot DM by username.",
        }
    finally:
        db.close()


@router.post("/telegram/settings")
def tarang_telegram_settings_set(body: TelegramSettingsBody, _user: User = Depends(_require_admin)) -> Dict[str, Any]:
    import json as json_lib

    from sqlalchemy import text as sql_text

    from backend.services.tarang.alerts_telegram import save_private_chat_id

    db = SessionLocal()
    try:
        if body.telegram_public_signals is not None:
            db.execute(
                sql_text(
                    """
                    INSERT INTO tarang_settings (key, value) VALUES ('telegram_public_signals', CAST(:v AS jsonb))
                    ON CONFLICT (key) DO UPDATE SET value = CAST(:v AS jsonb)
                    """
                ),
                {"v": json_lib.dumps(bool(body.telegram_public_signals))},
            )
        if body.telegram_private_chat_id:
            save_private_chat_id(db, str(body.telegram_private_chat_id))
        db.commit()
        return {"ok": True}
    finally:
        db.close()


@router.post("/telegram/poll-link")
def tarang_telegram_poll(_user: User = Depends(_require_admin)) -> Dict[str, Any]:
    from backend.services.tarang.alerts_telegram import poll_and_link

    return poll_and_link()


@router.post("/telegram/test")
def tarang_telegram_test(_user: User = Depends(_require_admin)) -> Dict[str, Any]:
    from backend.services.tarang.alerts_telegram import send_test_alert

    return send_test_alert()


@router.get("/evals/first-session")
def tarang_first_session(
    after: str,
    underlying: Optional[str] = None,
    _user: User = Depends(_require_admin),
) -> Dict[str, Any]:
    from backend.services.tarang.evals import first_in_session_evals

    unds = [u.strip() for u in underlying.split(",")] if underlying else None
    return first_in_session_evals(after_date=after, underlyings=unds)


@router.post("/telegram/webhook")
async def tarang_telegram_webhook(
    request: Request,
    secret: Optional[str] = Query(None),
    x_telegram_bot_api_secret_token: Optional[str] = Header(None),
) -> Dict[str, Any]:
    import os

    expected = (os.getenv("TARANG_TELEGRAM_WEBHOOK_SECRET") or os.getenv("TELEGRAM_WEBHOOK_SECRET") or "").strip()
    got = (secret or x_telegram_bot_api_secret_token or "").strip()
    if expected and got != expected:
        raise HTTPException(status_code=403, detail="bad webhook secret")
    from backend.services.tarang.alerts_telegram import handle_telegram_update

    body = await request.json()
    return handle_telegram_update(body if isinstance(body, dict) else {})


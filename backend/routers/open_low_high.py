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
    """Today's live/frozen screen state. Phase A greys from ~09:20; Phase B 09:25–09:29."""
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


@router.get("/ltp")
def get_ltp(_user: User = Depends(_auth_user)) -> Dict[str, Any]:
    """Live LTPs for today's screen keys (reference only — Entry stays frozen)."""
    from backend.services.open_low_high.persistence import list_screen_for_date
    from backend.services.open_low_high.screener import _enrich_ltp, _today

    rows = _enrich_ltp(list_screen_for_date(_today()))
    by_symbol = {
        str(r.get("symbol") or "").upper(): {
            "symbol": r.get("symbol"),
            "instrument_key": r.get("instrument_key"),
            "ltp": r.get("ltp"),
            "entry": r.get("entry"),  # frozen screen snapshot; not live LTP
            "active": bool(r.get("active")),
            "side": r.get("side"),
        }
        for r in rows
    }
    return {"ok": True, "trade_date": _today().isoformat(), "by_symbol": by_symbol}


@router.get("/health")
def health() -> Dict[str, Any]:
    """OLH-specific health (token, WS EQ+INDEX, PDC 145+Nifty, scheduler jobs).

    Webhook / IntradayStockOption alert volume stays on ``/scan/health`` only.
    """
    from backend.services.open_low_high.persistence import ensure_tables, get_day_meta
    from backend.services.open_low_high.pdc import pdc_completeness
    from backend.services.open_low_high.scheduler import _scheduler
    from backend.services.open_low_high.screener import _today, window_status
    from backend.services.open_low_high.ws_feed import check_equity_index_ws_warm

    try:
        ensure_tables()
        trade_date = _today()
        meta = get_day_meta(trade_date)

        token: Dict[str, Any] = {"ok": False, "last4": "none"}
        try:
            from backend.config import settings
            from backend.services.upstox_service import UpstoxService

            ux = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
            ux.reload_token_from_storage()
            tok = str(getattr(ux, "access_token", None) or "")
            token = {
                "ok": bool(tok),
                "last4": tok[-4:] if len(tok) >= 4 else ("****" if tok else "none"),
            }
        except Exception as tok_exc:
            token = {"ok": False, "last4": "none", "error": str(tok_exc)[:160]}

        ws_coverage: Dict[str, Any] = {"ok": False}
        try:
            warm = check_equity_index_ws_warm(session=trade_date)
            ws_coverage = {
                "ok": bool(warm.get("warm")),
                "token_ok": warm.get("token_ok"),
                "equity_keys": warm.get("equity_keys"),
                "index_keys": warm.get("index_keys"),
                "equity_with_bars": warm.get("equity_with_bars"),
                "equity_share": warm.get("equity_share"),
                "nifty_1m_bars": warm.get("nifty_1m_bars"),
                "feed_alive": warm.get("feed_alive"),
                "provider_keys": warm.get("provider_keys"),
                "reason": warm.get("reason"),
            }
        except Exception as ws_exc:
            ws_coverage = {"ok": False, "error": str(ws_exc)[:160]}

        pdc: Dict[str, Any] = {"ok": False}
        try:
            pdc = pdc_completeness(session=trade_date)
        except Exception as pdc_exc:
            pdc = {"ok": False, "error": str(pdc_exc)[:160]}

        jobs: list[Dict[str, Any]] = []
        scheduler_running = False
        required_job_prefixes = (
            "open_low_high_pdc_",
            "open_low_high_ws_",
            "open_low_high_phase_a_",
            "open_low_high_phase_b_",
            "open_low_high_rest_canary_",
        )
        try:
            if _scheduler is not None:
                scheduler_running = bool(getattr(_scheduler, "running", False))
                for j in sorted(_scheduler.get_jobs(), key=lambda x: x.id):
                    jobs.append(
                        {
                            "id": j.id,
                            "name": j.name,
                            "trigger": str(j.trigger),
                            "next_run_time": (
                                j.next_run_time.isoformat() if j.next_run_time else None
                            ),
                        }
                    )
        except Exception as sched_exc:
            logger.debug("open_low_high health scheduler dump failed: %s", sched_exc)

        job_ids = {j["id"] for j in jobs}
        missing_groups = [
            p for p in required_job_prefixes if not any(jid.startswith(p) for jid in job_ids)
        ]
        canary_jobs = sorted(jid for jid in job_ids if jid.startswith("open_low_high_rest_canary_"))
        scheduler_ok = bool(scheduler_running) and not missing_groups

        olh_ok = bool(token.get("ok")) and bool(pdc.get("ok")) and scheduler_ok
        # WS coverage is reported always; after hours / weekend warm may be false —
        # do not fail overall solely on idle WS bars outside the pre-warm window.
        return {
            "ok": olh_ok,
            "service": "open_low_high",
            "window": window_status(),
            "trade_date": trade_date.isoformat(),
            "meta": meta,
            "token": token,
            "ws_coverage": ws_coverage,
            "prev_close": pdc,
            "scheduler": {
                "running": scheduler_running,
                "ok": scheduler_ok,
                "job_count": len(jobs),
                "missing_groups": missing_groups,
                "canary_jobs": canary_jobs,
                "jobs": jobs,
            },
            "notes": (
                "OLH health: token + PDC (145+Nifty) + scheduler jobs. "
                "Webhook/alert volume is on /scan/health only."
            ),
        }
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


@router.post("/test-alert")
def test_alert(_user: User = Depends(_auth_user)) -> Dict[str, Any]:
    """Send a one-shot OLH alert on the TradeWithCTO canary/coverage channel."""
    from backend.services.open_low_high.alerts import send_olh_alert

    subject = "TEST alert (manual)"
    body = "Manual OLH channel check — same path as 09:05 canary / 09:21–09:26 coverage."
    sent = send_olh_alert(subject, body, meta={"source": "test-alert"})
    return {"ok": bool(sent), "sent": bool(sent), "subject": subject}

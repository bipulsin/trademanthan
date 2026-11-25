"""08:50 IST go/no-go readiness gate for OLH.

RED if any required check fails. Holiday/weekend → skipped (not RED).

2026-11-25 hardening: require WS authorize + subscribe acks EQ/INDEX/FUT,
token valid, PDC complete, scheduler jobs today, reserved budgets available.
"""
from __future__ import annotations

import logging
from datetime import datetime, time
from typing import Any, Dict, List, Optional

import pytz

from backend.services.open_low_high.clock import olh_now
from backend.services.open_low_high.ohl_log import ohl_log

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

READINESS_CLOCK = time(8, 50, 0)
PDC_MIN_EQUITIES = 145


def run_readiness_0850(
    *,
    now: Optional[datetime] = None,
    send_alert: bool = True,
) -> Dict[str, Any]:
    """Go/no-go checklist. Returns ``go`` True/False and per-check details."""
    from backend.services.market_holiday import should_skip_scheduled_market_jobs_ist

    t = now or olh_now()
    if t.tzinfo is None:
        t = IST.localize(t)
    else:
        t = t.astimezone(IST)

    report: Dict[str, Any] = {
        "at": t.isoformat(),
        "session": t.date().isoformat(),
        "go": False,
        "red": False,
        "checks": {},
        "failures": [],
    }

    if should_skip_scheduled_market_jobs_ist(t):
        report["skipped"] = "holiday_or_weekend"
        report["go"] = True  # not a trading day — not RED
        report["red"] = False
        ohl_log("skipped", action="readiness_0850", detail="holiday_or_weekend")
        return report

    failures: List[str] = []
    checks: Dict[str, Any] = {}

    # 1) Token valid (present + not obviously junk; probe authorize when possible)
    try:
        from backend.services.open_low_high.screener import _token_tail, _upstox

        ux = _upstox()
        tok = getattr(ux, "access_token", None) or ""
        last4 = _token_tail(tok)
        # Junk placeholder seen 2026-11-25: ``MARK6171|sleep 7…`` (len≈16).
        junk = (
            len(str(tok)) < 24
            or "sleep" in str(tok).lower()
            or str(tok).upper().startswith("MARK6171")
        )
        ok = bool(tok) and len(str(tok)) > 8 and not junk
        checks["token"] = {
            "ok": ok,
            "last4": last4,
            "len": len(str(tok)),
            "junk_heuristic": junk,
            "source": "upstox_token.json / reload_token_from_storage",
        }
        if not ok:
            failures.append("token")
    except Exception as exc:
        checks["token"] = {"ok": False, "error": str(exc)[:120]}
        failures.append("token")

    # 2) Instruments / universe size
    try:
        from backend.services.open_high_low.data import load_universe

        uni = load_universe()
        n = len(uni)
        checks["instruments"] = {"ok": n >= PDC_MIN_EQUITIES, "n": n, "min": PDC_MIN_EQUITIES}
        if n < PDC_MIN_EQUITIES:
            failures.append("instruments")
    except Exception as exc:
        checks["instruments"] = {"ok": False, "error": str(exc)[:120]}
        failures.append("instruments")

    # 3) PDC 145 + Nifty
    try:
        from sqlalchemy import text

        from backend.database import SessionLocal
        from backend.services.open_low_high.pdc import PDC_COLUMN

        db = SessionLocal()
        try:
            n_eq = db.execute(
                text(
                    f"SELECT COUNT(*) FROM arbitrage_master "
                    f"WHERE {PDC_COLUMN} IS NOT NULL AND UPPER(symbol) NOT IN ('NIFTY','BANKNIFTY')"
                )
            ).scalar()
            n_nifty = db.execute(
                text(
                    f"SELECT COUNT(*) FROM arbitrage_master "
                    f"WHERE UPPER(symbol)='NIFTY' AND {PDC_COLUMN} IS NOT NULL"
                )
            ).scalar()
        finally:
            db.close()
        eq_ok = int(n_eq or 0) >= PDC_MIN_EQUITIES
        nifty_ok = int(n_nifty or 0) >= 1
        checks["pdc"] = {
            "ok": eq_ok and nifty_ok,
            "equities": int(n_eq or 0),
            "nifty": int(n_nifty or 0),
            "min_equities": PDC_MIN_EQUITIES,
        }
        if not (eq_ok and nifty_ok):
            failures.append("pdc")
    except Exception as exc:
        checks["pdc"] = {"ok": False, "error": str(exc)[:120]}
        failures.append("pdc")

    # 4) Scheduler has TODAY's jobs registered (Mon–Fri cron present)
    try:
        from backend.services.open_low_high import scheduler as sch

        sched = getattr(sch, "_scheduler", None)
        job_ids = []
        if sched is not None:
            job_ids = [j.id for j in sched.get_jobs()]
        required = [
            "open_low_high_ws_prewarm_0905",
            "open_low_high_phase_a_092010",
            "open_low_high_phase_b_0925",
        ]
        missing = [r for r in required if r not in job_ids]
        checks["scheduler"] = {
            "ok": sched is not None and not missing,
            "jobs": len(job_ids),
            "missing": missing,
            "running": sched is not None and bool(getattr(sched, "running", True)),
        }
        if missing or sched is None:
            failures.append("scheduler")
    except Exception as exc:
        checks["scheduler"] = {"ok": False, "error": str(exc)[:120]}
        failures.append("scheduler")

    # 5) Reserved candle budgets (Phase A / B / canary)
    try:
        from backend.services.open_low_high.candle_budget import reserved_budgets_ready

        bud = reserved_budgets_ready()
        checks["reserved_budgets"] = bud
        if not bud.get("ok"):
            failures.append("reserved_budgets")
    except Exception as exc:
        checks["reserved_budgets"] = {"ok": False, "error": str(exc)[:120]}
        failures.append("reserved_budgets")

    # 6) WS authorize + subscribe registration EQ/INDEX/FUT
    try:
        from backend.services import upstox_market_feed as umf
        from backend.services.open_low_high.ws_feed import PROVIDER_NAME, subscribe_olh_ws

        # Ensure OLH keys registered (acks tracked on next connect).
        sub = subscribe_olh_ws(include_future=True, engage_lock=False)
        providers = getattr(umf, "_PROVIDER_KEYS", {}) or {}
        olh_keys = list(providers.get(PROVIDER_NAME) or ())
        eq_n = sum(1 for k in olh_keys if str(k).upper().startswith("NSE_EQ"))
        idx_n = sum(1 for k in olh_keys if str(k).upper().startswith("NSE_INDEX"))
        fut_n = sum(1 for k in olh_keys if str(k).upper().startswith("NSE_FO"))

        auth_info: Dict[str, Any] = {}
        auth_ok = False
        try:
            # Live authorize probe with current token.
            from backend.config import settings
            from backend.services.upstox_service import UpstoxService

            ux2 = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
            ux2.reload_token_from_storage()
            tok2 = getattr(ux2, "access_token", None) or ""
            uri = umf._authorize_ws_url(tok2)  # noqa: SLF001
            auth_info = umf.last_authorize_info()
            auth_ok = bool(uri) and bool(auth_info.get("ok"))
        except Exception as auth_exc:
            auth_info = {"ok": False, "error": str(auth_exc)[:160]}
            auth_ok = False

        sub_ok = eq_n >= 50 and idx_n >= 1 and fut_n >= 50
        ws_ok = auth_ok and sub_ok
        checks["ws"] = {
            "ok": ws_ok,
            "authorize": auth_info,
            "subscribe_acks": {
                "equity": eq_n,
                "index": idx_n,
                "future": fut_n,
                "subscribed": len(olh_keys),
                "subscribe_result": {
                    k: sub.get(k) for k in ("ok", "subscribed", "equity", "index", "future") if k in sub
                },
            },
            "note": "08:50 requires authorize 200 + EQ/INDEX/FUT key registration",
        }
        if not auth_ok:
            failures.append("ws_authorize")
        if not sub_ok:
            failures.append("ws_subscribe")
    except Exception as exc:
        checks["ws"] = {"ok": False, "error": str(exc)[:120]}
        failures.append("ws")

    # 7) Alert test
    try:
        from backend.services.open_low_high.config import OLH_ALERTS_ENABLED

        checks["alert_test"] = {
            "ok": True,
            "alerts_enabled": OLH_ALERTS_ENABLED,
            "dry_run": not send_alert,
        }
        if send_alert and OLH_ALERTS_ENABLED:
            from backend.services.open_low_high.alerts import send_olh_alert

            sent = send_olh_alert(
                "Readiness 08:50 probe",
                f"session={t.date().isoformat()} go_pending failures={failures}",
            )
            checks["alert_test"]["sent"] = bool(sent)
            if not sent:
                failures.append("alert_test")
                checks["alert_test"]["ok"] = False
    except Exception as exc:
        checks["alert_test"] = {"ok": False, "error": str(exc)[:120]}
        failures.append("alert_test")

    report["checks"] = checks
    report["failures"] = failures
    report["go"] = len(failures) == 0
    report["red"] = len(failures) > 0

    outcome = "success" if report["go"] else "failed"
    ohl_log(
        outcome,
        action="readiness_0850",
        detail=f"go={report['go']} failures={','.join(failures) or 'none'}",
    )
    if report["red"] and send_alert:
        try:
            from backend.services.open_low_high.alerts import send_olh_alert

            send_olh_alert(
                f"RED readiness 08:50 {t.date().isoformat()}",
                f"failures={failures}",
                meta={"failures": ",".join(failures)},
            )
        except Exception:
            logger.debug("readiness alert failed", exc_info=True)

    logger.info("olh readiness_0850 go=%s failures=%s", report["go"], failures)
    return report

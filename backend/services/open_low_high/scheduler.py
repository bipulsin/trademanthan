"""APScheduler jobs: Open Low High reliability timeline (IST Mon–Fri).

Timeline:
  16:05  EOD ``equity_prev_close`` (PDC) fill on arbitrage_master (+ Nifty)
  09:00  PDC null cross-check vs instrument keys
  09:05  Pre-warm WS Equity / Index / Future (only pre-warm stage)
  09:15  WS warm check for Equity/Index; re-warm if needed; engage WS lock
  09:20:10–09:23  Phase A (fresh 09:15 5m) — persist grey cards immediately
  09:24  Phase A refill for symbols still missing 09:15 OHLC
  09:25:03–09:29  Phase B on Phase A qualifiers only (hard stop 09:29)
  ~12:00  Delete OHL_Log_* older than 3 days

Startup catch-up (explicit — not APScheduler misfire alone):
  09:20–09:29 IST restart with missing Phase A/B → run the same runners immediately.
  After 09:29 with missing/invalid phase → DEGRADED + alert (force/re-screen still allowed).

One-day REST canary (2026-11-20 IST): 09:20:05, :30, 09:21:00, 09:25:05, :30
  — 3 equities + Nifty; logs to OHL_Log / app logger (see ``rest_canary.py``).

WS lock 09:15–09:25: EQ/INDEX provider keys are force-merged and never dropped
from the OLH sidecar subscribe set (see ``ws_feed.py``).
"""
from __future__ import annotations

import logging
import threading
from datetime import datetime, time
from typing import Any, Dict, Optional

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from backend.services.market_holiday import should_skip_scheduled_market_jobs_ist

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")

# Inclusive catch-up window (IST). After 09:29 → degrade path only.
_CATCHUP_START = time(9, 20, 0)
_CATCHUP_END = time(9, 29, 0)

_scheduler: BackgroundScheduler | None = None


def _skip() -> bool:
    if should_skip_scheduled_market_jobs_ist():
        logger.info("open_low_high: skipped holiday/weekend")
        return True
    return False


def _run_pdc_eod() -> None:
    if _skip():
        return
    try:
        from backend.services.open_low_high.pdc import fill_pdc_eod

        result = fill_pdc_eod()
        logger.info(
            "open_low_high PDC EOD status=%s ok=%s fail=%s column=%s",
            result.get("status"),
            result.get("ok_n"),
            result.get("fail_n"),
            result.get("column"),
        )
    except Exception:
        logger.exception("open_low_high PDC EOD failed")


def _run_pdc_0900() -> None:
    if _skip():
        return
    try:
        from backend.services.open_low_high.pdc import crosscheck_pdc_at_0900

        result = crosscheck_pdc_at_0900()
        logger.info(
            "open_low_high PDC 09:00 status=%s ok=%s fail=%s",
            result.get("status"),
            result.get("ok_n"),
            result.get("fail_n"),
        )
    except Exception:
        logger.exception("open_low_high PDC 09:00 failed")


def _run_ws_prewarm_0905() -> None:
    if _skip():
        return
    try:
        from backend.services.open_low_high.ws_feed import prewarm_ws

        result = prewarm_ws(engage_lock=False, canary=True)
        logger.info(
            "open_low_high WS prewarm 09:05 status=%s equity=%s warm=%s canary=%s",
            result.get("status"),
            (result.get("subscribe") or {}).get("equity"),
            (result.get("warm") or {}).get("warm"),
            (result.get("warm") or {}).get("canary"),
        )
    except Exception:
        logger.exception("open_low_high WS prewarm 09:05 failed")


def _run_ws_keepalive() -> None:
    if _skip():
        return
    try:
        from backend.services.open_low_high.ws_feed import resubscribe_olh_ws_keepalive

        result = resubscribe_olh_ws_keepalive()
        logger.info(
            "open_low_high WS keepalive status=%s warm=%s",
            result.get("status"),
            (result.get("warm") or {}).get("warm"),
        )
    except Exception:
        logger.exception("open_low_high WS keepalive failed")


def _run_ws_check_0915() -> None:
    if _skip():
        return
    try:
        from backend.services.open_low_high.ws_feed import ensure_equity_index_warm_or_rewarm

        result = ensure_equity_index_warm_or_rewarm()
        logger.info(
            "open_low_high WS check 09:15 status=%s rewarmed=%s warm=%s",
            result.get("status"),
            result.get("rewarmed"),
            (result.get("warm") or {}).get("warm"),
        )
    except Exception:
        logger.exception("open_low_high WS check 09:15 failed")


def _run_phase_a() -> None:
    if _skip():
        return
    try:
        from backend.services.open_low_high.screener import run_phase_a

        result = run_phase_a()
        logger.info(
            "open_low_high Phase A status=%s phase_a=%s",
            result.get("status"),
            result.get("phase_a"),
        )
    except Exception:
        logger.exception("open_low_high Phase A failed")


def _run_phase_a_refill() -> None:
    if _skip():
        return
    try:
        from backend.services.open_low_high.screener import run_phase_a_refill

        result = run_phase_a_refill()
        logger.info(
            "open_low_high Phase A refill status=%s filled=%s still_missing=%s",
            result.get("status"),
            result.get("filled"),
            result.get("still_missing"),
        )
    except Exception:
        logger.exception("open_low_high Phase A refill failed")


def _run_phase_b() -> None:
    if _skip():
        return
    try:
        from backend.services.open_low_high.ws_feed import release_ws_lock
        from backend.services.open_low_high.screener import run_phase_b

        # Lock window ends at 09:25; release drop-protection (subs may remain).
        try:
            release_ws_lock()
        except Exception:
            logger.debug("open_low_high WS lock release skipped", exc_info=True)
        result = run_phase_b()
        logger.info(
            "open_low_high Phase B status=%s active=%s failures=%s",
            result.get("status"),
            result.get("active"),
            result.get("phase_b_failures"),
        )
    except Exception:
        logger.exception("open_low_high Phase B failed")


def _run_ohl_log_cleanup() -> None:
    try:
        from backend.services.open_low_high.ohl_log import cleanup_ohl_logs

        result = cleanup_ohl_logs(keep_days=3)
        logger.info(
            "open_low_high OHL log cleanup deleted=%s kept=%s",
            len(result.get("deleted") or []),
            result.get("kept"),
        )
    except Exception:
        logger.exception("open_low_high OHL log cleanup failed")


def _run_rest_canary() -> None:
    """One-day REST lag canary (2026-11-20); no-op on other dates."""
    try:
        from backend.services.open_low_high.rest_canary import (
            canary_should_run_today,
            run_rest_canary,
        )

        if not canary_should_run_today():
            logger.info("open_low_high rest_canary skipped (not canary date)")
            return
        if _skip():
            return
        result = run_rest_canary()
        logger.info(
            "open_low_high rest_canary done probe_at=%s n=%s token_last4=%s",
            result.get("probe_at"),
            len(result.get("results") or []),
            result.get("token_last4"),
        )
    except Exception:
        logger.exception("open_low_high rest_canary failed")


def _aware_ist(now: Optional[datetime] = None) -> datetime:
    t = now or datetime.now(IST)
    if t.tzinfo is None:
        return IST.localize(t)
    return t.astimezone(IST)


def has_valid_phase_a(trade_date) -> bool:
    """True when Phase A has already produced a recorded result for the session."""
    from backend.services.open_low_high.persistence import get_day_meta, list_screen_for_date

    meta = get_day_meta(trade_date) or {}
    if meta.get("phase_a_at"):
        return True
    status = str(meta.get("status") or "").lower()
    if status in ("phase_a", "phase_b", "phase_b_partial", "frozen"):
        return True
    # Persisted greys imply Phase A already ran (even if meta row lagged).
    try:
        return bool(list_screen_for_date(trade_date))
    except Exception:
        return False


def has_valid_phase_b(trade_date) -> bool:
    """True when Phase B has already completed (freeze, partial, or recorded attempt)."""
    from backend.services.open_low_high.persistence import get_day_meta

    meta = get_day_meta(trade_date) or {}
    if meta.get("phase_b_at"):
        return True
    if meta.get("frozen") and not meta.get("degraded"):
        return True
    status = str(meta.get("status") or "").lower()
    return status in ("frozen", "phase_b", "phase_b_partial")


def run_startup_catchup(*, now: Optional[datetime] = None) -> Dict[str, Any]:
    """After restart: catch up missed Phase A/B inside 09:20–09:29, else DEGRADED.

    APScheduler ``misfire_grace_time`` is only 60–120s for Phase jobs and does
    **not** reliably replay triggers after a process restart. This hook fills that gap.
    """
    from backend.services.open_low_high.config import PHASE_A_START, PHASE_B_START, PHASE_B_HARD_STOP

    t = _aware_ist(now)
    trade_date = t.date()
    tt = t.time()
    out: Dict[str, Any] = {
        "ok": True,
        "trade_date": trade_date.isoformat(),
        "now": t.isoformat(),
        "actions": [],
    }

    if _skip():
        out["skipped"] = "holiday_or_weekend"
        return out

    if tt < _CATCHUP_START:
        out["skipped"] = "before_catchup_window"
        return out

    need_a = not has_valid_phase_a(trade_date)
    need_b = not has_valid_phase_b(trade_date)
    out["need_phase_a"] = need_a
    out["need_phase_b"] = need_b

    # After 09:29: do not auto-run; mark DEGRADED + alert; force/re-screen still allowed.
    if tt > _CATCHUP_END:
        if not need_a and not need_b:
            out["skipped"] = "complete_after_deadline"
            return out
        missing = []
        if need_a:
            missing.append("phase_a")
        if need_b:
            missing.append("phase_b")
        msg = (
            f"Startup after {PHASE_B_HARD_STOP.strftime('%H:%M')} IST with missing "
            f"{'+'.join(missing)}; marked DEGRADED (force/re-screen allowed)."
        )
        try:
            from backend.services.open_low_high.alerts import alert_degraded
            from backend.services.open_low_high.persistence import upsert_day_meta

            upsert_day_meta(
                trade_date,
                status="degraded",
                frozen=False,
                degraded=True,
                notes=msg,
                meta={
                    "phase": "startup_catchup",
                    "degraded": True,
                    "failure_banner": f"DEGRADED — {msg}",
                    "missing": missing,
                    "startup_catchup_at": t.isoformat(),
                },
            )
            alert_degraded(msg, trade_date=trade_date.isoformat(), missing=",".join(missing))
            out["actions"].append("degraded")
            out["message"] = msg
        except Exception as exc:
            logger.exception("open_low_high startup catch-up DEGRADED path failed")
            out["ok"] = False
            out["error"] = str(exc)
        return out

    # Inside 09:20–09:29: run missing phases via the same cron runners (retry loop intact).
    if need_a and tt >= PHASE_A_START:
        logger.info(
            "open_low_high startup catch-up: running Phase A (restart at %s)",
            t.strftime("%H:%M:%S"),
        )
        _run_phase_a()
        out["actions"].append("phase_a")
        need_a = not has_valid_phase_a(trade_date)
        out["need_phase_a"] = need_a

    if tt >= PHASE_B_START and not has_valid_phase_b(trade_date):
        # If Phase A still missing inside B window, A already attempted above when due.
        logger.info(
            "open_low_high startup catch-up: running Phase B (restart at %s)",
            t.strftime("%H:%M:%S"),
        )
        _run_phase_b()
        out["actions"].append("phase_b")
        out["need_phase_b"] = not has_valid_phase_b(trade_date)

    if not out["actions"]:
        out["skipped"] = "nothing_to_catch_up"
    return out


def _spawn_startup_catchup() -> None:
    """Fire catch-up in a daemon thread so FastAPI lifespan is not blocked."""

    def _bg() -> None:
        try:
            result = run_startup_catchup()
            logger.info("open_low_high startup catch-up result=%s", result)
        except Exception:
            logger.exception("open_low_high startup catch-up failed")

    threading.Thread(target=_bg, name="olh-startup-catchup", daemon=True).start()


def start_open_low_high_scheduler() -> None:
    global _scheduler
    if _scheduler is not None:
        return
    from backend.services.open_low_high.config import PDC_EOD_HOUR, PDC_EOD_MINUTE

    sched = BackgroundScheduler(timezone="Asia/Kolkata")

    # --- PDC ---
    sched.add_job(
        _run_pdc_eod,
        CronTrigger(
            day_of_week="mon-fri",
            hour=int(PDC_EOD_HOUR),
            minute=int(PDC_EOD_MINUTE),
            timezone="Asia/Kolkata",
        ),
        id="open_low_high_pdc_eod",
        name=f"Open Low High PDC EOD {PDC_EOD_HOUR:02d}:{PDC_EOD_MINUTE:02d} IST",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=600,
    )
    sched.add_job(
        _run_pdc_0900,
        CronTrigger(day_of_week="mon-fri", hour=9, minute=0, second=0, timezone="Asia/Kolkata"),
        id="open_low_high_pdc_0900",
        name="Open Low High PDC cross-check 09:00 IST",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=300,
    )

    # --- WS pre-warm (09:05 only) + 09:15 check ---
    sched.add_job(
        _run_ws_prewarm_0905,
        CronTrigger(day_of_week="mon-fri", hour=9, minute=5, second=0, timezone="Asia/Kolkata"),
        id="open_low_high_ws_prewarm_0905",
        name="Open Low High WS pre-warm 09:05 IST",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=180,
    )
    sched.add_job(
        _run_ws_check_0915,
        CronTrigger(day_of_week="mon-fri", hour=9, minute=15, second=0, timezone="Asia/Kolkata"),
        id="open_low_high_ws_check_0915",
        name="Open Low High WS warm check 09:15 IST",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=120,
    )
    # Auto-reconnect companion: re-subscribe every 2 min 09:07–09:28.
    sched.add_job(
        _run_ws_keepalive,
        CronTrigger(
            day_of_week="mon-fri",
            hour=9,
            minute="7-28/2",
            second=0,
            timezone="Asia/Kolkata",
        ),
        id="open_low_high_ws_keepalive",
        name="Open Low High WS keepalive 09:07–09:28 IST",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=60,
    )

    # --- Phase A primary window 09:20:10, 09:21, 09:22, 09:23 ---
    for minute, second, jid in (
        (20, 10, "open_low_high_phase_a_092010"),
        (21, 0, "open_low_high_phase_a_0921"),
        (22, 0, "open_low_high_phase_a_0922"),
        (23, 0, "open_low_high_phase_a_0923"),
    ):
        sched.add_job(
            _run_phase_a,
            CronTrigger(
                day_of_week="mon-fri",
                hour=9,
                minute=minute,
                second=second,
                timezone="Asia/Kolkata",
            ),
            id=jid,
            name=f"Open Low High Phase A 09:{minute:02d}:{second:02d} IST",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=90,
        )

    # --- Phase A refill 09:24 (null 09:15 subset only) ---
    sched.add_job(
        _run_phase_a_refill,
        CronTrigger(day_of_week="mon-fri", hour=9, minute=24, second=0, timezone="Asia/Kolkata"),
        id="open_low_high_phase_a_refill_0924",
        name="Open Low High Phase A refill 09:24 IST",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=90,
    )

    # --- Phase B 09:25:03 then retries through 09:29 ---
    sched.add_job(
        _run_phase_b,
        CronTrigger(day_of_week="mon-fri", hour=9, minute=25, second=3, timezone="Asia/Kolkata"),
        id="open_low_high_phase_b_092503",
        name="Open Low High Phase B 09:25:03 IST",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=120,
    )
    # Dense retries: every 30s from 09:25:30–09:29:00 (aligns with REST wait).
    sched.add_job(
        _run_phase_b,
        CronTrigger(
            day_of_week="mon-fri",
            hour=9,
            minute="25-28",
            second="30",
            timezone="Asia/Kolkata",
        ),
        id="open_low_high_phase_b_retry_30s",
        name="Open Low High Phase B retry :30 09:25–09:28 IST",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=60,
    )
    sched.add_job(
        _run_phase_b,
        CronTrigger(
            day_of_week="mon-fri",
            hour=9,
            minute="26-29",
            second="0",
            timezone="Asia/Kolkata",
        ),
        id="open_low_high_phase_b_retry_00s",
        name="Open Low High Phase B retry :00 09:26–09:29 IST",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=60,
    )

    # --- OHL log 3-day cleanup ---
    sched.add_job(
        _run_ohl_log_cleanup,
        CronTrigger(day_of_week="mon-fri", hour=12, minute=0, timezone="Asia/Kolkata"),
        id="open_low_high_ohl_log_cleanup",
        name="Open Low High OHL log cleanup (3d)",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=600,
    )

    # --- One-day REST lag canary 2026-11-20 (09:20:05/30, 09:21:00, 09:25:05/30 IST) ---
    from backend.services.open_low_high.rest_canary import CANARY_DATE, CANARY_TIMES

    for tt in CANARY_TIMES:
        jid = f"open_low_high_rest_canary_{tt.hour:02d}{tt.minute:02d}{tt.second:02d}"
        sched.add_job(
            _run_rest_canary,
            CronTrigger(
                year=CANARY_DATE.year,
                month=CANARY_DATE.month,
                day=CANARY_DATE.day,
                hour=tt.hour,
                minute=tt.minute,
                second=tt.second,
                timezone="Asia/Kolkata",
            ),
            id=jid,
            name=f"OLH REST canary {CANARY_DATE.isoformat()} {tt.isoformat()} IST",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=45,
        )

    # Remove legacy multi-prewarm / incomplete-freeze job ids if an old process
    # left them in a long-lived scheduler (replace_existing only covers same ids).
    for legacy_id in (
        "open_low_high_prewarm_0914",
        "open_low_high_prewarm_0918",
        "open_low_high_prewarm_0921",
        "open_low_high_prewarm_0923",
        "open_low_high_screen",
        "open_low_high_screen_retry",
    ):
        try:
            if sched.get_job(legacy_id):
                sched.remove_job(legacy_id)
        except Exception:
            pass

    sched.start()
    _scheduler = sched
    _spawn_startup_catchup()
    canary_ids = [
        f"open_low_high_rest_canary_{tt.hour:02d}{tt.minute:02d}{tt.second:02d}"
        for tt in CANARY_TIMES
    ]
    logger.info(
        "open_low_high scheduler started "
        "(PDC 16:05/09:00; WS 09:05+09:15; Phase A 09:20:10–09:23 + refill 09:24; "
        "Phase B 09:25:03–09:29; startup catch-up 09:20–09:29 / DEGRADED after; "
        "OHL log cleanup 12:00; REST canary %s jobs=%s)",
        CANARY_DATE.isoformat(),
        canary_ids,
    )


def stop_open_low_high_scheduler() -> None:
    global _scheduler
    if _scheduler is None:
        return
    try:
        _scheduler.shutdown(wait=False)
    except Exception:
        logger.exception("open_low_high scheduler shutdown failed")
    _scheduler = None

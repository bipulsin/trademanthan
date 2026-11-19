"""APScheduler jobs: Open Low High pre-warm + screen (~09:25 IST) with incomplete retries."""
from __future__ import annotations

import logging

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from backend.services.market_holiday import should_skip_scheduled_market_jobs_ist

logger = logging.getLogger(__name__)

_scheduler: BackgroundScheduler | None = None


def _run_olh_screen() -> None:
    if should_skip_scheduled_market_jobs_ist():
        logger.info("open_low_high: skipped holiday/weekend")
        return
    try:
        from backend.services.open_low_high.screener import run_screen

        result = run_screen(force=False)
        logger.info(
            "open_low_high scheduled screen status=%s kept=%s phase_a=%s freeze_reason=%s",
            result.get("status"),
            result.get("kept"),
            result.get("phase_a"),
            result.get("freeze_reason"),
        )
    except Exception:
        logger.exception("open_low_high scheduled screen failed")


def _run_olh_prewarm() -> None:
    if should_skip_scheduled_market_jobs_ist():
        logger.info("open_low_high prewarm: skipped holiday/weekend")
        return
    try:
        from backend.services.open_low_high.screener import prewarm_equity_data

        result = prewarm_equity_data()
        logger.info(
            "open_low_high prewarm status=%s ok_5m=%s ok_daily=%s nifty_pc=%s",
            result.get("status"),
            result.get("ok_5m"),
            result.get("ok_daily"),
            result.get("nifty_prev_close"),
        )
    except Exception:
        logger.exception("open_low_high prewarm failed")


def start_open_low_high_scheduler() -> None:
    global _scheduler
    if _scheduler is not None:
        return
    sched = BackgroundScheduler(timezone="Asia/Kolkata")

    # Prefetch equity/Nifty REST before Phase B so ~09:25 is not cold-start nulls.
    for mid, minute, second in (
        ("open_low_high_prewarm_0914", 14, 0),
        ("open_low_high_prewarm_0918", 18, 0),
        ("open_low_high_prewarm_0921", 21, 0),
        ("open_low_high_prewarm_0923", 23, 30),
    ):
        sched.add_job(
            _run_olh_prewarm,
            CronTrigger(
                day_of_week="mon-fri",
                hour=9,
                minute=minute,
                second=second,
                timezone="Asia/Kolkata",
            ),
            id=mid,
            name=f"Open Low High pre-warm 09:{minute:02d}:{second:02d} IST",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=120,
        )

    # Primary screen shortly after 09:20 five-minute bar closes.
    sched.add_job(
        _run_olh_screen,
        CronTrigger(day_of_week="mon-fri", hour=9, minute=25, second=10, timezone="Asia/Kolkata"),
        id="open_low_high_screen",
        name="Open Low High screen 09:25:10 IST",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=180,
    )

    # Dense retries while incomplete (empty Phase A + missing equity REST).
    # Every 20s from 09:25:30 through 09:29:50; frozen days no-op quickly.
    sched.add_job(
        _run_olh_screen,
        CronTrigger(
            day_of_week="mon-fri",
            hour=9,
            minute="25-29",
            second="10,30,50",
            timezone="Asia/Kolkata",
        ),
        id="open_low_high_screen_retry",
        name="Open Low High screen retry 09:25–09:29 IST",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=90,
    )

    sched.start()
    _scheduler = sched
    logger.info(
        "open_low_high scheduler started "
        "(prewarm 09:14/18/21/23:30 + screen 09:25:10 + retry */20s to 09:29)"
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

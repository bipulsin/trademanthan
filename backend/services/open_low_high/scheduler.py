"""APScheduler job: Open Low High screen at 09:20:30 IST on trading days."""
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
            "open_low_high scheduled screen status=%s kept=%s",
            result.get("status"),
            result.get("kept"),
        )
    except Exception:
        logger.exception("open_low_high scheduled screen failed")


def start_open_low_high_scheduler() -> None:
    global _scheduler
    if _scheduler is not None:
        return
    sched = BackgroundScheduler(timezone="Asia/Kolkata")
    # Phase A+B at ~09:20:30; freeze completes inside run_screen.
    sched.add_job(
        _run_olh_screen,
        CronTrigger(day_of_week="mon-fri", hour=9, minute=20, second=30, timezone="Asia/Kolkata"),
        id="open_low_high_screen",
        name="Open Low High screen 09:20:30 IST",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=180,
    )
    # Safety catch-up near freeze deadline if first job misfired.
    sched.add_job(
        _run_olh_screen,
        CronTrigger(day_of_week="mon-fri", hour=9, minute=21, second=45, timezone="Asia/Kolkata"),
        id="open_low_high_screen_catchup",
        name="Open Low High screen catch-up 09:21:45 IST",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=120,
    )
    sched.start()
    _scheduler = sched
    logger.info("open_low_high scheduler started (09:20:30 + 09:21:45 IST)")


def stop_open_low_high_scheduler() -> None:
    global _scheduler
    if _scheduler is None:
        return
    try:
        _scheduler.shutdown(wait=False)
    except Exception:
        logger.exception("open_low_high scheduler shutdown failed")
    _scheduler = None

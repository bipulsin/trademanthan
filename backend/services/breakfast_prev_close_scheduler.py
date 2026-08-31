"""APScheduler cron for Breakfast prev-close prefill (16:00, 16:30, 09:05 IST)."""
from __future__ import annotations

import logging
from typing import Optional

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from backend.services.breakfast_prev_close import run_breakfast_prev_close_job

logger = logging.getLogger(__name__)

_SCHEDULER: Optional[BackgroundScheduler] = None


def _tick(trigger: str) -> None:
    try:
        run_breakfast_prev_close_job(trigger=trigger)
    except Exception as e:
        logger.exception("breakfast_prev_close_job failed (%s): %s", trigger, e)


def start_breakfast_prev_close_scheduler() -> None:
    """16:00 + 16:30 post-close retries; 09:05 pre-open refresh (IST weekdays)."""
    global _SCHEDULER
    if _SCHEDULER is not None:
        return
    sch = BackgroundScheduler(timezone="Asia/Kolkata")
    for hour, minute, tid in (
        (16, 0, "breakfast_prev_close_1600"),
        (16, 30, "breakfast_prev_close_1630"),
        (9, 5, "breakfast_prev_close_0905"),
    ):
        sch.add_job(
            _tick,
            CronTrigger(day_of_week="mon-fri", hour=hour, minute=minute, timezone="Asia/Kolkata"),
            args=[f"scheduled_{hour:02d}{minute:02d}"],
            id=tid,
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=300,
        )
    sch.start()
    _SCHEDULER = sch
    logger.info("Breakfast prev-close scheduler started (16:00, 16:30, 09:05 IST weekdays)")


def stop_breakfast_prev_close_scheduler() -> None:
    global _SCHEDULER
    if _SCHEDULER:
        try:
            _SCHEDULER.shutdown(wait=False)
        finally:
            _SCHEDULER = None

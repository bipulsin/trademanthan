"""APScheduler jobs for Breakfast live 1m ticks (9:16–9:20) and 9:20:30 freeze."""
from __future__ import annotations

import logging
from typing import Optional

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from backend.services.breakfast_strategy.live_tick import (
    FREEZE_AT,
    run_breakfast_freeze_lock,
    run_breakfast_minute_tick,
)
from backend.services.breakfast_upstox_gate import is_breakfast_priority_window
from backend.services.market_holiday import should_skip_scheduled_market_jobs_ist

logger = logging.getLogger(__name__)

_SCHEDULER: Optional[BackgroundScheduler] = None


def _tick_job(minute: int) -> None:
    if should_skip_scheduled_market_jobs_ist():
        return
    if not is_breakfast_priority_window():
        return
    try:
        out = run_breakfast_minute_tick(minute)
        logger.info("breakfast minute tick :%02d -> %s", minute, out)
    except Exception as e:
        logger.exception("breakfast minute tick :%02d failed: %s", minute, e)


def _freeze_job() -> None:
    if should_skip_scheduled_market_jobs_ist():
        return
    try:
        out = run_breakfast_freeze_lock()
        logger.info("breakfast freeze lock -> %s", out)
    except Exception as e:
        logger.exception("breakfast freeze lock failed: %s", e)


class BreakfastLiveScheduler:
    def __init__(self) -> None:
        self.scheduler = BackgroundScheduler(timezone="Asia/Kolkata")
        self._started = False

    def start(self) -> None:
        if self._started:
            return
        for minute in (16, 17, 18, 19, 20):
            self.scheduler.add_job(
                _tick_job,
                CronTrigger(day_of_week="mon-fri", hour=9, minute=minute, second=5),
                args=[minute],
                id=f"breakfast_live_tick_{minute:02d}",
                replace_existing=True,
                max_instances=1,
                coalesce=True,
                misfire_grace_time=30,
            )
        self.scheduler.add_job(
            _freeze_job,
            CronTrigger(
                day_of_week="mon-fri",
                hour=FREEZE_AT.hour,
                minute=FREEZE_AT.minute,
                second=FREEZE_AT.second,
            ),
            id="breakfast_live_freeze_92030",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=60,
        )
        self.scheduler.start()
        self._started = True
        logger.info("Breakfast live scheduler started (9:16–9:20 ticks, 9:20:30 freeze IST)")

    def stop(self) -> None:
        if not self._started:
            return
        self.scheduler.shutdown(wait=False)
        self._started = False
        logger.info("Breakfast live scheduler stopped")


_breakfast_live_scheduler = BreakfastLiveScheduler()


def start_breakfast_live_scheduler() -> None:
    _breakfast_live_scheduler.start()


def stop_breakfast_live_scheduler() -> None:
    _breakfast_live_scheduler.stop()

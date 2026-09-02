"""APScheduler jobs for Breakfast live 1m ticks (9:16–9:19) and 9:20:05 freeze."""
from __future__ import annotations

import logging
from typing import Optional

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from backend.services.breakfast_strategy.live_tick import (
    FREEZE_AT,
    SCHEDULER_TICK_MINUTES,
    run_breakfast_freeze_lock,
    run_breakfast_minute_tick,
    run_breakfast_ws_resubscribe_915,
    run_breakfast_ws_warmup,
)
from backend.services.breakfast_upstox_gate import is_breakfast_priority_window
from backend.services.market_holiday import should_skip_scheduled_market_jobs_ist

logger = logging.getLogger(__name__)

_SCHEDULER: Optional[BackgroundScheduler] = None
_IST = "Asia/Kolkata"


def _freeze_cron_trigger() -> CronTrigger:
    """9:20:05 IST weekday freeze. Timezone must be explicit: CronTrigger defaults to local TZ, not the scheduler's."""
    return CronTrigger(
        day_of_week="mon-fri",
        hour=FREEZE_AT.hour,
        minute=FREEZE_AT.minute,
        second=FREEZE_AT.second,
        timezone=_IST,
    )


def _warmup_job() -> None:
    if should_skip_scheduled_market_jobs_ist():
        return
    try:
        out = run_breakfast_ws_warmup()
        logger.info("breakfast WS warmup 9:10 -> %s", out)
    except Exception as e:
        logger.exception("breakfast WS warmup 9:10 failed: %s", e)


def _resubscribe_915_job() -> None:
    if should_skip_scheduled_market_jobs_ist():
        return
    try:
        out = run_breakfast_ws_resubscribe_915()
        logger.info("breakfast WS 9:15 resubscribe -> %s", out)
    except Exception as e:
        logger.exception("breakfast WS 9:15 resubscribe failed: %s", e)


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
        self.scheduler.add_job(
            _warmup_job,
            CronTrigger(day_of_week="mon-fri", hour=9, minute=10, second=0, timezone=_IST),
            id="breakfast_ws_warmup_910",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=300,
        )
        self.scheduler.add_job(
            _resubscribe_915_job,
            CronTrigger(day_of_week="mon-fri", hour=9, minute=15, second=0, timezone=_IST),
            id="breakfast_ws_resubscribe_915",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=30,
        )
        for minute in SCHEDULER_TICK_MINUTES:
            self.scheduler.add_job(
                _tick_job,
                CronTrigger(day_of_week="mon-fri", hour=9, minute=minute, second=5, timezone=_IST),
                args=[minute],
                id=f"breakfast_live_tick_{minute:02d}",
                replace_existing=True,
                max_instances=1,
                coalesce=True,
                misfire_grace_time=30,
            )
        self.scheduler.add_job(
            _freeze_job,
            _freeze_cron_trigger(),
            id="breakfast_live_freeze_92005",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=60,
        )
        self.scheduler.start()
        self._started = True
        logger.info(
            "Breakfast live scheduler started "
            "(9:10 WS warmup, 9:15 index union, 9:16–9:19 ticks, 9:20:05 freeze IST)"
        )

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


def breakfast_live_scheduler_status() -> dict:
    """Runtime status for preflight checks (job ids, running flag)."""
    sch = _breakfast_live_scheduler
    job_ids: list[str] = []
    if sch._started:
        job_ids = sorted(j.id for j in sch.scheduler.get_jobs())
    return {"running": sch._started, "job_ids": job_ids}

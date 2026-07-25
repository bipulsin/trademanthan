"""Nightly cron: precompute ATR(14)% for arbitrage_master current-month futures (Mon–Fri IST)."""

from __future__ import annotations

import logging

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from backend.services.atr_daily_precompute import (
    ensure_atr_daily_precompute_tables,
    run_atr_daily_precompute_job,
    scheduled_tick_should_run,
)

logger = logging.getLogger(__name__)

_scheduler: BackgroundScheduler | None = None


def _tick() -> None:
    if not scheduled_tick_should_run():
        logger.info("atr_daily_precompute: skipped (weekend/holiday)")
        return
    ensure_atr_daily_precompute_tables()
    out = run_atr_daily_precompute_job(trigger="scheduled_1900")
    logger.info("atr_daily_precompute_job: %s", out)


def start_atr_daily_precompute_scheduler() -> None:
    """19:00 IST weekdays — after cash close; prepares next session's ATR cache."""
    global _scheduler
    if _scheduler is not None:
        return
    sch = BackgroundScheduler(timezone="Asia/Kolkata")
    sch.add_job(
        _tick,
        CronTrigger(day_of_week="mon-fri", hour=19, minute=0, timezone="Asia/Kolkata"),
        id="atr_daily_precompute_1900",
        name="ATR daily precompute 19:00 IST",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    sch.start()
    _scheduler = sch
    logger.info("ATR daily precompute scheduler started (19:00 IST weekdays)")


def stop_atr_daily_precompute_scheduler() -> None:
    global _scheduler
    if _scheduler:
        try:
            _scheduler.shutdown(wait=False)
        finally:
            _scheduler = None

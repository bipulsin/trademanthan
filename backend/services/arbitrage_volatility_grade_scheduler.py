"""Friday 17:00 IST: volatility grade for arbitrage_master current-month futures."""

from __future__ import annotations

import logging

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from backend.services.arbitrage_volatility_grade import (
    ensure_volatility_grade_columns,
    run_volatility_grade_job,
    scheduled_tick_should_run,
)

logger = logging.getLogger(__name__)

_scheduler: BackgroundScheduler | None = None


def _tick() -> None:
    if not scheduled_tick_should_run():
        logger.info("volatility_grade: skipped (weekend/holiday)")
        return
    ensure_volatility_grade_columns()
    out = run_volatility_grade_job(trigger="scheduled_fri_1700")
    logger.info("volatility_grade_job: %s", out)


def start_arbitrage_volatility_grade_scheduler() -> None:
    """17:00 IST Fridays — after cash close; Breakfast exclusivity is morning-only."""
    global _scheduler
    if _scheduler is not None:
        return
    sch = BackgroundScheduler(timezone="Asia/Kolkata")
    sch.add_job(
        _tick,
        CronTrigger(day_of_week="fri", hour=17, minute=0, timezone="Asia/Kolkata"),
        id="arbitrage_volatility_grade_fri_1700",
        name="Arbitrage volatility grade Friday 17:00 IST",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    sch.start()
    _scheduler = sch
    logger.info("Arbitrage volatility grade scheduler started (Friday 17:00 IST)")


def stop_arbitrage_volatility_grade_scheduler() -> None:
    global _scheduler
    if _scheduler:
        try:
            _scheduler.shutdown(wait=False)
        finally:
            _scheduler = None

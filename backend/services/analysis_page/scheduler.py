"""Weekday 17:00 IST: refresh analysis_symbol_snapshot (after cash close).

Mon–Thu: daily fields only. Friday: weekly then daily in one job (weekly fetch + daily).
Skips weekends and NSE holidays (same as volatility_grade Friday 17:00).
"""
from __future__ import annotations

import logging

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from backend.services.analysis_page.job import (
    ensure_analysis_snapshot_table,
    run_analysis_snapshot_job,
    should_refresh_weekly,
)
from backend.services.market_holiday import should_skip_scheduled_market_jobs_ist

logger = logging.getLogger(__name__)

_scheduler: BackgroundScheduler | None = None


def _tick() -> None:
    if should_skip_scheduled_market_jobs_ist():
        logger.info("analysis_snapshot: skipped (weekend/holiday)")
        return
    ensure_analysis_snapshot_table()
    weekly = should_refresh_weekly()
    trigger = "scheduled_fri_1700" if weekly else "scheduled_1700"
    out = run_analysis_snapshot_job(trigger=trigger, refresh_weekly=weekly)
    logger.info("analysis_snapshot_job: %s", out)


def start_analysis_snapshot_scheduler() -> None:
    global _scheduler
    if _scheduler is not None:
        return
    sch = BackgroundScheduler(timezone="Asia/Kolkata")
    sch.add_job(
        _tick,
        CronTrigger(day_of_week="mon-fri", hour=17, minute=0, timezone="Asia/Kolkata"),
        id="analysis_symbol_snapshot_1700",
        name="Analysis snapshot 17:00 IST",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    sch.start()
    _scheduler = sch
    logger.info("Analysis snapshot scheduler started (17:00 IST weekdays)")


def stop_analysis_snapshot_scheduler() -> None:
    global _scheduler
    if _scheduler:
        try:
            _scheduler.shutdown(wait=False)
        finally:
            _scheduler = None

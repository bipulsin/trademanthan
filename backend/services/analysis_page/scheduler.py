"""Weekday 16:15 IST: refresh analysis_symbol_snapshot (after cash close)."""
from __future__ import annotations

import logging

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from backend.services.analysis_page.job import (
    ensure_analysis_snapshot_table,
    run_analysis_snapshot_job,
)
from backend.services.market_holiday import should_skip_scheduled_market_jobs_ist

logger = logging.getLogger(__name__)

_scheduler: BackgroundScheduler | None = None


def _tick() -> None:
    if should_skip_scheduled_market_jobs_ist():
        logger.info("analysis_snapshot: skipped (weekend/holiday)")
        return
    ensure_analysis_snapshot_table()
    out = run_analysis_snapshot_job(trigger="scheduled_1615")
    logger.info("analysis_snapshot_job: %s", out)


def start_analysis_snapshot_scheduler() -> None:
    global _scheduler
    if _scheduler is not None:
        return
    sch = BackgroundScheduler(timezone="Asia/Kolkata")
    sch.add_job(
        _tick,
        CronTrigger(day_of_week="mon-fri", hour=16, minute=15, timezone="Asia/Kolkata"),
        id="analysis_symbol_snapshot_1615",
        name="Analysis snapshot 16:15 IST",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    sch.start()
    _scheduler = sch
    logger.info("Analysis snapshot scheduler started (16:15 IST weekdays)")


def stop_analysis_snapshot_scheduler() -> None:
    global _scheduler
    if _scheduler:
        try:
            _scheduler.shutdown(wait=False)
        finally:
            _scheduler = None

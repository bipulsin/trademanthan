"""Pre-market cron: refresh Iron Condor universe India VIX + equity history cache (Mon–Fri IST)."""

from __future__ import annotations

import logging

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from backend.services import market_holiday as mh
from backend.services.iron_condor_snapshot_cache import (
    run_iron_condor_daily_snapshot_job,
    ensure_iron_condor_snapshot_tables,
)

logger = logging.getLogger(__name__)

_scheduler: BackgroundScheduler | None = None


def _tick() -> None:
    now = mh._normalize_ist(None)
    if not mh.should_skip_scheduled_market_jobs_ist(now) and now.weekday() < 5:
        ensure_iron_condor_snapshot_tables()
        out = run_iron_condor_daily_snapshot_job()
        logger.info("iron_condor_daily_snapshot_job: %s", out)


def start_iron_condor_snapshot_scheduler() -> None:
    """08:33 IST weekdays — ahead of checklist traffic and arbitrage daily setup."""
    global _scheduler
    if _scheduler is not None:
        return
    sch = BackgroundScheduler(timezone="Asia/Kolkata")
    sch.add_job(
        _tick,
        CronTrigger(day_of_week="mon-fri", hour=8, minute=33, timezone="Asia/Kolkata"),
    )
    sch.start()
    _scheduler = sch
    logger.info("Iron Condor snapshot scheduler started (08:33 IST weekdays)")


def stop_iron_condor_snapshot_scheduler() -> None:
    global _scheduler
    if _scheduler:
        try:
            _scheduler.shutdown(wait=False)
        finally:
            _scheduler = None

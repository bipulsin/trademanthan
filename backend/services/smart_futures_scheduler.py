"""
APScheduler jobs for Smart Futures (IST).
Logs to root logger (trademanthan.log).
"""
from __future__ import annotations

import logging
from typing import Optional

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from backend.services.smart_futures.pipeline import (
    force_exit_all_smart_futures_positions,
    run_smart_futures_exit_check_job,
    run_smart_futures_scan_job,
)

logger = logging.getLogger(__name__)

_scheduler: Optional[BackgroundScheduler] = None


def _job_scan():
    try:
        run_smart_futures_scan_job()
    except Exception as e:
        logger.exception("smart_futures scan job: %s", e)


def _job_exit():
    try:
        run_smart_futures_exit_check_job()
    except Exception as e:
        logger.exception("smart_futures exit job: %s", e)


def _job_force_eod():
    try:
        force_exit_all_smart_futures_positions(user_id=None)
    except Exception as e:
        logger.exception("smart_futures force eod: %s", e)


def start_smart_futures_scheduler() -> None:
    global _scheduler
    if _scheduler and _scheduler.running:
        return
    tz = pytz.timezone("Asia/Kolkata")
    _scheduler = BackgroundScheduler(timezone=tz)
    # Every 5 minutes — data + Renko + candidates
    _scheduler.add_job(_job_scan, IntervalTrigger(minutes=5), id="sf_scan", replace_existing=True)
    # Every 60s — sync exit flags / lightweight checks
    _scheduler.add_job(_job_exit, IntervalTrigger(seconds=60), id="sf_exit", replace_existing=True)
    # 15:15 IST — force square-off all
    _scheduler.add_job(
        _job_force_eod,
        CronTrigger(hour=15, minute=15, timezone=tz),
        id="sf_force_eod",
        replace_existing=True,
    )
    _scheduler.start()
    logger.info("Smart Futures scheduler started (5m scan, 60s exit sync, 15:15 force exit)")


def stop_smart_futures_scheduler() -> None:
    global _scheduler
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
        logger.info("Smart Futures scheduler stopped")
    _scheduler = None

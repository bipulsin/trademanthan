"""Weekday 08:30 IST Dhan token refresh. Startup does not call Dhan."""
from __future__ import annotations

import logging
from typing import Optional

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from backend.dhan.auth import ensure_broker_tokens_table, proactive_refresh, redact_secrets

logger = logging.getLogger(__name__)

_scheduler: Optional[BackgroundScheduler] = None


def _refresh_job() -> None:
    try:
        expiry = proactive_refresh()
        logger.info("Dhan proactive token refresh ok; expires %s", expiry.isoformat())
    except Exception as exc:
        logger.error("Dhan proactive token refresh failed: %s", redact_secrets(exc))


def start_dhan_token_scheduler() -> None:
    global _scheduler
    if _scheduler is not None:
        return
    try:
        ensure_broker_tokens_table()
    except Exception as exc:
        logger.error("broker_tokens table ensure failed: %s", redact_secrets(exc))
    sch = BackgroundScheduler(timezone="Asia/Kolkata")
    sch.add_job(
        _refresh_job,
        CronTrigger(
            day_of_week="mon-fri",
            hour=8,
            minute=30,
            timezone="Asia/Kolkata",
        ),
        id="dhan_token_refresh_0830",
        name="Dhan token refresh 08:30 IST weekdays",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    sch.start()
    _scheduler = sch
    logger.info("Dhan token scheduler started (08:30 IST weekdays)")


def stop_dhan_token_scheduler() -> None:
    global _scheduler
    if _scheduler is None:
        return
    try:
        _scheduler.shutdown(wait=False)
    finally:
        _scheduler = None

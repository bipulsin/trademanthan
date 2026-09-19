"""APScheduler for Kosmic Tarang IV snapshots (IST)."""
from __future__ import annotations

import logging

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from backend.services.tarang.iv_snapshots import capture_iv_snapshots
from backend.services.tarang.schema import ensure_tarang_tables

logger = logging.getLogger(__name__)

_scheduler: BackgroundScheduler | None = None


def _tick_iv() -> None:
    try:
        ensure_tarang_tables()
        out = capture_iv_snapshots()
        logger.info("tarang IV snapshot job: %s", out)
    except Exception as e:
        logger.exception("tarang IV snapshot job failed: %s", e)


def start_tarang_scheduler() -> None:
    """IV snapshots: every 30m 09:00–23:30 IST daily (MCX session + crypto)."""
    global _scheduler
    if _scheduler is not None:
        return
    ensure_tarang_tables()
    sch = BackgroundScheduler(timezone="Asia/Kolkata")
    # Intraday ATM IV: top of hour and :30 during MCX hours; also a few Delta-friendly slots
    sch.add_job(
        _tick_iv,
        CronTrigger(minute="0,30", hour="9-23", timezone="Asia/Kolkata"),
        id="tarang_iv_snapshots",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    # One early morning capture for crypto / overnight
    sch.add_job(
        _tick_iv,
        CronTrigger(hour=8, minute=5, timezone="Asia/Kolkata"),
        id="tarang_iv_snapshots_morning",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    sch.start()
    _scheduler = sch
    logger.info("Kosmic Tarang scheduler started (IV snapshots IST)")
    # Kick once shortly after boot (non-blocking via scheduler)
    try:
        sch.add_job(_tick_iv, "date", id="tarang_iv_boot", replace_existing=True)
    except Exception:
        pass


def stop_tarang_scheduler() -> None:
    global _scheduler
    if _scheduler:
        try:
            _scheduler.shutdown(wait=False)
        finally:
            _scheduler = None

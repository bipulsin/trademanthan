"""IST 11:15 / 13:15 / 15:15 + post-market 15:45 — Stock Options 2h EMAs."""
from __future__ import annotations

import logging

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from backend.services.market_holiday import should_skip_scheduled_market_jobs_ist
from backend.services.stock_option_signals import ensure_index_radar_rows, run_ema_tick

logger = logging.getLogger(__name__)

_scheduler: BackgroundScheduler | None = None


def _tick() -> None:
    if should_skip_scheduled_market_jobs_ist():
        logger.info("stock_option ema: skipped (weekend/holiday)")
        return
    try:
        out = run_ema_tick()
        logger.info("stock_option ema tick: %s", out)
    except Exception:
        logger.exception("stock_option ema tick failed")


def start_stock_option_scheduler() -> None:
    global _scheduler
    if _scheduler is not None:
        return
    try:
        n = ensure_index_radar_rows()
        if n:
            logger.info("stock_option seeded %s permanent index Radar row(s) on start", n)
    except Exception:
        logger.exception("stock_option index Radar seed on start failed")
    sch = BackgroundScheduler(timezone="Asia/Kolkata")
    sch.add_job(
        _tick,
        CronTrigger(
            day_of_week="mon-fri",
            hour="11,13,15",
            minute=15,
            timezone="Asia/Kolkata",
        ),
        id="stock_option_ema_2h",
        name="Stock Options 2h EMA 11:15/13:15/15:15",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    # After cash close: last 2h bucket (13:15–15:15) and Upstox history are settled.
    # Retries same path as session ticks (hours/2 → 1h → 15m → Kavach 10m/5m).
    sch.add_job(
        _tick,
        CronTrigger(
            day_of_week="mon-fri",
            hour=15,
            minute=45,
            timezone="Asia/Kolkata",
        ),
        id="stock_option_ema_post_market",
        name="Stock Options post-market EMA 15:45",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    sch.start()
    _scheduler = sch
    logger.info(
        "Stock Options 2h EMA scheduler started (11:15, 13:15, 15:15, post-market 15:45 IST)"
    )


def stop_stock_option_scheduler() -> None:
    global _scheduler
    if _scheduler:
        try:
            _scheduler.shutdown(wait=False)
        finally:
            _scheduler = None

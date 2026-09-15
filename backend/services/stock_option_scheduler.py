"""IST 11:15 / 13:15 / 15:15 + post-market 15:45 — Stock Options 2h EMAs.

Also keeps Executed option instrument keys on the shared Upstox WS (1m sync from
09:30 IST after Breakfast) so sell/buy LTPs update live; 2h
``refresh_executed_ltps`` remains the REST fallback.
"""
from __future__ import annotations

import logging
from datetime import datetime, time as dt_time

import pytz

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from backend.services.market_holiday import should_skip_scheduled_market_jobs_ist
from backend.services.stock_option_signals import ensure_index_radar_rows, run_ema_tick

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")
# After Breakfast exclusivity (ends by lock or 09:25); user-facing live LTP from 09:30.
WS_LTP_SESSION_START = dt_time(9, 30)
WS_LTP_SESSION_END = dt_time(15, 35)

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


def _within_ws_ltp_session(now: datetime | None = None) -> bool:
    t = now or datetime.now(IST)
    if t.tzinfo is None:
        t = IST.localize(t)
    else:
        t = t.astimezone(IST)
    tt = t.time()
    return WS_LTP_SESSION_START <= tt <= WS_LTP_SESSION_END


def _ws_ltp_sync() -> None:
    if should_skip_scheduled_market_jobs_ist():
        return
    if not _within_ws_ltp_session():
        return
    try:
        from backend.services.stock_option_ws_ltp import sync_executed_option_subscriptions

        out = sync_executed_option_subscriptions()
        if out.get("skipped"):
            logger.debug("stock_option_ws_ltp sync skipped: %s", out.get("skipped"))
        else:
            logger.info("stock_option_ws_ltp sync: %s", out)
    except Exception:
        logger.exception("stock_option_ws_ltp sync failed")


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
    # Live Executed option LTPs via shared Upstox WS from 09:30 IST (after Breakfast).
    sch.add_job(
        _ws_ltp_sync,
        CronTrigger(
            day_of_week="mon-fri",
            hour=9,
            minute=30,
            timezone="Asia/Kolkata",
        ),
        id="stock_option_ws_ltp_0930",
        name="Stock Options Executed WS LTP start 09:30",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    sch.add_job(
        _ws_ltp_sync,
        IntervalTrigger(minutes=1, timezone="Asia/Kolkata"),
        id="stock_option_ws_ltp_1m",
        name="Stock Options Executed WS LTP sync 1m (09:30–15:35)",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    sch.start()
    _scheduler = sch
    try:
        _ws_ltp_sync()
    except Exception:
        logger.exception("stock_option_ws_ltp initial sync failed")
    logger.info(
        "Stock Options scheduler started "
        "(EMA 11:15/13:15/15:15/15:45 + Executed WS LTP from 09:30 IST)"
    )


def stop_stock_option_scheduler() -> None:
    global _scheduler
    if _scheduler:
        try:
            _scheduler.shutdown(wait=False)
        finally:
            _scheduler = None
        try:
            from backend.services.upstox_market_feed import set_feed_provider_keys
            from backend.services.stock_option_ws_ltp import PROVIDER_NAME

            set_feed_provider_keys(PROVIDER_NAME, [])
        except Exception:
            logger.debug("stock_option_ws_ltp provider clear on stop failed", exc_info=True)

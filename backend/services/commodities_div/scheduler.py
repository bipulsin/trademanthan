"""APScheduler jobs for Commodities Div In-Trade WS LTP + REST fallback."""
from __future__ import annotations

import logging
from typing import Optional

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from backend.services.commodities_div.ws_ltp import (
    sync_in_trade_subscriptions,
    within_mcx_ltp_session,
)

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

_scheduler: Optional[BackgroundScheduler] = None


def _should_skip_market_jobs() -> bool:
    try:
        from backend.services.market_holiday import should_skip_scheduled_market_jobs_ist

        return bool(should_skip_scheduled_market_jobs_ist())
    except Exception:
        return False


def _ws_ltp_sync() -> None:
    if _should_skip_market_jobs():
        return
    if not within_mcx_ltp_session():
        return
    try:
        out = sync_in_trade_subscriptions()
        if out.get("skipped"):
            logger.debug("commodities_div_ws_ltp sync skipped: %s", out.get("skipped"))
        else:
            logger.info("commodities_div_ws_ltp sync: %s", out)
    except Exception:
        logger.exception("commodities_div_ws_ltp sync failed")


def _rest_ltp_refresh() -> None:
    if _should_skip_market_jobs():
        return
    if not within_mcx_ltp_session():
        return
    try:
        from backend.services.breakfast_upstox_gate import defer_job_for_breakfast_exclusivity

        if defer_job_for_breakfast_exclusivity("commodities_div_rest_ltp"):
            return
    except Exception as e:
        logger.exception("breakfast_exclusivity: check_failed error=%s", e)
    try:
        from backend.services.commodities_div.ltp_sidecar import refresh_in_trade_ltp

        out = refresh_in_trade_ltp()
        logger.info("commodities_div REST LTP: %s", out)
    except Exception:
        logger.exception("commodities_div REST LTP failed")


def start_commodities_div_ltp_scheduler() -> None:
    global _scheduler
    if _scheduler is not None:
        return
    sch = BackgroundScheduler(timezone="Asia/Kolkata")
    # After Breakfast window: sync WS provider every minute during MCX hours.
    sch.add_job(
        _ws_ltp_sync,
        CronTrigger(
            day_of_week="mon-fri",
            hour=9,
            minute=30,
            timezone="Asia/Kolkata",
        ),
        id="commodities_div_ws_ltp_0930",
        name="Commodities Div In-Trade WS LTP start 09:30",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    sch.add_job(
        _ws_ltp_sync,
        IntervalTrigger(minutes=1, timezone="Asia/Kolkata"),
        id="commodities_div_ws_ltp_1m",
        name="Commodities Div In-Trade WS LTP sync 1m",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    # REST fallback every 5 minutes (independent of NSE cash 10m job window).
    sch.add_job(
        _rest_ltp_refresh,
        IntervalTrigger(minutes=5, timezone="Asia/Kolkata"),
        id="commodities_div_rest_ltp_5m",
        name="Commodities Div In-Trade REST LTP 5m",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    sch.start()
    _scheduler = sch
    try:
        _ws_ltp_sync()
        _rest_ltp_refresh()
    except Exception:
        logger.exception("commodities_div LTP initial sync/refresh failed")
    logger.info(
        "Commodities Div LTP scheduler started "
        "(WS sync 1m from 09:30 + REST every 5m during MCX session)"
    )


def stop_commodities_div_ltp_scheduler() -> None:
    global _scheduler
    if _scheduler:
        try:
            _scheduler.shutdown(wait=False)
        finally:
            _scheduler = None
        try:
            from backend.services.upstox_market_feed import set_feed_provider_keys
            from backend.services.commodities_div.ws_ltp import PROVIDER_NAME

            set_feed_provider_keys(PROVIDER_NAME, [])
        except Exception:
            logger.debug("commodities_div_ws_ltp provider clear on stop failed", exc_info=True)

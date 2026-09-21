"""IST 11:15 / 13:15 / 15:15 + post-market 15:45 — Stock Options WR scan + 2h EMAs.

Main ticks (11:15 / 13:15 / 15:15): WR(280) Radar scan over arbitrage_master, then
EMA arm/demote for Radar/Active. Post-market 15:45: EMA tick + Radar EOD cleanup
(no full-universe WR scan). Catch-up 11:45 / 13:45: EMA fetch-failed only.

Contract rollover (Active / Radar-with-contract → arbitrage_master currmth):
- Morning **09:12 IST** Mon–Fri — just after arbitrage daily setup 09:10 metadata roll.
- EOD **15:50 IST** Mon–Fri — after post-market EMA + Radar cleanup at 15:45.
Executed / Completed (Trade report) are never updated.

Also keeps Executed option instrument keys on the shared Upstox WS (1m sync from
09:30 IST after Breakfast) so sell/buy LTPs update live; 2h
``refresh_executed_ltps`` remains the REST fallback.

EMA non-update guardrails:
- Failed fetches set ``ema_fetch_ok=FALSE`` (UI ⚠) without wiping last good EMAs.
- After a main tick with failures, schedule +10m retry for failed rows only; if that
  retry still fails, chain another +10m (up to ``EMA_RETRY_CHAIN_MAX``) while the
  session is open — same path for stocks and NIFTY/BANKNIFTY.
- Mid-session catch-up cron at :45 past 11/13 (and reliance on 15:45 post-market)
  refreshes any leftover ``ema_fetch_ok=FALSE`` rows if a main :15 cycle was missed
  or starved.
- Tick order: indices → fetch-failed Active → Active → Radar.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, time as dt_time

import pytz

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger

from backend.services.market_holiday import should_skip_scheduled_market_jobs_ist
from backend.services.stock_option_signals import (
    ensure_index_radar_rows,
    run_contract_rollover,
    run_ema_tick,
    run_radar_eod_cleanup,
    run_wr_radar_scan,
)

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")
# After Breakfast exclusivity (ends by lock or 09:25); user-facing live LTP from 09:30.
WS_LTP_SESSION_START = dt_time(9, 30)
WS_LTP_SESSION_END = dt_time(15, 35)
EMA_RETRY_DELAY = timedelta(minutes=10)
EMA_RETRY_CHAIN_MAX = 3
EMA_RETRY_JOB_ID = "stock_option_ema_retry"
EMA_CATCHUP_JOB_ID = "stock_option_ema_catchup"

_scheduler: BackgroundScheduler | None = None
_ema_retry_chain: int = 0


def _within_ema_retry_window(now: datetime | None = None) -> bool:
    """Allow chained EMA retries through post-market catch-up window."""
    t = now or datetime.now(IST)
    if t.tzinfo is None:
        t = IST.localize(t)
    else:
        t = t.astimezone(IST)
    tt = t.time()
    return dt_time(9, 20) <= tt <= dt_time(16, 15)


def _schedule_ema_fetch_retry(*, delay: timedelta = EMA_RETRY_DELAY) -> None:
    """Enqueue a one-shot retry for rows left with ema_fetch_ok=FALSE."""
    global _ema_retry_chain
    if _scheduler is None:
        logger.warning("stock_option ema retry: scheduler not started; skip schedule")
        return
    if not _within_ema_retry_window():
        logger.info("stock_option ema retry: outside session window; skip schedule")
        _ema_retry_chain = 0
        return
    if _ema_retry_chain >= EMA_RETRY_CHAIN_MAX:
        logger.info(
            "stock_option ema retry: chain cap %s reached; wait for next main/catch-up tick",
            EMA_RETRY_CHAIN_MAX,
        )
        return
    run_at = datetime.now(IST) + delay
    try:
        _scheduler.add_job(
            _retry_tick,
            DateTrigger(run_date=run_at, timezone="Asia/Kolkata"),
            id=EMA_RETRY_JOB_ID,
            name="Stock Options EMA fetch retry +10m",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=300,
        )
        logger.info(
            "stock_option ema retry scheduled for %s IST (chain=%s/%s)",
            run_at.isoformat(),
            _ema_retry_chain + 1,
            EMA_RETRY_CHAIN_MAX,
        )
    except Exception:
        logger.exception("stock_option ema retry schedule failed")


def _tick() -> None:
    """Main 2h tick: WR Radar scan then EMA arm/demote."""
    global _ema_retry_chain
    if should_skip_scheduled_market_jobs_ist():
        logger.info("stock_option ema: skipped (weekend/holiday)")
        return
    try:
        _ema_retry_chain = 0
        scan_out = run_wr_radar_scan()
        logger.info("stock_option WR scan: %s", scan_out)
        out = run_ema_tick()
        logger.info("stock_option ema tick: %s", out)
        if int(out.get("ema_fetch_failed") or 0) > 0:
            _schedule_ema_fetch_retry()
    except Exception:
        logger.exception("stock_option ema tick failed")


def _post_market_tick() -> None:
    """15:45: EMA only (no full WR universe scan) + Radar EOD WR cleanup."""
    global _ema_retry_chain
    if should_skip_scheduled_market_jobs_ist():
        logger.info("stock_option post-market: skipped (weekend/holiday)")
        return
    try:
        _ema_retry_chain = 0
        out = run_ema_tick()
        logger.info("stock_option post-market ema tick: %s", out)
        if int(out.get("ema_fetch_failed") or 0) > 0:
            _schedule_ema_fetch_retry()
        cleanup = run_radar_eod_cleanup()
        logger.info("stock_option Radar EOD cleanup: %s", cleanup)
    except Exception:
        logger.exception("stock_option post-market tick failed")


def _retry_tick() -> None:
    global _ema_retry_chain
    if should_skip_scheduled_market_jobs_ist():
        logger.info("stock_option ema retry: skipped (weekend/holiday)")
        return
    try:
        _ema_retry_chain += 1
        out = run_ema_tick(only_fetch_failed=True)
        logger.info("stock_option ema retry tick: %s", out)
        if int(out.get("ema_fetch_failed") or 0) > 0:
            _schedule_ema_fetch_retry()
        else:
            _ema_retry_chain = 0
    except Exception:
        logger.exception("stock_option ema retry tick failed")


def _catchup_tick() -> None:
    """Mid-session backup: refresh any leftover ema_fetch_ok=FALSE open rows."""
    global _ema_retry_chain
    if should_skip_scheduled_market_jobs_ist():
        logger.info("stock_option ema catch-up: skipped (weekend/holiday)")
        return
    try:
        out = run_ema_tick(only_fetch_failed=True)
        logger.info("stock_option ema catch-up tick: %s", out)
        if int(out.get("ema_fetch_failed") or 0) > 0:
            _schedule_ema_fetch_retry()
        else:
            _ema_retry_chain = 0
    except Exception:
        logger.exception("stock_option ema catch-up tick failed")


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


def _contract_rollover_tick() -> None:
    """Morning 09:12 / EOD 15:50: roll Active (and Radar-with-contract) to master currmth."""
    if should_skip_scheduled_market_jobs_ist():
        logger.info("stock_option contract rollover: skipped (weekend/holiday)")
        return
    try:
        out = run_contract_rollover()
        logger.info("stock_option contract rollover: %s", out)
    except Exception:
        logger.exception("stock_option contract rollover failed")


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
        name="Stock Options WR scan + 2h EMA 11:15/13:15/15:15",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    # After cash close: EMA settle + Radar EOD WR cleanup (no full universe scan).
    sch.add_job(
        _post_market_tick,
        CronTrigger(
            day_of_week="mon-fri",
            hour=15,
            minute=45,
            timezone="Asia/Kolkata",
        ),
        id="stock_option_ema_post_market",
        name="Stock Options post-market EMA + Radar EOD 15:45",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    # Backup if a main :15 cycle left fetch failures (or was starved): catch-up at :45.
    sch.add_job(
        _catchup_tick,
        CronTrigger(
            day_of_week="mon-fri",
            hour="11,13",
            minute=45,
            timezone="Asia/Kolkata",
        ),
        id=EMA_CATCHUP_JOB_ID,
        name="Stock Options EMA catch-up 11:45/13:45",
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
    # After arbitrage_master 09:10 roll: align Active (and Radar-with-contract) months.
    sch.add_job(
        _contract_rollover_tick,
        CronTrigger(
            day_of_week="mon-fri",
            hour=9,
            minute=12,
            timezone="Asia/Kolkata",
        ),
        id="stock_option_contract_rollover_0912",
        name="Stock Options contract rollover 09:12 (master currmth)",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=600,
    )
    # Post day-end: second pass after 15:45 EMA / Radar cleanup.
    sch.add_job(
        _contract_rollover_tick,
        CronTrigger(
            day_of_week="mon-fri",
            hour=15,
            minute=50,
            timezone="Asia/Kolkata",
        ),
        id="stock_option_contract_rollover_1550",
        name="Stock Options contract rollover 15:50 (master currmth)",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=600,
    )
    sch.start()
    _scheduler = sch
    try:
        _ws_ltp_sync()
    except Exception:
        logger.exception("stock_option_ws_ltp initial sync failed")
    logger.info(
        "Stock Options scheduler started "
        "(WR+EMA 11:15/13:15/15:15; EMA+EOD cleanup 15:45; catch-up 11:45/13:45; "
        "contract rollover 09:12/15:50; "
        "fetch-fail +10m chained retry + Executed WS LTP from 09:30 IST)"
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

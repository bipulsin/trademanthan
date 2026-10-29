"""APScheduler jobs for MomentX 15m scan + 5m EMA refresh."""
from __future__ import annotations

import logging
from datetime import datetime, time as dt_time, timedelta

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger

from backend.services.market_holiday import should_skip_scheduled_market_jobs_ist
from backend.services.momentx.config import CONFIG

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

_scheduler: BackgroundScheduler | None = None


def _now() -> datetime:
    return datetime.now(IST)


def _within_session(t: datetime | None = None) -> bool:
    now = t or _now()
    if now.tzinfo is None:
        now = IST.localize(now)
    else:
        now = now.astimezone(IST)
    open_h, open_m = CONFIG.market_open_hhmm
    close_h, close_m = CONFIG.market_close_hhmm
    tt = now.time()
    return dt_time(open_h, open_m) <= tt <= dt_time(close_h, close_m)


def _after_first_15m(t: datetime | None = None) -> bool:
    now = t or _now()
    if now.tzinfo is None:
        now = IST.localize(now)
    else:
        now = now.astimezone(IST)
    h, m = CONFIG.first_15m_scan_hhmm
    return now.time() >= dt_time(h, m)


def _schedule_delayed(job_id: str, name: str, fn, delay_sec: int) -> None:
    if _scheduler is None:
        return
    run_at = _now() + timedelta(seconds=max(0, int(delay_sec)))
    try:
        _scheduler.add_job(
            fn,
            DateTrigger(run_date=run_at, timezone="Asia/Kolkata"),
            id=job_id,
            name=name,
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=120,
        )
    except Exception:
        logger.exception("momentx schedule delayed job failed id=%s", job_id)


def _on_15m_close_tick() -> None:
    from backend.services.momentx.status_rules import past_scan_cutoff

    if should_skip_scheduled_market_jobs_ist():
        logger.info("momentx 15m: skipped holiday/weekend")
        return
    if not _within_session() or not _after_first_15m():
        return
    if past_scan_cutoff():
        logger.info("momentx 15m: skipped after scan cutoff")
        return
    _schedule_delayed(
        "momentx_scan_delayed",
        "MomentX Stage1 scan (delayed)",
        _run_scan,
        CONFIG.scan_delay_sec,
    )


def _on_5m_close_tick() -> None:
    from backend.services.momentx.status_rules import past_scan_cutoff

    if should_skip_scheduled_market_jobs_ist():
        return
    if not _within_session():
        return
    if past_scan_cutoff():
        return
    _schedule_delayed(
        "momentx_ema_delayed",
        "MomentX Stage2 EMA (delayed)",
        _run_ema,
        CONFIG.ema_delay_sec,
    )


def _run_status_tick() -> None:
    """Apply Active→Locked timeout and 15:00 lock-all without new candle scans."""
    try:
        from backend.services.momentx import state as mx_state
        from backend.services.momentx.status_rules import refresh_board_statuses

        refreshed = refresh_board_statuses(mx_state.ready_cards())
        mx_state.set_ready_cards(refreshed)
    except Exception:
        logger.exception("momentx status tick failed")


def _run_scan() -> None:
    try:
        from backend.services.momentx.scanner import run_stage1_scan

        run_stage1_scan(force=False)
    except Exception:
        logger.exception("momentx scheduled scan failed")


def _run_ema() -> None:
    try:
        from backend.services.momentx.tracker import refresh_emas

        refresh_emas(force=False)
    except Exception:
        logger.exception("momentx scheduled ema failed")


def _run_daily_cache_refresh() -> None:
    if should_skip_scheduled_market_jobs_ist():
        return
    try:
        from backend.services.momentx.candles import clear_daily_cache
        from backend.services.momentx import state as mx_state

        clear_daily_cache()
        mx_state.clear_day_board(reason="market_open")
        logger.info("momentx daily candle cache + day board cleared at market open")
    except Exception:
        logger.exception("momentx daily cache refresh failed")


def _run_ws_sync() -> None:
    if should_skip_scheduled_market_jobs_ist():
        return
    if not _within_session():
        return
    try:
        from backend.services.momentx.ws_ltp import sync_momentx_subscriptions

        sync_momentx_subscriptions()
    except Exception:
        logger.exception("momentx ws sync failed")


def start_momentx_scheduler() -> None:
    global _scheduler
    if _scheduler is not None:
        return
    sched = BackgroundScheduler(timezone="Asia/Kolkata")
    # 15m candle closes: :00 :15 :30 :45 (first meaningful close 09:30)
    sched.add_job(
        _on_15m_close_tick,
        CronTrigger(minute="0,15,30,45", hour="9-15", timezone="Asia/Kolkata"),
        id="momentx_15m_close",
        name="MomentX 15m close tick",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=60,
    )
    # 5m candle closes
    sched.add_job(
        _on_5m_close_tick,
        CronTrigger(minute="*/5", hour="9-15", timezone="Asia/Kolkata"),
        id="momentx_5m_close",
        name="MomentX 5m close tick",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=60,
    )
    # Daily cache clear at market open
    sched.add_job(
        _run_daily_cache_refresh,
        CronTrigger(hour=9, minute=15, timezone="Asia/Kolkata"),
        id="momentx_daily_cache",
        name="MomentX daily cache refresh",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=300,
    )
    # WS subscription sync every minute while session open
    sched.add_job(
        _run_ws_sync,
        CronTrigger(minute="*", hour="9-15", timezone="Asia/Kolkata"),
        id="momentx_ws_sync",
        name="MomentX WS LTP sync",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=30,
    )
    # Status / lock-all tick (no candle fetches)
    sched.add_job(
        _run_status_tick,
        CronTrigger(minute="*", hour="9-15", timezone="Asia/Kolkata"),
        id="momentx_status_tick",
        name="MomentX status / lock tick",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=30,
    )
    sched.start()
    _scheduler = sched
    logger.info("momentx scheduler started (15m scan + 5m EMA + WS sync)")
    try:
        from backend.services.momentx.persistence import ensure_momentx_tables

        ensure_momentx_tables()
    except Exception:
        logger.exception("momentx table ensure failed")
    # After deploy/restart, in-memory Ready is empty — kick a scan if session is open.
    try:
        from backend.services.momentx.status_rules import past_scan_cutoff

        if (
            not should_skip_scheduled_market_jobs_ist()
            and _within_session()
            and _after_first_15m()
            and not past_scan_cutoff()
        ):
            _schedule_delayed(
                "momentx_scan_startup",
                "MomentX Stage1 scan (startup)",
                _run_scan,
                3,
            )
            logger.info("momentx startup scan queued")
    except Exception:
        logger.exception("momentx startup scan queue failed")


def stop_momentx_scheduler() -> None:
    global _scheduler
    if _scheduler is None:
        return
    try:
        _scheduler.shutdown(wait=False)
    except Exception:
        logger.exception("momentx scheduler shutdown failed")
    _scheduler = None

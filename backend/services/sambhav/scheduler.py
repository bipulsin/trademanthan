"""Sambhav live prediction scheduler — after each completed 10m candle (prediction only)."""

from __future__ import annotations

import logging
from datetime import datetime

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from backend.database import SessionLocal
from backend.services import market_holiday as mh
from backend.services.sambhav.config import IST, SESSION_END, SESSION_START
from backend.services.sambhav.tables import ensure_sambhav_tables

logger = logging.getLogger(__name__)

_scheduler: BackgroundScheduler | None = None


def _in_session(now: datetime) -> bool:
    return SESSION_START <= now.time() <= SESSION_END


def _tick() -> None:
    now = mh._normalize_ist(None)
    if mh.should_skip_scheduled_market_jobs_ist(now) or now.weekday() >= 5:
        return
    if not _in_session(now):
        return
    # Fire near :05/:15/:25/:35/:45/:55 — shortly after 09:15-aligned 10m closes
    # (closes at :25,:35,...,:15). Cron below handles minutes.
    db = SessionLocal()
    try:
        ensure_sambhav_tables()
        from backend.services.sambhav.predict import (
            predict_latest,
            refresh_recent_10m,
            resolve_pending_predictions,
        )

        refresh_recent_10m(db, days_back=2)
        resolve_pending_predictions(db)
        out = predict_latest(db, source="live")
        logger.info("sambhav predict tick: %s", {k: out.get(k) for k in ("ok", "candle_start", "p_up_calibrated", "status", "message")})
    except Exception:
        logger.exception("sambhav scheduler tick failed")
    finally:
        db.close()


def start_sambhav_scheduler() -> None:
    global _scheduler
    if _scheduler is not None:
        return
    sch = BackgroundScheduler(timezone="Asia/Kolkata")
    # 10m bars close at 09:25, 09:35, ... 15:25 → run at minute 26,36,...,16 (+1m slack)
    sch.add_job(
        _tick,
        CronTrigger(
            day_of_week="mon-fri",
            hour="9-15",
            minute="6,16,26,36,46,56",
            timezone="Asia/Kolkata",
        ),
        id="sambhav_predict_tick",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    sch.start()
    _scheduler = sch
    logger.info("Sambhav prediction scheduler started (native 10m IST weekdays)")


def stop_sambhav_scheduler() -> None:
    global _scheduler
    if _scheduler:
        try:
            _scheduler.shutdown(wait=False)
        finally:
            _scheduler = None

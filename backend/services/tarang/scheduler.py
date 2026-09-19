"""APScheduler for Kosmic Tarang — IV snapshots + ExitEngine / hard-exit (IST)."""
from __future__ import annotations

import logging

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from backend.services.tarang.config import get_risk
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


def _tick_exit_engine() -> None:
    try:
        from backend.services.tarang.lifecycle import run_exit_engine_once

        out = run_exit_engine_once()
        logger.info("tarang ExitEngine: %s", out.get("checked"))
    except Exception as e:
        logger.exception("tarang ExitEngine failed: %s", e)


def _tick_hard_warn() -> None:
    try:
        from backend.services.tarang.lifecycle import run_hard_exit_warnings

        out = run_hard_exit_warnings()
        if out.get("alerts"):
            logger.info("tarang hard-exit warnings: %s", out)
    except Exception as e:
        logger.exception("tarang hard-exit warning failed: %s", e)


def _tick_hard_flat() -> None:
    try:
        from backend.services.tarang.lifecycle import run_hard_exit_flatten

        out = run_hard_exit_flatten()
        if out.get("results"):
            logger.info("tarang hard-exit flatten: %s", out)
    except Exception as e:
        logger.exception("tarang hard-exit flatten failed: %s", e)


def start_tarang_scheduler() -> None:
    """IV snapshots + ExitEngine poll + venue hard-exit warning/flatten (IST)."""
    global _scheduler
    if _scheduler is not None:
        return
    ensure_tarang_tables()
    sch = BackgroundScheduler(timezone="Asia/Kolkata")
    sch.add_job(
        _tick_iv,
        CronTrigger(minute="0,30", hour="9-23", timezone="Asia/Kolkata"),
        id="tarang_iv_snapshots",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    sch.add_job(
        _tick_iv,
        CronTrigger(hour=8, minute=5, timezone="Asia/Kolkata"),
        id="tarang_iv_snapshots_morning",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    # ExitEngine every minute during session hours
    sch.add_job(
        _tick_exit_engine,
        CronTrigger(minute="*", hour="9-23", timezone="Asia/Kolkata"),
        id="tarang_exit_engine",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    # Also poll crypto hours lightly overnight
    sch.add_job(
        _tick_exit_engine,
        CronTrigger(minute="*/5", hour="0-8", timezone="Asia/Kolkata"),
        id="tarang_exit_engine_night",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )

    risk = get_risk()
    he = risk.get("hard_exits") or {}

    def _hhmm(key: str, default: str) -> tuple[int, int]:
        raw = str(he.get(key) or default)
        parts = raw.split(":")
        return int(parts[0]), int(parts[1]) if len(parts) > 1 else 0

    mcx_w_h, mcx_w_m = _hhmm("mcx_warning_ist", "23:10")
    mcx_f_h, mcx_f_m = _hhmm("mcx_flat_ist", "23:15")
    d_w_h, d_w_m = _hhmm("delta_warning_ist", "16:55")
    d_f_h, d_f_m = _hhmm("delta_flat_ist", "17:00")

    sch.add_job(
        _tick_hard_warn,
        CronTrigger(hour=mcx_w_h, minute=mcx_w_m, timezone="Asia/Kolkata"),
        id="tarang_hard_warn_mcx",
        replace_existing=True,
        max_instances=1,
    )
    sch.add_job(
        _tick_hard_flat,
        CronTrigger(hour=mcx_f_h, minute=mcx_f_m, timezone="Asia/Kolkata"),
        id="tarang_hard_flat_mcx",
        replace_existing=True,
        max_instances=1,
    )
    sch.add_job(
        _tick_hard_warn,
        CronTrigger(hour=d_w_h, minute=d_w_m, timezone="Asia/Kolkata"),
        id="tarang_hard_warn_delta",
        replace_existing=True,
        max_instances=1,
    )
    sch.add_job(
        _tick_hard_flat,
        CronTrigger(hour=d_f_h, minute=d_f_m, timezone="Asia/Kolkata"),
        id="tarang_hard_flat_delta",
        replace_existing=True,
        max_instances=1,
    )

    sch.start()
    _scheduler = sch
    logger.info("Kosmic Tarang scheduler started (IV + ExitEngine + hard-exit IST)")
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

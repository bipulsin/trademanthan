"""APScheduler for Kosmic Tarang — chain snapshots + ExitEngine / hard-exit (IST)."""
from __future__ import annotations

import logging

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from backend.services.tarang.calendar import mcx_session_open, should_run_delta_snapshot
from backend.services.tarang.chain_snapshots import capture_chain_snapshots
from backend.services.tarang.config import get_risk
from backend.services.tarang.schema import ensure_tarang_tables

logger = logging.getLogger(__name__)

_scheduler: BackgroundScheduler | None = None


def _tick_snapshots_delta() -> None:
    try:
        if not should_run_delta_snapshot():
            return
        ensure_tarang_tables()
        out = capture_chain_snapshots(venues=["delta_india"], respect_mcx_session=False)
        logger.info("tarang Delta chain snapshot: %s", out.get("results"))
    except Exception as e:
        logger.exception("tarang Delta chain snapshot failed: %s", e)


def _tick_snapshots_mcx() -> None:
    try:
        if not mcx_session_open():
            return
        ensure_tarang_tables()
        out = capture_chain_snapshots(venues=["upstox_mcx"], respect_mcx_session=True)
        logger.info("tarang MCX chain snapshot: %s", out.get("results"))
    except Exception as e:
        logger.exception("tarang MCX chain snapshot failed: %s", e)


def _tick_exit_engine_delta() -> None:
    try:
        from backend.services.tarang.lifecycle import run_exit_engine_once

        out = run_exit_engine_once(venues=["delta_india"])
        logger.info("tarang ExitEngine delta: %s", out.get("checked"))
    except Exception as e:
        logger.exception("tarang ExitEngine delta failed: %s", e)


def _tick_exit_engine_mcx() -> None:
    try:
        from backend.services.tarang.lifecycle import run_exit_engine_once

        out = run_exit_engine_once(venues=["upstox_mcx"])
        logger.info("tarang ExitEngine mcx: %s", out.get("checked"))
    except Exception as e:
        logger.exception("tarang ExitEngine mcx failed: %s", e)


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


def _tick_feed_health() -> None:
    try:
        from backend.services.tarang.lifecycle import check_feed_alerts

        check_feed_alerts()
    except Exception as e:
        logger.exception("tarang feed health tick failed: %s", e)


def start_tarang_scheduler() -> None:
    """Chain snapshots + ExitEngine + venue hard-exit warning/flatten (IST)."""
    global _scheduler
    if _scheduler is not None:
        return
    ensure_tarang_tables()
    sch = BackgroundScheduler(timezone="Asia/Kolkata")
    # Delta snapshots: every 15m; helper keeps 30m except Fri 18:00–Sun 17:00 IST
    sch.add_job(
        _tick_snapshots_delta,
        CronTrigger(minute="*/15", timezone="Asia/Kolkata"),
        id="tarang_chain_delta",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    # MCX snapshots: in-session 30m, weekdays; job still checks holidays/hours
    sch.add_job(
        _tick_snapshots_mcx,
        CronTrigger(minute="0,30", hour="9-23", day_of_week="mon-fri", timezone="Asia/Kolkata"),
        id="tarang_chain_mcx",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    # ExitEngine: Delta every 1 min 24x7
    sch.add_job(
        _tick_exit_engine_delta,
        CronTrigger(minute="*", timezone="Asia/Kolkata"),
        id="tarang_exit_engine_delta",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    # MCX ExitEngine every minute in session
    sch.add_job(
        _tick_exit_engine_mcx,
        CronTrigger(minute="*", hour="9-23", day_of_week="mon-fri", timezone="Asia/Kolkata"),
        id="tarang_exit_engine_mcx",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    # MCX overnight 5-min (no-op while closed; used around the open)
    sch.add_job(
        _tick_exit_engine_mcx,
        CronTrigger(minute="*/5", hour="0-8", timezone="Asia/Kolkata"),
        id="tarang_exit_engine_mcx_night",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    sch.add_job(
        _tick_feed_health,
        CronTrigger(minute="*/10", timezone="Asia/Kolkata"),
        id="tarang_feed_health",
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
    logger.info("Kosmic Tarang scheduler started (chain snapshots + ExitEngine + hard-exit IST)")
    try:
        sch.add_job(_tick_snapshots_delta, "date", id="tarang_chain_boot_delta", replace_existing=True)
        if mcx_session_open():
            sch.add_job(_tick_snapshots_mcx, "date", id="tarang_chain_boot_mcx", replace_existing=True)
    except Exception:
        pass


def stop_tarang_scheduler() -> None:
    global _scheduler
    if _scheduler:
        try:
            _scheduler.shutdown(wait=False)
        finally:
            _scheduler = None

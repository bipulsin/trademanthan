"""APScheduler for Kosmic Tarang — chain snapshots + ExitEngine / hard-exit (IST)."""
from __future__ import annotations

import logging

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from backend.services.tarang.calendar import mcx_session_open, should_run_delta_snapshot
from backend.services.tarang.chain_snapshots import capture_chain_snapshots
from backend.services.tarang.config import get_risk
from backend.services.tarang.job_lock import try_job_lock
from backend.services.tarang.job_runs import record_job_run
from backend.services.tarang.schema import ensure_tarang_tables

logger = logging.getLogger(__name__)

_scheduler: BackgroundScheduler | None = None


def _locked(job: str, fn, next_hint: str = "") -> None:
    with try_job_lock(job) as got:
        if not got:
            return
        try:
            out = fn()
            record_job_run(job, ok=True, detail=out if isinstance(out, dict) else {}, next_run=next_hint)
        except Exception as e:
            logger.exception("tarang job %s failed", job)
            record_job_run(job, ok=False, detail={"error": str(e)[:200]}, next_run=next_hint)
            from backend.services.tarang.alerts_telegram import notify_ops
            from backend.database import SessionLocal

            db = SessionLocal()
            try:
                notify_ops(db, kind="job_failed", message=f"Job {job} failed: {e}", dedupe_key=f"job_failed:{job}")
                db.commit()
            finally:
                db.close()


def _tick_snapshots_delta(*, force: bool = False) -> None:
    def _run():
        if not force and not should_run_delta_snapshot():
            return {"skipped": "cadence"}
        ensure_tarang_tables()
        out = capture_chain_snapshots(venues=["delta_india"], respect_mcx_session=False)
        logger.info("tarang Delta chain snapshot: %s", out.get("results"))
        _record_expected_misses("delta_india", out)
        from backend.services.tarang.heartbeat import beat

        beat("scheduler", {"job": "delta_snapshots"})
        return out

    _locked("delta_snapshots", _run, "every 15m")


def _tick_snapshots_mcx() -> None:
    def _run():
        if not mcx_session_open():
            return {"skipped": "mcx_session_closed"}
        ensure_tarang_tables()
        out = capture_chain_snapshots(venues=["upstox_mcx"], respect_mcx_session=True)
        logger.info("tarang MCX chain snapshot: %s", out.get("results"))
        _record_expected_misses("upstox_mcx", out)
        return out

    _locked("mcx_snapshots", _run, "in-session 30m")


def _record_expected_misses(venue: str, out: dict) -> None:
    from backend.services.tarang.data_gaps import record_data_gap

    for row in out.get("results") or []:
        if row.get("ok") or row.get("skipped") in ("mcx_session_closed",):
            continue
        if row.get("skipped") == "no_valid_quotes" or row.get("error"):
            # capture_chain_snapshots already inserts a gap in the same txn
            continue
        record_data_gap(
            venue=venue,
            profile_id=row.get("profile_id"),
            reason=str(row.get("skipped") or row.get("error") or "missed_snapshot"),
            detail=row,
        )


def _tick_exit_engine_delta() -> None:
    def _run():
        from backend.services.tarang.lifecycle import run_exit_engine_once

        out = run_exit_engine_once(venues=["delta_india"])
        logger.info("tarang ExitEngine delta: %s", out.get("checked"))
        return out

    _locked("exit_engine", _run, "1m")


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


def _tick_premarket_token(escalate: str = "0845") -> None:
    def _run():
        from backend.services.tarang.token_check import check_upstox_token

        out = check_upstox_token(escalate=escalate)
        logger.info("tarang Upstox token check escalate=%s expired=%s", escalate, out.get("expired"))
        return out

    _locked("token_check", _run, "08:45/09:00/09:05 IST")


def _tick_mcx_bhavcopy() -> None:
    from backend.services.tarang.calendar import to_ist

    ist = to_ist()
    if ist.weekday() >= 5:
        return
    if ist.hour > 12 or (ist.hour == 12 and ist.minute > 5):
        return

    def _run():
        from backend.services.tarang.mcx_download import run_mcx_bhavcopy_download

        return run_mcx_bhavcopy_download()

    _locked("mcx_bhavcopy", _run, "00:30 then hourly until 12:00 IST")


def _tick_delta_candles() -> None:
    def _run():
        from backend.services.tarang.delta_history import load_underlying_1h

        return load_underlying_1h()

    _locked("delta_candles", _run, "daily 00:45 IST")


def _tick_expired_archiver() -> None:
    def _run():
        from backend.services.tarang.delta_history import archive_expired_options

        return archive_expired_options()

    _locked("expired_archiver", _run, "daily 01:15 IST")


def _tick_backup() -> None:
    def _run():
        from backend.services.tarang.backup import run_backup

        return run_backup(dry_run=False)

    _locked("backups", _run, "daily 02:00 IST")


def _tick_weekly_digest() -> None:
    def _run():
        from backend.services.tarang.weekly_digest import send_weekly_digest

        return send_weekly_digest()

    _locked("weekly_digest", _run, "Sunday 18:00 IST")


def _tick_liquidity_probe() -> None:
    try:
        if not mcx_session_open():
            return
        from backend.services.tarang.liquidity_probe import run_mcx_liquidity_probe

        out = run_mcx_liquidity_probe()
        logger.info("tarang MCX liquidity probe: %s underlyings skipped=%s", len(out.get("underlyings") or []), out.get("skipped"))
    except Exception as e:
        logger.exception("tarang liquidity probe failed: %s", e)


def _tick_forward_tests() -> None:
    def _run():
        from backend.services.tarang.lifecycle import run_auto_paper_once

        out = run_auto_paper_once()
        if out.get("taken") or out.get("forward_tests"):
            logger.info("tarang forward-test tick: %s", out)
        return out

    _locked("screener", _run, "5m")


def _tick_watchdog() -> None:
    try:
        from backend.services.tarang.heartbeat import check_watchdog, recon_job

        check_watchdog()
        recon_job()
    except Exception as e:
        logger.exception("tarang watchdog failed: %s", e)


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
    sch.add_job(
        _tick_premarket_token,
        CronTrigger(hour=8, minute=45, timezone="Asia/Kolkata"),
        id="tarang_premarket_token",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=3600,
        kwargs={"escalate": "0845"},
    )
    sch.add_job(
        _tick_premarket_token,
        CronTrigger(hour=9, minute=0, timezone="Asia/Kolkata"),
        id="tarang_premarket_token_0900",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=1800,
        kwargs={"escalate": "0900"},
    )
    sch.add_job(
        _tick_premarket_token,
        CronTrigger(hour=9, minute=5, timezone="Asia/Kolkata"),
        id="tarang_premarket_token_0905",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=1800,
        kwargs={"escalate": "0905"},
    )
    sch.add_job(
        _tick_mcx_bhavcopy,
        CronTrigger(minute=30, hour="0-12", day_of_week="mon-fri", timezone="Asia/Kolkata"),
        id="tarang_mcx_bhavcopy",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=2400,
    )
    sch.add_job(
        _tick_delta_candles,
        CronTrigger(hour=0, minute=45, timezone="Asia/Kolkata"),
        id="tarang_delta_candles",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=7200,
    )
    sch.add_job(
        _tick_expired_archiver,
        CronTrigger(hour=1, minute=15, timezone="Asia/Kolkata"),
        id="tarang_expired_archiver",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=7200,
    )
    sch.add_job(
        _tick_backup,
        CronTrigger(hour=2, minute=0, timezone="Asia/Kolkata"),
        id="tarang_backup",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=7200,
    )
    sch.add_job(
        _tick_weekly_digest,
        CronTrigger(day_of_week="sun", hour=18, minute=0, timezone="Asia/Kolkata"),
        id="tarang_weekly_digest",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=7200,
    )
    sch.add_job(
        _tick_liquidity_probe,
        CronTrigger(hour="10,15,19,22", minute=0, day_of_week="mon-fri", timezone="Asia/Kolkata"),
        id="tarang_mcx_liquidity_probe",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    sch.add_job(
        _tick_forward_tests,
        CronTrigger(minute="*/5", timezone="Asia/Kolkata"),
        id="tarang_forward_tests",
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
    sch.add_job(
        _tick_watchdog,
        CronTrigger(minute="*", timezone="Asia/Kolkata"),
        id="tarang_watchdog",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    logger.info("Kosmic Tarang scheduler started (snapshots + ExitEngine + forward-test record + watchdog IST)")
    try:
        sch.add_job(
            _tick_snapshots_delta,
            "date",
            id="tarang_chain_boot_delta",
            replace_existing=True,
            kwargs={"force": True},
        )
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

"""IST refresh of the Crypto Options Report. Crypto session, every calendar day."""
from __future__ import annotations

import logging
import threading
from typing import Callable, List, Optional, Sequence, Tuple

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from backend.services.crypto_options_report.job import run_crypto_options_refresh

logger = logging.getLogger(__name__)

# Asia/Kolkata, every day. Minute 15 except the pre-close pass at 17:45.
SCHEDULE_IST: Sequence[Tuple[int, int]] = (
    (18, 15),
    (19, 15),
    (23, 15),
    (2, 15),
    (7, 15),
    (10, 15),
    (17, 45),
)

_scheduler: Optional[BackgroundScheduler] = None


def register_crypto_options_jobs(
    scheduler: BackgroundScheduler,
    func: Optional[Callable[[], None]] = None,
) -> List[str]:
    """Attach one cron job per IST slot. Returns job ids."""
    tick = func or _tick
    ids: List[str] = []
    for hour, minute in SCHEDULE_IST:
        job_id = f"crypto_options_report_{hour:02d}{minute:02d}"
        scheduler.add_job(
            tick,
            CronTrigger(hour=hour, minute=minute, timezone="Asia/Kolkata"),
            id=job_id,
            name=f"Crypto Options Report {hour:02d}:{minute:02d} IST",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=900,
        )
        ids.append(job_id)
    return ids


def _tick() -> None:
    try:
        run_crypto_options_refresh(trigger="scheduled")
    except Exception:
        logger.exception("crypto options report scheduled refresh failed")


def _startup_catchup() -> None:
    try:
        run_crypto_options_refresh(trigger="startup")
    except Exception:
        logger.exception("crypto options report startup refresh failed")


def start_crypto_options_scheduler() -> None:
    global _scheduler
    if _scheduler is not None:
        return
    sch = BackgroundScheduler(timezone="Asia/Kolkata")
    register_crypto_options_jobs(sch)
    sch.start()
    _scheduler = sch
    threading.Thread(target=_startup_catchup, name="crypto-options-startup", daemon=True).start()
    logger.info(
        "Crypto Options Report scheduler started (%s)",
        ", ".join(f"{h:02d}:{m:02d}" for h, m in SCHEDULE_IST),
    )


def stop_crypto_options_scheduler() -> None:
    global _scheduler
    if _scheduler is not None:
        try:
            _scheduler.shutdown(wait=False)
        finally:
            _scheduler = None

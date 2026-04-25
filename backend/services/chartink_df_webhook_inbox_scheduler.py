"""
Periodic cleanup of ChartInk Daily Futures raw webhook files on disk.

IST 08:45 daily: if at least N calendar days (default 5) have passed since the last
refresh, delete ``*.raw.json`` and ``*.raw.bear.json`` from both inbox directories.

State: ``logs/chartink_df_inbox_refresh_state.json`` (``last_refresh_date_ist``).

Env:
  CHARTINK_DF_INBOX_REFRESH_ENABLED=1   (default on; set 0 to disable)
  CHARTINK_DF_INBOX_REFRESH_DAYS=5        (min interval in days; default 5)
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Optional

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from backend.services.daily_futures_service import refresh_chartink_webhook_inbox_dirs
from backend.services.market_holiday import IST

logger = logging.getLogger(__name__)

_PROJ_ROOT = Path(__file__).resolve().parents[2]
_STATE_PATH = _PROJ_ROOT / "logs" / "chartink_df_inbox_refresh_state.json"

_scheduler: Optional[BackgroundScheduler] = None


def _read_state() -> Optional[Dict[str, Any]]:
    if not _STATE_PATH.exists():
        return None
    try:
        return json.loads(_STATE_PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as e:
        logger.warning("chartink df inbox: could not read state: %s", e)
        return None


def _write_state(today_ist: date) -> None:
    _STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(
        {
            "last_refresh_date_ist": today_ist.isoformat(),
            "recorded_at_ist": datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S"),
        },
        ensure_ascii=True,
        sort_keys=True,
    )
    fd, tmp = tempfile.mkstemp(
        dir=str(_STATE_PATH.parent), prefix="chartink_inbox_state_", suffix=".json.tmp", text=True
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(payload)
        os.replace(tmp, str(_STATE_PATH))
    except OSError as e:
        logger.warning("chartink df inbox: could not write state: %s", e)
        try:
            os.unlink(tmp)
        except OSError:
            pass


def _maybe_refresh_job() -> None:
    if (os.getenv("CHARTINK_DF_INBOX_REFRESH_ENABLED") or "1").strip().lower() in (
        "0",
        "false",
        "no",
        "off",
    ):
        return
    try:
        interval = int((os.getenv("CHARTINK_DF_INBOX_REFRESH_DAYS") or "5").strip() or 5)
    except ValueError:
        interval = 5
    if interval < 1:
        interval = 1

    today_ist = datetime.now(IST).date()
    st = _read_state()
    if st is None:
        _write_state(today_ist)
        logger.info(
            "chartink df inbox: initialized state (no file purge on first run); next refresh in %d day(s)",
            interval,
        )
        return

    last_raw = (st.get("last_refresh_date_ist") or "").strip()
    try:
        last = date.fromisoformat(last_raw) if last_raw else None
    except ValueError:
        last = None
    if last is None:
        _write_state(today_ist)
        logger.info("chartink df inbox: repaired state (invalid date); no purge this run")
        return

    if (today_ist - last).days < interval:
        return

    out = refresh_chartink_webhook_inbox_dirs()
    _write_state(today_ist)
    logger.info(
        "chartink df inbox: refreshed (removed %s file(s)); detail=%s",
        out.get("files_removed", 0),
        out,
    )


def start_chartink_df_webhook_inbox_scheduler() -> None:
    global _scheduler
    if _scheduler is not None and _scheduler.running:
        return
    _scheduler = BackgroundScheduler(timezone="Asia/Kolkata")
    _scheduler.add_job(
        _maybe_refresh_job,
        trigger=CronTrigger(hour=8, minute=45, second=0, timezone="Asia/Kolkata"),
        id="chartink_df_webhook_inbox_refresh",
        replace_existing=True,
    )
    _scheduler.start()
    logger.info("chartink df inbox scheduler: started (08:45 IST daily, refresh every N days per state)")


def stop_chartink_df_webhook_inbox_scheduler() -> None:
    global _scheduler
    if _scheduler is None or not _scheduler.running:
        return
    _scheduler.shutdown(wait=False)
    _scheduler = None
    logger.info("chartink df inbox scheduler: stopped")

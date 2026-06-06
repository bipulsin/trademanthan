"""Orchestration entry points for Volume Mismatch Futures."""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, Optional

import pytz

from backend.services.volume_mismatch.monitor import (
    expire_stale_signals,
    is_monitor_window,
    run_volume_mismatch_entry_monitor,
)
from backend.services.volume_mismatch.scanner import run_volume_mismatch_scan

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")


def is_scan_window(now: Optional[datetime] = None) -> bool:
    t = now or datetime.now(IST)
    if t.weekday() >= 5:
        return False
    t = t.astimezone(IST) if t.tzinfo else IST.localize(t)
    # Allow scan from 09:30:30 through 10:00 (misfire grace)
    m = t.hour * 60 + t.minute
    return (9 * 60 + 30) <= m <= (10 * 60)


def run_volume_mismatch_daily_scan_job() -> Dict[str, Any]:
    if not is_scan_window():
        return {"success": True, "skipped": "outside_scan_window"}
    return run_volume_mismatch_scan()


def run_volume_mismatch_monitor_job() -> Dict[str, Any]:
    now = datetime.now(IST)
    if not is_monitor_window(now):
        if now.weekday() < 5 and (now.hour * 60 + now.minute) > 15 * 60 + 30:
            expire_stale_signals()
        return {"success": True, "skipped": "outside_monitor_window"}
    return run_volume_mismatch_entry_monitor()

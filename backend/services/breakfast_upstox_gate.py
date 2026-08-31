"""System-wide Upstox priority window for Breakfast 9:15–9:20:30 IST.

During the window, non-breakfast callers are blocked at ``UpstoxService.make_api_request``.
Breakfast scheduler ticks run under ``breakfast_upstox_priority_owner()`` so they retain access.
"""
from __future__ import annotations

import logging
import threading
from contextlib import contextmanager
from datetime import datetime, time as dt_time
from typing import Optional

import pytz

from backend.services.market_holiday import is_nse_holiday_ist

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

PRIORITY_WINDOW_START = dt_time(9, 15)
PRIORITY_WINDOW_END = dt_time(9, 20, 30)

_OWNER = threading.local()
_BLOCKED_LOG_ONCE: set[str] = set()
_BLOCKED_LOG_LOCK = threading.Lock()


def _now_ist(now: Optional[datetime] = None) -> datetime:
    if now is None:
        return datetime.now(IST)
    if now.tzinfo is None:
        return IST.localize(now)
    return now.astimezone(IST)


def _is_trading_day_ist(now: datetime) -> bool:
    if now.weekday() >= 5:
        return False
    noon = IST.localize(datetime.combine(now.date(), dt_time(12, 0)))
    return not is_nse_holiday_ist(noon)


def is_breakfast_priority_window(now: Optional[datetime] = None) -> bool:
    """True on NSE trading days between 09:15:00 and 09:20:30 IST inclusive."""
    t = _now_ist(now)
    if not _is_trading_day_ist(t):
        return False
    tt = t.time()
    return PRIORITY_WINDOW_START <= tt <= PRIORITY_WINDOW_END


def breakfast_priority_owner_active() -> bool:
    return bool(getattr(_OWNER, "active", False))


def breakfast_upstox_allowed(*, caller: str = "") -> bool:
    """Return True when an Upstox API call may proceed."""
    if not is_breakfast_priority_window():
        return True
    if breakfast_priority_owner_active():
        return True
    tag = str(caller or "unknown")
    with _BLOCKED_LOG_LOCK:
        if tag not in _BLOCKED_LOG_ONCE:
            _BLOCKED_LOG_ONCE.add(tag)
            logger.info(
                "breakfast_upstox_gate: blocked Upstox call from %s during priority window",
                tag,
            )
    return False


@contextmanager
def breakfast_upstox_priority_owner():
    """Mark current thread as the Breakfast owner for Upstox API access."""
    prev = getattr(_OWNER, "active", False)
    _OWNER.active = True
    try:
        yield
    finally:
        _OWNER.active = prev


def reset_blocked_log_cache() -> None:
    """Test helper — clear one-shot blocked-call log keys."""
    with _BLOCKED_LOG_LOCK:
        _BLOCKED_LOG_ONCE.clear()

"""System-wide Upstox priority window for Breakfast 9:15–9:20:30 IST.

During the window, non-breakfast callers are blocked at ``UpstoxService.make_api_request``.
Breakfast scheduler ticks run under ``breakfast_upstox_priority_owner()`` so they retain access.

Separately, ``breakfast_exclusivity_active`` covers 09:10 IST until today's freeze lock is
``locked`` or ``failed`` (including ``no_data``), or until 09:25 IST, whichever is first.
"""
from __future__ import annotations

import contextvars
import logging
import threading
import time
from contextlib import contextmanager
from datetime import datetime, time as dt_time
from typing import Optional

import pytz

from backend.services.market_holiday import is_nse_holiday_ist

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

PRIORITY_WINDOW_START = dt_time(9, 15)
PRIORITY_WINDOW_END = dt_time(9, 20, 30)
EXCLUSIVITY_START = dt_time(9, 10)
EXCLUSIVITY_HARD_CEILING = dt_time(9, 25)
_TERMINAL_LOCK = frozenset({"locked", "failed"})

_OWNER = threading.local()
_OWNER_CV: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "breakfast_upstox_owner", default=False
)
_BLOCKED_LOG_ONCE: set[str] = set()
_RELEASE_LOG_ONCE: set[str] = set()
_BLOCKED_LOG_LOCK = threading.Lock()
_LOCK_STATUS_CACHE: tuple[str, Optional[str], float] = ("", None, 0.0)
_LOCK_CACHE_TTL_SEC = 2.0
_LOCK_CACHE_GUARD = threading.Lock()


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


def _today_freeze_lock_status(session_date: str) -> Optional[str]:
    """Latest breakfast_session_lock.lock_status for the IST date, cached briefly."""
    global _LOCK_STATUS_CACHE
    sd = str(session_date or "")[:10]
    now_m = time.monotonic()
    with _LOCK_CACHE_GUARD:
        cached_sd, cached_st, cached_at = _LOCK_STATUS_CACHE
        if cached_sd == sd and (now_m - cached_at) < _LOCK_CACHE_TTL_SEC:
            return cached_st
    status: Optional[str] = None
    try:
        from backend.services.breakfast_strategy.live_persist import fetch_session_lock

        row = fetch_session_lock(sd)
        raw = str((row or {}).get("lock_status") or "").strip().lower()
        status = raw or None
    except Exception as e:
        logger.debug("breakfast_exclusivity: lock lookup failed: %s", e)
        status = None
    with _LOCK_CACHE_GUARD:
        _LOCK_STATUS_CACHE = (sd, status, time.monotonic())
    return status


def _log_exclusivity_release_once(reason: str) -> None:
    with _BLOCKED_LOG_LOCK:
        if reason in _RELEASE_LOG_ONCE:
            return
        _RELEASE_LOG_ONCE.add(reason)
    logger.info("breakfast_exclusivity: released reason=%s", reason)


def breakfast_exclusivity_active(now: Optional[datetime] = None) -> bool:
    """True from 09:10 IST until freeze lock is locked/failed, or 09:25 IST, whichever first.

    Missing lock row keeps exclusivity on after 09:10 until the 09:25 hard ceiling.
    """
    t = _now_ist(now)
    if not _is_trading_day_ist(t):
        return False
    if t.time() < EXCLUSIVITY_START:
        return False
    status = _today_freeze_lock_status(t.date().isoformat())
    if status in _TERMINAL_LOCK:
        _log_exclusivity_release_once("lock_resolved")
        return False
    if t.time() >= EXCLUSIVITY_HARD_CEILING:
        _log_exclusivity_release_once("hard_ceiling_09_25")
        return False
    return True


def breakfast_priority_owner_active() -> bool:
    return bool(_OWNER_CV.get()) or bool(getattr(_OWNER, "active", False))


def breakfast_upstox_allowed(*, caller: str = "") -> bool:
    """Return True when an Upstox API call may proceed."""
    if breakfast_priority_owner_active():
        return True
    window = is_breakfast_priority_window()
    exclusive = False if window else breakfast_exclusivity_active()
    if not window and not exclusive:
        return True
    tag = str(caller or "unknown")
    reason = "priority_window" if window else "exclusivity"
    with _BLOCKED_LOG_LOCK:
        key = f"{reason}:{tag}"
        if key not in _BLOCKED_LOG_ONCE:
            _BLOCKED_LOG_ONCE.add(key)
            logger.info(
                "breakfast_upstox_gate: blocked Upstox call from %s (%s)",
                tag,
                reason,
            )
    return False


def defer_job_for_breakfast_exclusivity(job_id: str) -> bool:
    """True when a non-Breakfast job must skip this fire (shared REST/WS exclusivity)."""
    if not breakfast_exclusivity_active():
        return False
    logger.info(
        "breakfast_exclusivity: deferred job_id=%s until freeze lock locked/failed or 09:25",
        job_id,
    )
    return True


@contextmanager
def breakfast_upstox_priority_owner():
    """Mark current context as the Breakfast owner (thread-local + ContextVar for TPE)."""
    prev_tl = getattr(_OWNER, "active", False)
    token = _OWNER_CV.set(True)
    _OWNER.active = True
    try:
        yield
    finally:
        _OWNER.active = prev_tl
        _OWNER_CV.reset(token)


def reset_blocked_log_cache() -> None:
    """Test helper — clear one-shot blocked-call log keys and lock-status cache."""
    global _LOCK_STATUS_CACHE
    with _BLOCKED_LOG_LOCK:
        _BLOCKED_LOG_ONCE.clear()
        _RELEASE_LOG_ONCE.clear()
    with _LOCK_CACHE_GUARD:
        _LOCK_STATUS_CACHE = ("", None, 0.0)

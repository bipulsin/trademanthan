"""IST session / weekend-window helpers for Kosmic Tarang snapshots and exits."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")

# MCX energy: 09:00–23:30 IST on exchange days.
MCX_OPEN_MINUTES = 9 * 60
MCX_CLOSE_MINUTES = 23 * 60 + 30

# Crypto weekend window: Friday 18:00 IST → Sunday 17:00 IST (inclusive of the bounds).
WEEKEND_FRI_START_MIN = 18 * 60
WEEKEND_SUN_END_MIN = 17 * 60

_DOW_NAMES = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")


def to_ist(now: Optional[datetime] = None) -> datetime:
    if now is None:
        return datetime.now(IST)
    if now.tzinfo is None:
        return now.replace(tzinfo=timezone.utc).astimezone(IST)
    return now.astimezone(IST)


def ist_clock(now: Optional[datetime] = None) -> Dict[str, Any]:
    ist = to_ist(now)
    return {
        "ist_dow": ist.weekday(),  # 0=Mon … 6=Sun
        "ist_dow_name": _DOW_NAMES[ist.weekday()],
        "ist_hour": ist.hour,
        "ist_minute": ist.minute,
        "ist_iso": ist.isoformat(),
        "ist_weekend_window": in_crypto_weekend_window(ist),
    }


def _minutes(ist: datetime) -> int:
    return ist.hour * 60 + ist.minute


def in_crypto_weekend_window(now: Optional[datetime] = None) -> bool:
    """Friday 18:00 IST through Sunday 17:00 IST."""
    ist = to_ist(now)
    wd = ist.weekday()
    mins = _minutes(ist)
    if wd == 4 and mins >= WEEKEND_FRI_START_MIN:
        return True
    if wd == 5:
        return True
    if wd == 6 and mins <= WEEKEND_SUN_END_MIN:
        return True
    return False


def should_run_delta_snapshot(now: Optional[datetime] = None) -> bool:
    """30-minute cadence 24x7; 15-minute cadence inside the weekend window."""
    ist = to_ist(now)
    if in_crypto_weekend_window(ist):
        return ist.minute % 15 == 0
    return ist.minute in (0, 30)


def mcx_is_holiday_or_weekend(now: Optional[datetime] = None) -> bool:
    try:
        from backend.services.market_holiday import should_skip_scheduled_market_jobs_ist

        return bool(should_skip_scheduled_market_jobs_ist(to_ist(now)))
    except Exception:
        ist = to_ist(now)
        return ist.weekday() >= 5


def mcx_session_open(now: Optional[datetime] = None) -> bool:
    """True during MCX energy session: weekday (non-holiday) 09:00–23:30 IST."""
    if mcx_is_holiday_or_weekend(now):
        return False
    ist = to_ist(now)
    return MCX_OPEN_MINUTES <= _minutes(ist) < MCX_CLOSE_MINUTES


def mcx_feed_closed(now: Optional[datetime] = None) -> bool:
    """Closed period 23:30–09:00 IST, plus weekends and exchange holidays."""
    return not mcx_session_open(now)

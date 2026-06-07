"""
IST session_date for Smart Futures UI and DB rows — single source of truth.

Must match what GET /smart-futures/daily filters on.
"""
from __future__ import annotations

from datetime import date, datetime, time, timedelta
from typing import Optional, Tuple

import pytz

from backend.services.market_holiday import is_nse_holiday_ist, should_skip_scheduled_market_jobs_ist

IST = pytz.timezone("Asia/Kolkata")

VMF_SCAN_READY_TIME = time(9, 30)


def _prev_trading_day(d: date) -> date:
    x = d - timedelta(days=1)
    for _ in range(10):
        if x.weekday() < 5:
            return x
        x -= timedelta(days=1)
    return d - timedelta(days=1)


def is_nse_trading_day(d: date) -> bool:
    """Weekday that is not listed in the NSE ``holiday`` table."""
    if d.weekday() >= 5:
        return False
    noon = IST.localize(datetime.combine(d, time(12, 0)))
    return not is_nse_holiday_ist(noon)


def previous_nse_trading_day(before: date) -> date:
    """Last NSE session strictly before ``before`` (skips weekends and holidays)."""
    x = before - timedelta(days=1)
    for _ in range(15):
        if is_nse_trading_day(x):
            return x
        x -= timedelta(days=1)
    return before - timedelta(days=1)


def vmf_live_sections_ist(
    now_ist: Optional[datetime] = None,
) -> Tuple[date, date, bool, Optional[str], bool]:
    """
    Volume Mismatch Futures live page session split.

    Returns:
        (today_calendar_date, previous_trading_day, market_closed, closed_reason, awaiting_scan)

    ``today_calendar_date`` is always the IST calendar date (even on weekends/holidays).
    ``previous_trading_day`` skips weekends and NSE holidays (Monday → Friday, etc.).
    """
    now = now_ist or datetime.now(IST)
    if now.tzinfo is None:
        now = IST.localize(now)
    else:
        now = now.astimezone(IST)
    today = now.date()
    prev = previous_nse_trading_day(today)
    if should_skip_scheduled_market_jobs_ist(now):
        reason = "weekend" if today.weekday() >= 5 else "holiday"
        return today, prev, True, reason, False
    awaiting = now.time() < VMF_SCAN_READY_TIME
    return today, prev, False, None, awaiting


def effective_session_date_ist_for_trend(now_ist: Optional[datetime] = None) -> date:
    """
    Session date for Today's Trend (aligned with cash open 9:15 IST and first picker run).

    Before 9:15 on a weekday → previous trading day. From 9:15 → calendar ``d``.
    Weekend → last Friday's session_date (Sat/Sun).
    """
    now = now_ist or datetime.now(IST)
    if now.tzinfo is None:
        now = IST.localize(now)
    else:
        now = now.astimezone(IST)
    d = now.date()
    wd = d.weekday()
    if wd == 5:
        return d - timedelta(days=1)
    if wd == 6:
        return d - timedelta(days=2)
    if now.time() < time(9, 15):
        return _prev_trading_day(d)
    return d

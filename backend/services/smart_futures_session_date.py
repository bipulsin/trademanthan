"""
IST session_date for Smart Futures UI and DB rows — single source of truth.

Must match what GET /smart-futures/daily filters on.
"""
from __future__ import annotations

from datetime import date, datetime, time, timedelta
from typing import Optional

import pytz

IST = pytz.timezone("Asia/Kolkata")


def _prev_trading_day(d: date) -> date:
    x = d - timedelta(days=1)
    for _ in range(10):
        if x.weekday() < 5:
            return x
        x -= timedelta(days=1)
    return d - timedelta(days=1)


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

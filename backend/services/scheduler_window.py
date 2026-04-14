"""
IST window for scheduled job *execution*: 08:30–21:00 (inclusive of both endpoints).
Used to skip interval-triggered work outside this window (APScheduler still ticks).
Morning 8:10 Telegram ping is a deliberate exception and does not use this guard.

Market-data fetch jobs also skip Saturdays/Sundays and dates in the ``holiday`` table
(see ``backend.services.market_holiday``).
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

import pytz

IST = pytz.timezone("Asia/Kolkata")


def is_allowed_scheduler_window_ist(now: Optional[datetime] = None) -> bool:
    """True if local IST clock time is between 08:30 and 21:00 inclusive."""
    if now is None:
        now = datetime.now(IST)
    elif now.tzinfo is None:
        now = IST.localize(now)
    else:
        now = now.astimezone(IST)
    minutes = now.hour * 60 + now.minute
    start = 8 * 60 + 30
    end = 21 * 60
    return start <= minutes <= end

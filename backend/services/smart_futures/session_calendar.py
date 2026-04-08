"""
IST session date for Smart Futures (UI, positions, scanner persistence).

Before 08:50 IST on a calendar day, the effective session key is the **previous calendar day**
(so overnight / pre-roll access shows the last session’s rows). From 08:50 IST onward, the
effective session key is **today** (aligned with the trading day “rotation” around 9 AM).
"""
from __future__ import annotations

from datetime import date, datetime, time, timedelta
from typing import Optional

import pytz

IST = pytz.timezone("Asia/Kolkata")

# Inclusive from this clock time on the IST calendar day: session_date == today.
# Before this time: session_date == yesterday (calendar).
SMART_FUTURES_SESSION_ROLL_TIME_IST = time(8, 50)


def effective_session_date_ist(now: Optional[datetime] = None) -> date:
    if now is None:
        now = datetime.now(IST)
    elif now.tzinfo is None:
        now = IST.localize(now)
    else:
        now = now.astimezone(IST)
    d = now.date()
    if now.time() < SMART_FUTURES_SESSION_ROLL_TIME_IST:
        return d - timedelta(days=1)
    return d

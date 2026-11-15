"""Trading days and front-month expiry for the EMA scan.

The front contract is the monthly future whose expiry is still ahead, including
the expiry session. The next contract is used from the following session.
This does not use the platform's early roll (day 18 / day 20).
"""
from __future__ import annotations

from datetime import date, datetime, time, timedelta
from typing import Dict, List, Set

import pytz

from backend.services.mtf_swing.bars import is_trading_day
from backend.services.mtf_swing.data import known_holidays, monthly_expiry

IST = pytz.timezone("Asia/Kolkata")
SESSION_OPEN = time(9, 15)
SESSION_CLOSE = time(15, 30)

STUDY_FROM = date(2026, 10, 20)
STUDY_TO = date(2026, 11, 13)


def holidays() -> Set[date]:
    return known_holidays()


def trading_days(start: date, end: date, hols: Set[date] | None = None) -> List[date]:
    hols = holidays() if hols is None else hols
    out: List[date] = []
    cursor = start
    while cursor <= end:
        if is_trading_day(cursor, hols):
            out.append(cursor)
        cursor += timedelta(days=1)
    return out


def previous_trading_day(day: date, hols: Set[date] | None = None) -> date | None:
    hols = holidays() if hols is None else hols
    cursor = day - timedelta(days=1)
    for _ in range(12):
        if is_trading_day(cursor, hols):
            return cursor
        cursor -= timedelta(days=1)
    return None


def front_expiry(day: date, hols: Set[date] | None = None) -> date:
    """Expiry of the front-month future valid on ``day`` (expiry session included)."""
    hols = holidays() if hols is None else hols
    expiry = monthly_expiry(day.year, day.month, hols)
    if day <= expiry:
        return expiry
    year, month = day.year, day.month + 1
    if month == 13:
        year, month = year + 1, 1
    return monthly_expiry(year, month, hols)


def expiry_by_day(days: List[date], hols: Set[date] | None = None) -> Dict[date, date]:
    hols = holidays() if hols is None else hols
    return {day: front_expiry(day, hols) for day in days}


def session_open_dt(day: date) -> datetime:
    return IST.localize(datetime.combine(day, SESSION_OPEN))


def candle_start(day: date, candle_number: int, bar_minutes: int = 5) -> datetime:
    """Candle numbers are 1-based. Candle 1 starts at 09:15 IST."""
    start = session_open_dt(day) + timedelta(minutes=(int(candle_number) - 1) * int(bar_minutes))
    return start


def in_cash_session(ts: datetime, bar_minutes: int = 5) -> bool:
    """True when a bar starts at or after 09:15 and ends at or before 15:30 IST."""
    local = ts.astimezone(IST) if ts.tzinfo else IST.localize(ts)
    start_t = local.time()
    end_t = (local + timedelta(minutes=int(bar_minutes))).time()
    return SESSION_OPEN <= start_t and end_t <= SESSION_CLOSE

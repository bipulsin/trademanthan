"""IST wall-clock helpers for naive PostgreSQL timestamp columns."""
from __future__ import annotations

from datetime import datetime
from typing import Optional

import pytz

IST = pytz.timezone("Asia/Kolkata")


def naive_ist(dt: datetime) -> datetime:
    """Persist as naive IST wall clock (timestamp without time zone)."""
    if dt.tzinfo is None:
        out = dt
    else:
        out = dt.astimezone(IST).replace(tzinfo=None)
    return out.replace(microsecond=0)


def ist_midnight(dt: datetime) -> datetime:
    """Naive IST 00:00:00 for the calendar day of ``dt``."""
    if dt.tzinfo is not None:
        local = dt.astimezone(IST)
    else:
        local = dt
    return datetime(local.year, local.month, local.day)


def ist_isoformat(dt: Optional[datetime]) -> Optional[str]:
    """ISO-8601 with +05:30 for scan UI / API consumers."""
    if dt is None:
        return None
    if dt.tzinfo is not None:
        local = dt.astimezone(IST)
    else:
        local = IST.localize(dt)
    return local.strftime("%Y-%m-%dT%H:%M:%S") + "+05:30"

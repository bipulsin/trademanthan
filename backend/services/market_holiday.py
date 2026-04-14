"""
IST trading calendar helpers: weekends + rows in ``holiday`` (NSE closed dates).

Scheduled jobs that fetch market/exchange data should bail when
``should_skip_scheduled_market_jobs_ist()`` is true. Dates in the DB are
calendar dates in Asia/Kolkata (no separate timezone column).
"""

from __future__ import annotations

import logging
import threading
import time
from datetime import date, datetime
from typing import Optional, Set

import pytz
from sqlalchemy import text
from sqlalchemy.exc import OperationalError, ProgrammingError

from backend.database import SessionLocal

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")

_cache_lock = threading.Lock()
_cached_holiday_dates: Set[date] = set()
_cache_mono: float = 0.0
CACHE_TTL_SEC = 300.0


def _normalize_ist(now: Optional[datetime]) -> datetime:
    if now is None:
        return datetime.now(IST)
    if now.tzinfo is None:
        return IST.localize(now)
    return now.astimezone(IST)


def refresh_holiday_dates_from_db() -> Set[date]:
    """Load holiday dates from PostgreSQL; safe if table is missing (returns empty set)."""
    global _cached_holiday_dates, _cache_mono
    out: Set[date] = set()
    db = None
    try:
        db = SessionLocal()
        result = db.execute(text("SELECT holiday_date FROM holiday"))
        for row in result.fetchall():
            raw = row[0]
            if raw is None:
                continue
            if isinstance(raw, datetime):
                out.add(raw.date())
            elif isinstance(raw, date):
                out.add(raw)
    except (ProgrammingError, OperationalError) as e:
        logger.debug("market_holiday: holiday table unavailable: %s", e)
    except Exception as e:
        logger.warning("market_holiday: failed to read holiday table: %s", e)
    finally:
        if db is not None:
            db.close()

    with _cache_lock:
        _cached_holiday_dates = out
        _cache_mono = time.monotonic()
    return set(out)


def _holiday_dates_cached() -> Set[date]:
    with _cache_lock:
        age = time.monotonic() - _cache_mono
        if _cache_mono > 0.0 and age < CACHE_TTL_SEC:
            return set(_cached_holiday_dates)
    return refresh_holiday_dates_from_db()


def is_nse_holiday_ist(now: Optional[datetime] = None) -> bool:
    """True if *IST calendar date* is listed in ``holiday`` (ignores weekends)."""
    d = _normalize_ist(now).date()
    return d in _holiday_dates_cached()


def should_skip_scheduled_market_jobs_ist(now: Optional[datetime] = None) -> bool:
    """
    True on Saturday/Sunday or on a date present in ``holiday`` — skip market data fetch schedulers.

    Uses IST for the calendar date so it matches NSE session dates.
    """
    now_ist = _normalize_ist(now)
    if now_ist.weekday() >= 5:
        return True
    return now_ist.date() in _holiday_dates_cached()

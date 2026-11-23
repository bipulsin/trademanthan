"""Injectable IST clock for OLH dress rehearsal / fault injection.

Production code paths that need a mockable clock should call ``olh_now()``
instead of ``datetime.now(IST)`` when a hook is available. Default is wall clock.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Callable, Optional

import pytz

IST = pytz.timezone("Asia/Kolkata")

_NowFn = Callable[[], datetime]
_now_override: Optional[_NowFn] = None


def olh_now() -> datetime:
    """Current IST datetime (overrideable for rehearsal)."""
    if _now_override is not None:
        t = _now_override()
        if t.tzinfo is None:
            return IST.localize(t)
        return t.astimezone(IST)
    return datetime.now(IST)


def set_olh_clock(fn: Optional[_NowFn]) -> None:
    """Install or clear a clock override. Pass ``None`` to restore wall clock."""
    global _now_override
    _now_override = fn


def fixed_clock(when: datetime) -> _NowFn:
    """Return a clock frozen at ``when`` (localized to IST if naive)."""
    if when.tzinfo is None:
        when = IST.localize(when)
    else:
        when = when.astimezone(IST)

    def _fn() -> datetime:
        return when

    return _fn


class AdvancingClock:
    """Clock that starts at ``start`` and advances on ``tick`` / ``advance``."""

    def __init__(self, start: datetime) -> None:
        if start.tzinfo is None:
            start = IST.localize(start)
        else:
            start = start.astimezone(IST)
        self._t = start

    def __call__(self) -> datetime:
        return self._t

    def advance(self, *, seconds: float = 0, minutes: float = 0) -> datetime:
        self._t = self._t + timedelta(seconds=seconds, minutes=minutes)
        return self._t

    def set(self, when: datetime) -> datetime:
        if when.tzinfo is None:
            when = IST.localize(when)
        else:
            when = when.astimezone(IST)
        self._t = when
        return self._t


def install_advancing_clock(start: datetime) -> AdvancingClock:
    clock = AdvancingClock(start)
    set_olh_clock(clock)
    return clock

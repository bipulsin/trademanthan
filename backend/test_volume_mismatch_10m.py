"""Unit tests for VM first-10m aggregation from 5m bars."""
from datetime import date

from backend.services.volume_mismatch.candles import (
    first_10m_bar_from_5m,
    first_10m_volumes_by_session,
)


def _bar(ts: str, o: float, h: float, l: float, c: float, v: float):
    return {"timestamp": ts, "open": o, "high": h, "low": l, "close": c, "volume": v}


def test_first_10m_requires_both_5m_legs():
    d = date(2026, 7, 31)
    only_one = [_bar("2026-07-31T09:15:00+05:30", 100, 101, 99, 100.5, 1000)]
    assert first_10m_bar_from_5m(only_one, d) is None


def test_first_10m_aggregates_0915_and_0920():
    d = date(2026, 7, 31)
    bars = [
        _bar("2026-07-31T09:15:00+05:30", 100, 102, 99, 101, 1000),
        _bar("2026-07-31T09:20:00+05:30", 101, 103, 100.5, 102.5, 1500),
    ]
    out = first_10m_bar_from_5m(bars, d)
    assert out is not None
    assert out["open"] == 100
    assert out["high"] == 103
    assert out["low"] == 99
    assert out["close"] == 102.5
    assert out["volume"] == 2500


def test_first_10m_volumes_by_session():
    bars = [
        _bar("2026-07-30T09:15:00+05:30", 1, 1, 1, 1, 100),
        _bar("2026-07-30T09:20:00+05:30", 1, 1, 1, 1, 200),
        _bar("2026-07-31T09:15:00+05:30", 1, 1, 1, 1, 50),
        _bar("2026-07-31T09:20:00+05:30", 1, 1, 1, 1, 75),
    ]
    hist = first_10m_volumes_by_session(bars, before_date=date(2026, 7, 31), max_sessions=5)
    assert hist == [(date(2026, 7, 30), 300.0)]

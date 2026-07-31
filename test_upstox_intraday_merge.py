"""Unit tests for merging Upstox intraday candles into historical series."""
from datetime import date

from backend.services.upstox_service import _merge_historical_with_intraday


def test_merge_drops_today_historical_and_appends_intraday():
    session = date(2026, 5, 20)
    historical = [
        {"timestamp": "2026-05-19T15:00:00+05:30", "close": 100.0},
        {"timestamp": "2026-05-20T09:15:00+05:30", "close": 99.0},
    ]
    intraday = [
        {"timestamp": "2026-05-20T09:15:00+05:30", "close": 101.0},
        {"timestamp": "2026-05-20T09:45:00+05:30", "close": 102.0},
    ]
    out = _merge_historical_with_intraday(historical, intraday, session_date=session)
    assert len(out) == 3
    assert out[0]["timestamp"].startswith("2026-05-19")
    assert out[-1]["close"] == 102.0
    # Same-day hist bar overwritten by intraday on conflict.
    assert out[1]["close"] == 101.0


def test_merge_intraday_only_when_no_historical():
    session = date(2026, 5, 20)
    intraday = [{"timestamp": "2026-05-20T10:00:00+05:30", "close": 50.0}]
    out = _merge_historical_with_intraday(None, intraday, session_date=session)
    assert len(out) == 1
    assert out[0]["close"] == 50.0


def test_merge_keeps_hist_today_when_intraday_empty():
    session = date(2026, 5, 20)
    historical = [
        {"timestamp": "2026-05-19T15:00:00+05:30", "close": 100.0},
        {"timestamp": "2026-05-20T10:50:00+05:30", "close": 110.0},
    ]
    out = _merge_historical_with_intraday(historical, None, session_date=session)
    assert len(out) == 2
    assert out[-1]["close"] == 110.0


def test_merge_keeps_afternoon_hist_when_intraday_truncated():
    """Partial morning intraday must not wipe afternoon historical tip."""
    session = date(2026, 7, 31)
    historical = [
        {"timestamp": "2026-07-30T15:00:00+05:30", "close": 600.0},
        {"timestamp": "2026-07-31T09:35:00+05:30", "close": 636.8},
        {"timestamp": "2026-07-31T10:45:00+05:30", "close": 625.1},
    ]
    intraday = [
        {"timestamp": "2026-07-31T09:15:00+05:30", "close": 630.0},
        {"timestamp": "2026-07-31T09:35:00+05:30", "close": 636.8},
    ]
    out = _merge_historical_with_intraday(historical, intraday, session_date=session)
    assert out[-1]["timestamp"].startswith("2026-07-31T10:45")
    assert out[-1]["close"] == 625.1

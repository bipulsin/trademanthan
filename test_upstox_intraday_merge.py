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


def test_merge_intraday_only_when_no_historical():
    session = date(2026, 5, 20)
    intraday = [{"timestamp": "2026-05-20T10:00:00+05:30", "close": 50.0}]
    out = _merge_historical_with_intraday(None, intraday, session_date=session)
    assert len(out) == 1
    assert out[0]["close"] == 50.0

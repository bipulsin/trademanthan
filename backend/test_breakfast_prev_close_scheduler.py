"""Tests for breakfast prev-close prefill helpers."""
from __future__ import annotations

from datetime import date, datetime

import pytz

from backend.services.breakfast_prev_close import (
    latest_settled_daily_close,
    latest_settled_daily_ohlc,
    merge_settled_today_from_intrabars,
    parse_daily_bars,
    session_ohlc_from_intrabars,
)

IST = pytz.timezone("Asia/Kolkata")


def test_parse_daily_bars_sorts_and_filters():
    candles = [
        {"timestamp": "2026-08-28T00:00:00+05:30", "close": 100.0},
        {"timestamp": "2026-08-29T00:00:00+05:30", "close": 0},
        {"timestamp": "2026-08-27T00:00:00+05:30", "close": 99.0},
    ]
    bars = parse_daily_bars(candles)
    assert bars == [(date(2026, 8, 27), 99.0), (date(2026, 8, 28), 100.0)]


def test_latest_settled_daily_close_before_close_uses_prior_day():
    now = IST.localize(datetime(2026, 8, 31, 10, 0))
    candles = [
        {"timestamp": "2026-08-28T00:00:00+05:30", "close": 100.0},
        {"timestamp": "2026-08-31T00:00:00+05:30", "close": 105.0},
    ]
    d, px = latest_settled_daily_close(candles, now_ist=now)
    assert d == date(2026, 8, 28)
    assert px == 100.0


def test_latest_settled_daily_close_after_close_uses_today():
    now = IST.localize(datetime(2026, 8, 31, 16, 0))
    candles = [
        {"timestamp": "2026-08-28T00:00:00+05:30", "close": 100.0},
        {"timestamp": "2026-08-31T00:00:00+05:30", "close": 105.0},
    ]
    d, px = latest_settled_daily_close(candles, now_ist=now)
    assert d == date(2026, 8, 31)
    assert px == 105.0


def test_latest_settled_falls_back_when_days_feed_lacks_today():
    now = IST.localize(datetime(2026, 9, 2, 16, 30))
    candles = [
        {"timestamp": "2026-09-01T00:00:00+05:30", "open": 100, "high": 101, "low": 99, "close": 100.0},
    ]
    d, px = latest_settled_daily_close(candles, now_ist=now)
    assert d == date(2026, 9, 1)
    assert px == 100.0


def test_merge_settled_today_from_hourly_when_days_lag():
    now = IST.localize(datetime(2026, 9, 2, 16, 30))
    daily = [
        {"timestamp": "2026-09-01T00:00:00+05:30", "open": 100, "high": 101, "low": 99, "close": 100.0},
    ]
    hours = [
        {"timestamp": "2026-09-02T09:15:00+05:30", "open": 710, "high": 712, "low": 709, "close": 711},
        {"timestamp": "2026-09-02T15:15:00+05:30", "open": 715, "high": 718, "low": 714, "close": 716.5},
    ]
    merged = merge_settled_today_from_intrabars(daily, hours, now_ist=now)
    row = latest_settled_daily_ohlc(merged, now_ist=now)
    assert row is not None
    assert row[0] == date(2026, 9, 2)
    assert row[1] == 710
    assert row[2] == 718
    assert row[3] == 709
    assert row[4] == 716.5


def test_session_ohlc_ignores_other_days():
    hours = [
        {"timestamp": "2026-09-01T15:15:00+05:30", "open": 1, "high": 2, "low": 1, "close": 1.5},
        {"timestamp": "2026-09-02T09:15:00+05:30", "open": 10, "high": 12, "low": 9, "close": 11},
    ]
    assert session_ohlc_from_intrabars(hours, date(2026, 9, 2)) == (date(2026, 9, 2), 10.0, 12.0, 9.0, 11.0)


def test_merge_does_not_invent_today_before_close():
    now = IST.localize(datetime(2026, 9, 2, 10, 0))
    daily = [
        {"timestamp": "2026-09-01T00:00:00+05:30", "open": 100, "high": 101, "low": 99, "close": 100.0},
    ]
    hours = [
        {"timestamp": "2026-09-02T09:15:00+05:30", "open": 710, "high": 712, "low": 709, "close": 711},
    ]
    merged = merge_settled_today_from_intrabars(daily, hours, now_ist=now)
    d, px = latest_settled_daily_close(merged, now_ist=now)
    assert d == date(2026, 9, 1)
    assert px == 100.0

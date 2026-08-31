"""Tests for breakfast prev-close prefill helpers."""
from __future__ import annotations

from datetime import date, datetime

import pytz

from backend.services.breakfast_prev_close import (
    latest_settled_daily_close,
    parse_daily_bars,
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

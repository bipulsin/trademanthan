"""Tests for daily checklist morning snapshot lock."""
from datetime import datetime

import pytz

from backend.services.daily_checklist_snapshot import (
    LOCK_MINUTES_IST,
    at_or_after_lock_time,
    sort_by_snapshot_rank,
)

IST = pytz.timezone("Asia/Kolkata")


def test_at_or_after_lock_time():
    before = IST.localize(datetime(2026, 7, 3, 9, 24, 0))
    at = IST.localize(datetime(2026, 7, 3, 9, 25, 0))
    after = IST.localize(datetime(2026, 7, 3, 10, 0, 0))
    assert not at_or_after_lock_time(before)
    assert at_or_after_lock_time(at)
    assert at_or_after_lock_time(after)
    assert LOCK_MINUTES_IST == 9 * 60 + 25


def test_sort_by_snapshot_rank():
    stocks = [
        {"symbol": "C", "direction": "LONG"},
        {"symbol": "A", "direction": "LONG"},
        {"symbol": "Z", "direction": "SHORT"},
    ]
    rank_map = {"A": (0, 1), "C": (0, 2), "Z": (1, 1)}
    ordered = sort_by_snapshot_rank(stocks, rank_map)
    assert [s["symbol"] for s in ordered] == ["A", "C", "Z"]

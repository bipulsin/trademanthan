"""Unit tests for Smart Futures signal gate helpers."""
from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from backend.services.smart_futures_signal_gates import (
    apply_live_signal_invalidation,
    cap_early_session_volume_surge,
    gate_price_from_session_m5,
    sentiment_blocks_side,
    vwap_side_confirmed,
)

IST = ZoneInfo("Asia/Kolkata")


def test_gate_price_from_last_m5_close():
    bars = [
        {"close": 430.0},
        {"close": 431.7},
    ]
    assert gate_price_from_session_m5(bars) == 431.7


def test_cap_early_session_volume_surge():
    bar_end = datetime(2026, 6, 23, 9, 50, tzinfo=IST)
    assert cap_early_session_volume_surge(bar_end, 64.0) == 8.0
    late = datetime(2026, 6, 23, 11, 0, tzinfo=IST)
    assert cap_early_session_volume_surge(late, 64.0) == 64.0


def test_vwap_two_bar_long_requires_consecutive_closes():
    vwap = 431.0
    ok = [
        {"close": 432.0},
        {"close": 431.5},
    ]
    bad = [
        {"close": 432.0},
        {"close": 430.5},
    ]
    assert vwap_side_confirmed("LONG", ok, vwap, n_bars=2) is True
    assert vwap_side_confirmed("LONG", bad, vwap, n_bars=2) is False


def test_sentiment_blocks_long():
    assert sentiment_blocks_side("LONG", -0.604) is not None
    assert sentiment_blocks_side("LONG", -0.2) is None


def test_live_invalidation_bos_long():
    now = datetime(2026, 6, 23, 10, 25, tzinfo=IST)
    row = {
        "side": "LONG",
        "order_status": None,
        "entry_at": datetime(2026, 6, 23, 10, 15, tzinfo=IST),
        "current_ltp": 429.0,
        "scan_bar_low": 430.5,
        "scan_bar_high": 432.9,
        "m15_vwap_at_scan": 431.08,
        "entry_gate_permitted": False,
    }
    m5 = [{"timestamp": "2026-06-23T10:15:00+05:30", "high": 432.9, "low": 430.5, "close": 431.7}]
    apply_live_signal_invalidation(row, m5, now)
    assert row["signal_status"] == "INVALIDATED"

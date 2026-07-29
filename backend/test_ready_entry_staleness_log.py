"""Unit tests for READY entry staleness shadow helpers (no DB)."""
from datetime import datetime

import pytz

from backend.services.kavach_ready_entry_staleness_log import (
    EVENT_INITIAL,
    EVENT_RECHECK,
    _attempt_from_since,
    build_staleness_row,
)

IST = pytz.timezone("Asia/Kolkata")


def test_attempt_increments_across_10m_slots():
    since = IST.localize(datetime(2026, 7, 29, 10, 0, 0))
    now = IST.localize(datetime(2026, 7, 29, 10, 25, 0))
    # slots from 09:15: 10:00 → idx 4, 10:25 → idx 7 → attempt 1+(7-4)=4
    assert _attempt_from_since(since, now) == 4
    assert _attempt_from_since(since, since) == 1


def test_build_initial_when_no_prev():
    now = IST.localize(datetime(2026, 7, 29, 11, 0, 0))
    row = build_staleness_row(
        session_date="2026-07-29",
        stock={
            "symbol": "KAYNES",
            "direction": "LONG",
            "trade_state": "READY",
            "trade_entry": 3245.71,
            "live_candle_ema5": 3245.70,
            "live_candle_ema10": 3200.0,
            "live_candle_price": 3431.60,
            "confidence": "A",
            "trade_score": 85,
            "trade_take_enabled": True,
            "card_visible": True,
            "ready_visible_since": now.isoformat(),
        },
        now=now,
        prev=None,
    )
    assert row is not None
    assert row["event_type"] == EVENT_INITIAL
    assert row["entry_matches_ema5"] is True
    assert abs(row["gap_pct"] - 5.727) < 0.05  # ~5.7%


def test_sticky_computed_ts_when_entry_off_ema5():
    t0 = IST.localize(datetime(2026, 7, 29, 10, 0, 0))
    t1 = IST.localize(datetime(2026, 7, 29, 10, 15, 0))
    prev = {
        "entry_price": 3245.71,
        "entry_price_last_computed_ts": t0,
        "event_type": EVENT_INITIAL,
        "logged_at": t0,
        "rendered_state": "READY",
        "attempt_number": 1,
        "gap_pct": 1.0,
    }
    row = build_staleness_row(
        session_date="2026-07-29",
        stock={
            "symbol": "KAYNES",
            "trade_state": "READY",
            "trade_entry": 3245.71,
            "live_candle_ema5": 3300.0,  # entry no longer matches
            "live_candle_price": 3431.60,
            "ready_visible_since": t0.isoformat(),
            "card_visible": True,
        },
        now=t1,
        prev=prev,
    )
    assert row is not None
    assert row["event_type"] == EVENT_RECHECK
    assert row["entry_matches_ema5"] is False
    assert row["entry_price_last_computed_ts"] == t0
    assert row["attempt_number"] >= 2


def test_skips_non_ready_non_soft():
    row = build_staleness_row(
        session_date="2026-07-29",
        stock={"symbol": "X", "trade_state": "WAIT FOR PULLBACK", "card_visible": False},
        now=IST.localize(datetime(2026, 7, 29, 11, 0, 0)),
    )
    assert row is None

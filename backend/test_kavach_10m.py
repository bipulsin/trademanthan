"""Unit tests for 10m Kavach aggregation and edge-flip detection."""
from datetime import datetime, timedelta

import pytz

from backend.services.kavach_10m import aggregate_10m_bars, last_closed_10m_pair_end_idx, metrics_from_10m_candles
from backend.services.rs_fast_watch import is_edge_flip, kavach_direction, _is_reversal

IST = pytz.timezone("Asia/Kolkata")


def _bar(day: str, hm: str, close: float, vol: float = 1000.0) -> dict:
    h, m = map(int, hm.split(":"))
    ts = IST.localize(datetime.strptime(day, "%Y-%m-%d").replace(hour=h, minute=m))
    return {
        "timestamp": ts.isoformat(),
        "open": close,
        "high": close + 1,
        "low": close - 1,
        "close": close,
        "volume": vol,
    }


def test_aggregate_10m_pairs_same_day():
    day = "2026-07-08"
    candles = [_bar(day, "09:15", 100), _bar(day, "09:20", 101), _bar(day, "09:25", 102)]
    bars = aggregate_10m_bars(candles)
    assert len(bars) == 1
    assert bars[0]["close"] == 101
    assert bars[0]["volume"] == 2000
    assert bars[0]["end_5m_idx"] == 1


def test_aggregate_10m_resets_pairing_at_session_boundary():
    """Odd prior-day bar count must not drop today's 09:15 (global-index bug)."""
    prev = "2026-07-15"
    day = "2026-07-16"
    # 75 bars/day is odd; one complete prior day → first_today_idx odd under
    # the old global stepper. Three 5m bars are enough to prove the reset.
    prior = [_bar(prev, "15:15", 90), _bar(prev, "15:20", 91), _bar(prev, "15:25", 92)]
    today = [
        _bar(day, "09:15", 100, vol=5000),
        _bar(day, "09:20", 101, vol=1000),
        _bar(day, "09:25", 102, vol=1000),
        _bar(day, "09:30", 103, vol=1000),
    ]
    candles = prior + today
    assert len(prior) % 2 == 1  # odd prefix → old bug would skip 09:15

    bars = aggregate_10m_bars(candles)
    today_bars = [b for b in bars if str(b["timestamp"]).startswith(day)]
    assert len(today_bars) == 2
    # First session 10m = 09:15 + 09:20 (not 09:20 + 09:25)
    assert today_bars[0]["close"] == 101
    assert today_bars[0]["volume"] == 6000
    assert today_bars[0]["end_5m_idx"] == len(prior) + 1  # global idx of 09:20
    assert today_bars[1]["close"] == 103
    assert today_bars[1]["end_5m_idx"] == len(prior) + 3


def test_edge_flip_requires_transition():
    assert is_edge_flip(None, "BUY") is False
    assert is_edge_flip("SELL", "BUY") is True
    assert is_edge_flip("BUY", "BUY") is False


def test_reversal_detection():
    assert _is_reversal("BUY", "SHORT") is True
    assert _is_reversal("SELL", "LONG") is True
    assert _is_reversal("SELL", "SHORT") is False


def test_kavach_direction():
    assert kavach_direction("BUY") == "LONG"
    assert kavach_direction("READY SHORT") == "SHORT"
    assert kavach_direction("WATCH") is None

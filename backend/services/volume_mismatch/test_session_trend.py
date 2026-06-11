"""Unit tests for VM session trend flip logic."""
from backend.services.volume_mismatch.session_trend import (
    assess_session_trend,
    flipped_direction,
)


def test_bearish_lower_low_below_vwap_ema():
    trend = assess_session_trend(
        price=98.0,
        vwap=100.0,
        ema5=99.5,
        cur_5m={"low": 97.5, "high": 99.0},
        prev_5m={"low": 98.0, "high": 100.0},
    )
    assert trend == "BEARISH"


def test_bullish_higher_high_above_vwap_ema():
    trend = assess_session_trend(
        price=102.0,
        vwap=100.0,
        ema5=101.0,
        cur_5m={"low": 101.0, "high": 103.0},
        prev_5m={"low": 100.0, "high": 102.0},
    )
    assert trend == "BULLISH"


def test_no_trend_without_structure():
    trend = assess_session_trend(
        price=98.0,
        vwap=100.0,
        ema5=99.5,
        cur_5m={"low": 98.5, "high": 99.0},
        prev_5m={"low": 98.0, "high": 100.0},
    )
    assert trend is None


def test_flip_long_to_short_on_bearish():
    assert flipped_direction("LONG", "BEARISH") == "SHORT"
    assert flipped_direction("SHORT", "BULLISH") == "LONG"
    assert flipped_direction("LONG", "BULLISH") is None
    assert flipped_direction("SHORT", "BEARISH") is None

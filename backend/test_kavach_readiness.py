"""Tests for Kavach v3.0 readiness + nearer-EMA10/VWAP pullback counts."""
from backend.services.kavach_readiness import (
    classify_kavach_readiness,
    count_nearer_pullbacks,
    vol_decel_3_from_10m,
)


def test_grade_ready_excludes_stretch_bang():
    r = classify_kavach_readiness(
        confidence_display="B!",
        trade_score=80,
        panel_trend="Bullish",
        kavach_state="BUY",
        pct_from_open=1.0,
        pullback_long=1,
        pullback_short=0,
        volume_ratio_for_enter=1.2,
        vol_decel_3=False,
    )
    assert r["readiness"] == "NOT READY"


def test_watching_when_stretched_from_open():
    r = classify_kavach_readiness(
        confidence_display="B",
        trade_score=70,
        panel_trend="Bullish",
        kavach_state="BUY",
        pct_from_open=4.5,  # over 3% max → cannot be READY
        pullback_long=0,
        pullback_short=0,
        volume_ratio_for_enter=1.2,
        vol_decel_3=False,
    )
    assert r["readiness"] == "WATCHING"
    assert r["ready_long_practical"] is False


def test_ready_to_long_allows_zero_pullbacks():
    r = classify_kavach_readiness(
        confidence_display="A",
        trade_score=88,
        panel_trend="Bullish",
        kavach_state="BUY",
        pct_from_open=1.2,
        pullback_long=0,
        pullback_short=0,
        volume_ratio_for_enter=1.0,
        vol_decel_3=False,
    )
    assert r["readiness"] == "READY TO LONG"


def test_zero_pullbacks_in_range():
    r = classify_kavach_readiness(
        confidence_display="B",
        trade_score=66,
        panel_trend="Bullish",
        kavach_state="READY",
        pct_from_open=0.5,
        pullback_long=0,
        pullback_short=5,
        volume_ratio_for_enter=0.9,
        vol_decel_3=False,
    )
    assert r["pull_long_ok"] is True
    assert r["readiness"] == "READY TO LONG"


def test_vol_decel_blocks_ready():
    r = classify_kavach_readiness(
        confidence_display="A",
        trade_score=90,
        panel_trend="Bullish",
        kavach_state="BUY",
        pct_from_open=1.0,
        pullback_long=1,
        pullback_short=0,
        volume_ratio_for_enter=1.5,
        vol_decel_3=True,
    )
    assert r["readiness"] == "WATCHING"


def test_vol_decel_helper():
    assert vol_decel_3_from_10m([10, 9, 8, 7]) is True
    assert vol_decel_3_from_10m([10, 9, 9, 7]) is False
    assert vol_decel_3_from_10m([1, 2, 3]) is False


def test_pullback_empty_candles():
    assert count_nearer_pullbacks([], session_date="2026-07-20") == (0, 0)

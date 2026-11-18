"""Unit tests for Open Low High screener rules — no network."""
from __future__ import annotations

from backend.services.open_low_high.signals import (
    confirm_phase_b,
    evaluate_phase_a,
    freeze_tags_immutable,
    next_pivot_targets,
    passes_second_candle_wick_filter,
    traditional_pivot_levels,
)


def test_phase_a_bullish_close_above_pdh_vwap_green():
    r = evaluate_phase_a(
        first_open=100.0,
        first_high=103.0,
        first_low=99.5,
        first_close=102.5,
        vwap_0915=101.0,
        prev_high=101.0,
        prev_low=95.0,
        prev_close=100.5,
    )
    assert r is not None
    assert r.side == "BULLISH"
    assert r.first_close == 102.5


def test_phase_a_bullish_rejects_red_candle():
    r = evaluate_phase_a(
        first_open=103.0,
        first_high=104.0,
        first_low=101.0,
        first_close=102.5,  # above PDH + VWAP but red
        vwap_0915=101.0,
        prev_high=101.0,
        prev_low=95.0,
    )
    assert r is None


def test_phase_a_bullish_accepts_doji():
    r = evaluate_phase_a(
        first_open=102.5,
        first_high=103.0,
        first_low=101.5,
        first_close=102.5,  # doji above PDH + VWAP
        vwap_0915=101.0,
        prev_high=101.0,
        prev_low=95.0,
    )
    assert r is not None
    assert r.side == "BULLISH"


def test_phase_a_bullish_rejects_close_not_above_pdh():
    # Wick above PDH but close at/below — live screener uses close, not wick.
    r = evaluate_phase_a(
        first_open=99.0,
        first_high=101.5,
        first_low=98.5,
        first_close=100.5,
        vwap_0915=99.0,
        prev_high=101.0,
        prev_low=95.0,
    )
    assert r is None


def test_phase_a_bearish_close_below_pdl_vwap_red():
    r = evaluate_phase_a(
        first_open=100.0,
        first_high=100.5,
        first_low=96.0,
        first_close=97.0,
        vwap_0915=98.5,
        prev_high=105.0,
        prev_low=98.0,
        prev_close=99.0,
    )
    assert r is not None
    assert r.side == "BEARISH"


def test_phase_a_bearish_accepts_doji():
    # INDIGO-style: close == open below PDL and below VWAP.
    r = evaluate_phase_a(
        first_open=5600.0,
        first_high=5614.2,
        first_low=5587.7,
        first_close=5600.0,
        vwap_0915=5600.633,
        prev_high=5672.2,
        prev_low=5608.7,
    )
    assert r is not None
    assert r.side == "BEARISH"


def test_phase_a_bearish_rejects_green():
    r = evaluate_phase_a(
        first_open=96.0,
        first_high=98.0,
        first_low=95.0,
        first_close=97.0,  # below PDL + VWAP but green
        vwap_0915=98.5,
        prev_high=105.0,
        prev_low=98.0,
    )
    assert r is None


def test_phase_b_bullish_keep_when_second_high_above_first_high():
    assert (
        confirm_phase_b(
            "BULLISH",
            first_high=102.0,
            first_low=99.0,
            second_high=102.5,
            second_low=100.0,
        )
        is True
    )
    assert (
        confirm_phase_b(
            "BULLISH",
            first_high=102.0,
            first_low=99.0,
            second_high=102.0,
            second_low=100.0,
        )
        is False
    )
    # Close above first high is not enough if high does not exceed.
    assert (
        confirm_phase_b(
            "BULLISH",
            first_high=102.0,
            first_low=99.0,
            second_high=101.5,
            second_low=100.0,
        )
        is False
    )


def test_phase_b_bearish_keep_when_second_low_below_first_low():
    assert (
        confirm_phase_b(
            "BEARISH",
            first_high=100.0,
            first_low=97.0,
            second_high=99.0,
            second_low=96.5,
        )
        is True
    )
    assert (
        confirm_phase_b(
            "BEARISH",
            first_high=100.0,
            first_low=97.0,
            second_high=99.0,
            second_low=97.0,
        )
        is False
    )
    # Close below first low without a lower low fails (old close-based rule).
    assert (
        confirm_phase_b(
            "BEARISH",
            first_high=100.0,
            first_low=97.0,
            second_high=99.0,
            second_low=97.2,
        )
        is False
    )


def test_wick_filter_bullish_upper_wick_ratio():
    # body=10, upper wick=3.5 → 0.35 pass; upper wick=4 → fail
    assert (
        passes_second_candle_wick_filter(
            "BULLISH",
            second_open=100.0,
            second_high=113.5,
            second_low=99.0,
            second_close=110.0,
        )
        is True
    )
    assert (
        passes_second_candle_wick_filter(
            "BULLISH",
            second_open=100.0,
            second_high=114.0,
            second_low=99.0,
            second_close=110.0,
        )
        is False
    )


def test_wick_filter_bearish_lower_wick_ratio():
    # body=17, lower wick=4.4 → ~0.26 (INDIGO REST-like) pass at 0.35; wick=6 → fail
    assert (
        passes_second_candle_wick_filter(
            "BEARISH",
            second_open=5606.9,
            second_high=5606.9,
            second_low=5585.5,
            second_close=5589.9,
        )
        is True
    )
    assert (
        passes_second_candle_wick_filter(
            "BEARISH",
            second_open=5606.9,
            second_high=5606.9,
            second_low=5583.9,
            second_close=5589.9,
        )
        is False
    )


def test_wick_filter_rejects_zero_body():
    assert (
        passes_second_candle_wick_filter(
            "BEARISH",
            second_open=100.0,
            second_high=101.0,
            second_low=99.0,
            second_close=100.0,
        )
        is False
    )
    assert (
        passes_second_candle_wick_filter(
            "BULLISH",
            second_open=100.0,
            second_high=101.0,
            second_low=99.0,
            second_close=100.0,
        )
        is False
    )


def test_freeze_tags_immutable_snapshots_display():
    tags = freeze_tags_immutable(
        {"sector_badge": "S1", "sector_star": True, "nifty_sector_label": "Nifty IT"},
        frozen=True,
    )
    assert tags["frozen"] is True
    assert tags["sector_badge"] == "S1"
    assert tags["sector_badge_display"] == "S1*"
    # Mutating a fresh live dict must not affect the frozen snapshot copy.
    live = {"sector_badge": "W2", "sector_star": False}
    frozen = freeze_tags_immutable(live, frozen=True)
    live["sector_badge"] = "S3"
    assert frozen["sector_badge"] == "W2"
    assert frozen["sector_badge_display"] == "W2"


def test_traditional_pivot_levels_basic():
    # H=110, L=100, C=105 → PP=105; R1=110; S1=100
    lv = traditional_pivot_levels(110.0, 100.0, 105.0)
    assert abs(lv["pp"] - 105.0) < 1e-9
    assert abs(lv["r1"] - 110.0) < 1e-9
    assert abs(lv["s1"] - 100.0) < 1e-9
    assert abs(lv["r2"] - 115.0) < 1e-9
    assert abs(lv["s2"] - 95.0) < 1e-9


def test_next_pivot_targets_bullish_and_bearish():
    # reference between S1 and PP → bullish next = PP, R1; bearish next = S1, S2
    t1, t2 = next_pivot_targets(
        "BULLISH",
        prev_high=110.0,
        prev_low=100.0,
        prev_close=105.0,
        reference=102.0,
    )
    assert t1 == 105.0
    assert t2 == 110.0
    t1b, t2b = next_pivot_targets(
        "BEARISH",
        prev_high=110.0,
        prev_low=100.0,
        prev_close=105.0,
        reference=102.0,
    )
    assert t1b == 100.0
    assert t2b == 95.0


def test_window_status_boundaries():
    from datetime import datetime

    import pytz

    from backend.services.open_low_high.screener import window_status

    IST = pytz.timezone("Asia/Kolkata")
    assert window_status(IST.localize(datetime(2026, 11, 17, 9, 20, 30))) == "waiting"
    assert window_status(IST.localize(datetime(2026, 11, 17, 9, 25, 0))) == "waiting"
    assert window_status(IST.localize(datetime(2026, 11, 17, 9, 25, 5))) == "open"
    assert window_status(IST.localize(datetime(2026, 11, 17, 9, 26, 0))) == "open"
    assert window_status(IST.localize(datetime(2026, 11, 17, 9, 27, 0))) == "open"
    assert window_status(IST.localize(datetime(2026, 11, 17, 9, 27, 1))) == "past"

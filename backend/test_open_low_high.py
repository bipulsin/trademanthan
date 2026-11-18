"""Unit tests for Open Low High screener rules — no network."""
from __future__ import annotations

from backend.services.open_low_high.signals import (
    confirm_phase_b,
    evaluate_nifty_cue,
    evaluate_phase_a,
    evaluate_phase_b,
    freeze_tags_immutable,
    next_pivot_targets,
    open_extreme_badge,
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


def test_phase_b_bullish_keep_when_second_close_ge_first_close():
    assert (
        confirm_phase_b(
            "BULLISH",
            first_close=102.0,
            second_close=102.5,
        )
        is True
    )
    assert (
        confirm_phase_b(
            "BULLISH",
            first_close=102.0,
            second_close=102.0,
        )
        is True
    )
    assert (
        confirm_phase_b(
            "BULLISH",
            first_close=102.0,
            second_close=101.5,
        )
        is False
    )


def test_phase_b_bearish_keep_when_second_close_le_first_close():
    assert (
        confirm_phase_b(
            "BEARISH",
            first_close=97.0,
            second_close=96.5,
        )
        is True
    )
    assert (
        confirm_phase_b(
            "BEARISH",
            first_close=97.0,
            second_close=97.0,
        )
        is True
    )
    assert (
        confirm_phase_b(
            "BEARISH",
            first_close=97.0,
            second_close=97.2,
        )
        is False
    )


def test_evaluate_phase_b_fail_reasons():
    missing = evaluate_phase_b(
        "BULLISH",
        first_close=100.0,
        second_open=None,
        second_high=None,
        second_low=None,
        second_close=None,
    )
    assert missing.ok is False
    assert missing.fail_reason == "missing 9:20 candle"

    # Red 9:20 + close below 9:15 → reverse-direction wording
    reverse = evaluate_phase_b(
        "BULLISH",
        first_close=100.0,
        second_open=100.0,
        second_high=101.0,
        second_low=99.0,
        second_close=99.5,
    )
    assert reverse.ok is False
    assert reverse.fail_reason == "9:20 candle is in reverse direction of 9:15 candle"

    # Green-ish 9:20 but still closes below 9:15
    close_fail = evaluate_phase_b(
        "BULLISH",
        first_close=100.0,
        second_open=99.0,
        second_high=100.5,
        second_low=98.5,
        second_close=99.5,
    )
    assert close_fail.ok is False
    assert close_fail.fail_reason == "9:20 candle closes below 9:15 candle"

    bear_close = evaluate_phase_b(
        "BEARISH",
        first_close=100.0,
        second_open=100.5,
        second_high=101.0,
        second_low=100.0,
        second_close=100.2,
    )
    assert bear_close.ok is False
    assert bear_close.fail_reason == "9:20 candle closes above 9:15 candle"

    wick_fail = evaluate_phase_b(
        "BULLISH",
        first_close=100.0,
        second_open=100.0,
        second_high=114.0,
        second_low=99.0,
        second_close=110.0,
    )
    assert wick_fail.ok is False
    assert wick_fail.fail_reason == (
        "9:20 candle has wick longer than 35% of candle body (40%)"
    )
    assert wick_fail.wick_body_ratio == 0.4

    ok = evaluate_phase_b(
        "BULLISH",
        first_close=100.0,
        second_open=100.0,
        second_high=113.5,
        second_low=99.0,
        second_close=110.0,
    )
    assert ok.ok is True
    assert ok.fail_reason is None
    assert ok.wick_body_ratio == 0.35


def test_open_extreme_badge_ol_oh():
    assert (
        open_extreme_badge("BULLISH", first_open=100.0, first_high=105.0, first_low=100.0)
        == "O=L"
    )
    assert (
        open_extreme_badge("BULLISH", first_open=100.0, first_high=105.0, first_low=99.0)
        is None
    )
    assert (
        open_extreme_badge("BEARISH", first_open=105.0, first_high=105.0, first_low=100.0)
        == "O=H"
    )
    assert (
        open_extreme_badge("BEARISH", first_open=104.0, first_high=105.0, first_low=100.0)
        is None
    )


def test_nifty_cue_gap_and_bias():
    # Bias = session close vs PDC (prefer 09:20); gap = 09:15 open vs PDC
    bull = evaluate_nifty_cue(
        prev_close=100.0,
        first_open=101.0,
        first_close=101.5,
        second_close=102.0,
    )
    assert bull["nifty_gap"] == "up"
    assert bull["nifty_bias"] == "bullish"
    assert bull["nifty_show_on"] == "BULLISH"

    bear = evaluate_nifty_cue(
        prev_close=100.0,
        first_open=99.0,
        first_close=99.5,
        second_close=98.5,
    )
    assert bear["nifty_gap"] == "down"
    assert bear["nifty_bias"] == "bearish"
    assert bear["nifty_show_on"] == "BEARISH"

    # Gap up but 09:20 close below PDC → bearish bias, no header badge
    mixed = evaluate_nifty_cue(
        prev_close=100.0,
        first_open=101.0,
        first_close=102.0,
        second_close=99.0,
    )
    assert mixed["nifty_gap"] == "up"
    assert mixed["nifty_bias"] == "bearish"
    assert mixed["nifty_show_on"] is None

    # Before 09:20: bias from 09:15 close vs PDC
    early = evaluate_nifty_cue(
        prev_close=100.0,
        first_open=101.0,
        first_close=101.5,
        second_close=None,
    )
    assert early["nifty_bias"] == "bullish"
    assert early["nifty_show_on"] == "BULLISH"


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


def test_resolve_equity_key_prefers_universe_nse_eq():
    from backend.services.open_low_high.screener import _resolve_equity_key

    class _Ux:
        def get_instrument_key(self, symbol):
            return "NSE_EQ|FALLBACK"

    row = {"underlying_key": "NSE_EQ|INE123A01016"}
    assert _resolve_equity_key(_Ux(), row, "FOO") == "NSE_EQ|INE123A01016"


def test_resolve_equity_key_falls_back_to_upstox():
    from backend.services.open_low_high.screener import _resolve_equity_key

    class _Ux:
        def get_instrument_key(self, symbol):
            assert symbol == "INDIGO"
            return "NSE_EQ:INE646L01027"

    assert _resolve_equity_key(_Ux(), {"underlying_key": ""}, "INDIGO") == "NSE_EQ|INE646L01027"


def test_row_to_dict_does_not_copy_equity_open_as_sl():
    from backend.services.open_low_high.persistence import _row_to_dict

    legacy = _row_to_dict(
        {"first_open": 100.0, "sl": None, "active": True, "phase_b_ok": True}
    )
    assert legacy["sl"] == 100.0

    equity = _row_to_dict(
        {
            "first_open": 100.0,
            "sl": None,
            "equity_instrument_key": "NSE_EQ|INE123A01016",
            "active": True,
            "phase_b_ok": True,
        }
    )
    assert equity["sl"] is None
    assert equity["data_source_logic"] == "equity"
    assert equity["data_source_levels"] == "future"

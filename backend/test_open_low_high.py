"""Unit tests for Open Low High screener rules — no network."""
from __future__ import annotations

from backend.services.open_low_high.signals import (
    confirm_phase_b,
    evaluate_phase_a,
    freeze_tags_immutable,
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


def test_phase_b_bullish_keep_when_second_closes_above_first_high():
    assert confirm_phase_b("BULLISH", first_high=102.0, first_low=99.0, second_close=102.5) is True
    assert confirm_phase_b("BULLISH", first_high=102.0, first_low=99.0, second_close=102.0) is False
    assert confirm_phase_b("BULLISH", first_high=102.0, first_low=99.0, second_close=101.0) is False


def test_phase_b_bearish_keep_when_second_closes_below_first_low():
    assert confirm_phase_b("BEARISH", first_high=100.0, first_low=97.0, second_close=96.5) is True
    assert confirm_phase_b("BEARISH", first_high=100.0, first_low=97.0, second_close=97.0) is False
    assert confirm_phase_b("BEARISH", first_high=100.0, first_low=97.0, second_close=97.5) is False


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

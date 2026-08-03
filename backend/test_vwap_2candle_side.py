"""Unit tests for 2-candle VWAP directional side confirmation."""
from __future__ import annotations

from datetime import datetime

import pytz

from backend.services.vwap_2candle_side import (
    evaluate_2candle_vwap_confirm,
    resolve_directional_side,
)

IST = pytz.timezone("Asia/Kolkata")


def _bar(h: int, m: int, close: float, vwap: float) -> dict:
    return {
        "bar_end": IST.localize(datetime(2026, 8, 3, h, m)),
        "close": close,
        "vwap": vwap,
    }


def test_short_confirm_requires_extension():
    bars = [
        _bar(10, 35, 970.0, 969.0),  # still above
        _bar(10, 45, 966.95, 969.25),  # flip below
        _bar(10, 55, 966.25, 969.22),  # further below → confirm
    ]
    # At idx=1 (flip only): not confirmed yet
    r0 = evaluate_2candle_vwap_confirm("LONG", bars, 1)
    assert r0["confirmed"] is False
    # At idx=2
    r = evaluate_2candle_vwap_confirm("LONG", bars, 2)
    assert r["confirmed"] is True
    assert r["new_side"] == "SHORT"


def test_reject_raw_flip_without_confirm():
    bars = [
        _bar(10, 35, 970.0, 969.0),
        _bar(10, 45, 968.5, 969.0),  # below but alone
    ]
    # Only one adverse bar — no extension yet
    out = resolve_directional_side("LONG", "SHORT", bars, 1)
    assert out["action"] == "flip_rejected_no_confirm"
    assert out["side"] == "LONG"


def test_force_confirm_even_if_raw_still_long():
    bars = [
        _bar(10, 45, 966.95, 969.25),
        _bar(10, 55, 966.25, 969.22),
    ]
    out = resolve_directional_side("LONG", "LONG", bars, 1)
    assert out["action"] == "confirmed_flip"
    assert out["side"] == "SHORT"


def test_no_whipsaw_accept_without_extension():
    bars = [
        _bar(10, 45, 966.95, 969.25),
        _bar(10, 55, 968.0, 969.22),  # still below but closer → not extended
    ]
    r = evaluate_2candle_vwap_confirm("LONG", bars, 1)
    assert r["confirmed"] is False
    assert "not_extended" in (r.get("reason") or "")

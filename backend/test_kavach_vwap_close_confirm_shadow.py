"""Unit tests for VWAP close-confirmation shadow helpers (no DB)."""
from __future__ import annotations

from backend.services.kavach_vwap_close_confirm_shadow import (
    compute_vwap_close_confirmed,
    is_ready_like,
)


def test_long_close_above_vwap_confirms():
    assert compute_vwap_close_confirmed(direction="LONG", close=100.5, vwap=100.0) is True
    assert compute_vwap_close_confirmed(direction="LONG", close=100.0, vwap=100.0) is False
    assert compute_vwap_close_confirmed(direction="LONG", close=99.5, vwap=100.0) is False


def test_short_close_below_vwap_confirms():
    assert compute_vwap_close_confirmed(direction="SHORT", close=99.5, vwap=100.0) is True
    assert compute_vwap_close_confirmed(direction="SHORT", close=100.0, vwap=100.0) is False
    assert compute_vwap_close_confirmed(direction="SHORT", close=100.5, vwap=100.0) is False


def test_wick_alone_does_not_confirm():
    # Helper is close-vs-vwap only — wick through VWAP with close on wrong side = False.
    assert compute_vwap_close_confirmed(direction="LONG", close=99.0, vwap=100.0) is False
    assert compute_vwap_close_confirmed(direction="SHORT", close=101.0, vwap=100.0) is False


def test_ready_like_states():
    assert is_ready_like("READY") is True
    assert is_ready_like("READY(RECHECK)") is True
    assert is_ready_like("WAIT FOR PULLBACK") is False
    assert is_ready_like("EXPIRED") is False

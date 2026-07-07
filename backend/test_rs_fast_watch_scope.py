"""Tests for Fast Watch universe scope (locked ∪ RS top-5)."""
from backend.services.rs_fast_watch import (
    SCOPE_LOCKED_ONLY,
    SCOPE_LOCKED_OR_TOP5,
    _flip_state,
    universe_symbols,
)


def test_universe_locked_or_top5_union():
    locked = {"DIXON", "MANAPPURAM"}
    top5 = {"NAUKRI", "SBILIFE"}
    out = universe_symbols(
        "2026-07-07",
        locked=locked,
        top5_symbols=top5,
    )
    assert out == {"DIXON", "MANAPPURAM", "NAUKRI", "SBILIFE"}


def test_naukri_in_top5_not_locked_would_record():
    """NAUKRI 2026-07-07: in RS top-5 from 09:41, not on 09:25 lock."""
    locked = {
        "DIXON", "MANAPPURAM", "BANDHANBNK", "INDUSINDBK", "CGPOWER",
        "KALYANKJIL", "COCHINSHIP", "PREMIERENE", "AUROPHARMA", "COALINDIA",
    }
    top5 = {"NAUKRI", "SBILIFE", "TATAELXSI", "EICHERMOT", "INFY"}
    out = universe_symbols("2026-07-07", locked=locked, top5_symbols=top5)
    assert "NAUKRI" in out
    assert "NAUKRI" not in locked
    assert _flip_state("BUY", "LONG") is True


def test_flip_state_unchanged():
    assert _flip_state("BUY", "LONG") is True
    assert _flip_state("WATCH", "LONG") is False

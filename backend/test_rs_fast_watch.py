"""L4 — Fast Watch flip detection (unit, no DB)."""
from backend.services.rs_fast_watch import _conflict, _flip_state


def test_bull_flip_states():
    assert _flip_state("BUY", "LONG") is True
    assert _flip_state("READY", "LONG") is True
    assert _flip_state("WATCH", "LONG") is False


def test_bear_flip_states():
    assert _flip_state("SELL", "SHORT") is True
    assert _flip_state("READY SHORT", "SHORT") is True
    assert _flip_state("WATCH SHORT", "SHORT") is False


def test_conflict_opposite_direction():
    assert _conflict("BUY", "SHORT") is True
    assert _conflict("SELL", "LONG") is True
    assert _conflict("BUY", "LONG") is False

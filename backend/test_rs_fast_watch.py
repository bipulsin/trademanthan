"""L4 — Fast Watch flip detection (unit, no DB)."""
from backend.services.rs_fast_watch import _conflict, is_edge_flip, kavach_direction


def test_bull_flip_direction():
    assert kavach_direction("BUY") == "LONG"
    assert kavach_direction("READY") == "LONG"


def test_bear_flip_direction():
    assert kavach_direction("SELL") == "SHORT"
    assert kavach_direction("READY SHORT") == "SHORT"


def test_edge_flip_transition():
    assert is_edge_flip("SELL", "BUY") is True
    assert is_edge_flip(None, "BUY") is False


def test_conflict_opposite_direction():
    assert _conflict("BUY", "SHORT") is True
    assert _conflict("SELL", "LONG") is True
    assert _conflict("BUY", "LONG") is False

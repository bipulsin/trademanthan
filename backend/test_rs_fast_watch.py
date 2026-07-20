"""L4 — Fast Watch flip detection (unit, no DB)."""
from backend.services import rs_fast_watch as fw
from backend.services.rs_fast_watch import _conflict, is_edge_flip, kavach_direction
from backend.services.rs_live_kavach_audit import latest_audit_pair


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


def test_latest_audit_pair_imported_for_edge_fallback():
    """Regression: record_fast_watch_flips must resolve latest_audit_pair (Jul-2026 outage)."""
    assert getattr(fw, "latest_audit_pair", None) is latest_audit_pair
    assert callable(fw.latest_audit_pair)

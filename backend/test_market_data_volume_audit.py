"""Tests for volume-zero audit and WS ltq volume accumulation."""
from backend.services.market_data_volume_audit import audit_zero_volume_fraction
from backend.services.upstox_market_feed import _accumulate_ltq_volume


def test_audit_warns_on_mostly_zero_volumes():
    msg = audit_zero_volume_fraction([0] * 95 + [100] * 5, context="test")
    assert msg is not None
    assert "95/100" in msg


def test_audit_silent_when_volumes_ok():
    msg = audit_zero_volume_fraction([10, 20, 30, 40], context="test", min_samples=4)
    assert msg is None


def test_ltq_accumulates_per_minute():
    ik = "NSE_FO|1"
    minute = "2026-07-07T09:30:00+05:30"
    v1 = _accumulate_ltq_volume(ik, minute, 50)
    v2 = _accumulate_ltq_volume(ik, minute, 30)
    assert v1 == 50
    assert v2 == 80

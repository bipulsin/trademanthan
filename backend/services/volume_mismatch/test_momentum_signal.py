"""Tests for VM momentum-open and gap/BB signal paths."""
from backend.services.volume_mismatch.signal_rules import evaluate_vm_signal


def _bb():
    return {"bb_upper": 110.0, "bb_middle": 100.0, "bb_lower": 90.0}


def test_momentum_open_long_mild_gap_down_high_rel_vol():
    """Mild gap-down (-0.35%) with 3x rel vol — actual ICICIBANK 11-Jun first 15m shape."""
    first_bar = {
        "open": 1292.1,
        "high": 1305.0,
        "low": 1290.0,
        "close": 1302.8,
        "volume": 800_000,
    }
    sig = evaluate_vm_signal(
        symbol="ICICIBANK",
        future_symbol="ICICIBANK26JUNFUT",
        instrument_key="NSE_FO|X",
        first_bar=first_bar,
        previous_close=1296.6,
        bb={"bb_upper": 1320.0, "bb_middle": 1264.8, "bb_lower": 1229.7},
        relative_volume=2.98,
    )
    assert sig is not None
    assert sig["direction"] == "LONG"
    assert sig["signal_path"] == "momentum_open"


def test_classic_gap_down_long_still_works():
    first_bar = {
        "open": 98.0,
        "high": 99.5,
        "low": 97.0,
        "close": 99.0,
        "volume": 400_000,
    }
    sig = evaluate_vm_signal(
        symbol="TEST",
        future_symbol="TESTFUT",
        instrument_key="NSE_FO|Y",
        first_bar=first_bar,
        previous_close=100.0,
        bb={"bb_upper": 105.0, "bb_middle": 100.0, "bb_lower": 99.5},
        relative_volume=1.5,
    )
    assert sig is not None
    assert sig["direction"] == "LONG"
    assert sig["signal_path"] == "gap_bb"


def test_momentum_open_rejects_low_rel_vol():
    first_bar = {
        "open": 101.0,
        "high": 102.0,
        "low": 100.5,
        "close": 101.8,
        "volume": 50_000,
    }
    sig = evaluate_vm_signal(
        symbol="ICICIBANK",
        future_symbol="ICICIBANK26JUNFUT",
        instrument_key="NSE_FO|X",
        first_bar=first_bar,
        previous_close=100.0,
        bb=_bb(),
        relative_volume=0.9,
    )
    assert sig is None

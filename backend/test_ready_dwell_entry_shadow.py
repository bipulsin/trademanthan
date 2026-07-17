"""Unit tests for READY dwell + entry distance shadow helpers (no live flip)."""
from backend.services.ready_dwell_entry_shadow import (
    ENTRY_EMA5_WARN_PCT,
    evaluate_entry_distance_guard,
    evaluate_threshold_sensitivity,
    min_gap_pts,
    ready_dwell_entry_live_enabled,
    soft_hide_reason,
)


def test_live_flip_default_off(monkeypatch):
    monkeypatch.delenv("READY_DWELL_ENTRY_LIVE", raising=False)
    assert ready_dwell_entry_live_enabled() is False


def test_live_flip_on(monkeypatch):
    monkeypatch.setenv("READY_DWELL_ENTRY_LIVE", "1")
    assert ready_dwell_entry_live_enabled() is True


def test_min_gap_option_a_pct_dominates():
    # BSE-like: 0.3%*3600=10.8 > 300/200=1.5
    assert abs(min_gap_pts(3600.0, 200) - 10.8) < 1e-9
    assert abs(min_gap_pts(3600.0, 200, option="A") - 10.8) < 1e-9


def test_min_gap_option_b_stricter_lot_floor():
    # Cheap name: A uses 300/lot=6, B uses 500/lot=10
    assert abs(min_gap_pts(100.0, 50, option="A") - 6.0) < 1e-9
    assert abs(min_gap_pts(100.0, 50, option="B") - 10.0) < 1e-9


def test_min_gap_option_c_atr():
    # 0.3%*100=0.3, 0.25*ATR=0.25*8=2 → 2
    assert abs(min_gap_pts(100.0, 50, option="C", atr_pts=8.0) - 2.0) < 1e-9


def test_min_gap_option_a_lot_floor_when_cheap():
    assert abs(min_gap_pts(100.0, 50) - 6.0) < 1e-9


def test_check3_only_cumminsind_style():
    out = evaluate_entry_distance_guard(
        is_long=True,
        entry=5400.0,
        ema5=5400.0,
        ema10=5398.38,
        price=5400.0,
        lot=200,
    )
    assert out["would_block"] is True
    assert out["check2_entry_thin"] is True
    assert out["check3_stack_thin"] is True
    assert out["check1_beyond_ema10"] is False


def test_check3_only_when_entry_far_but_stack_thin():
    out = evaluate_entry_distance_guard(
        is_long=True,
        entry=5500.0,
        ema5=5401.0,
        ema10=5400.0,
        price=5500.0,
        lot=200,
    )
    assert out["check2_entry_thin"] is False
    assert out["check3_stack_thin"] is True
    assert out["check3_only"] is True
    assert out["would_block"] is True


def test_check1_beyond_blocks_long():
    out = evaluate_entry_distance_guard(
        is_long=True,
        entry=99.0,
        ema5=99.0,
        ema10=100.0,
        price=99.0,
        lot=50,
    )
    assert out["check1_beyond_ema10"] is True
    assert out["would_block"] is True


def test_warn_entry_off_ema5_does_not_block():
    out = evaluate_entry_distance_guard(
        is_long=True,
        entry=101.0,
        ema5=100.0,
        ema10=90.0,
        price=101.0,
        lot=50,
        ema5_tol_pct=ENTRY_EMA5_WARN_PCT,
    )
    assert out["would_block"] is False
    assert out["warn_entry_off_ema5"] is True
    assert out["would_warn"] is True


def test_healthy_gap_passes():
    out = evaluate_entry_distance_guard(
        is_long=True,
        entry=3600.0,
        ema5=3600.0,
        ema10=3580.0,
        price=3600.0,
        lot=200,
    )
    assert out["would_block"] is False
    assert out["block_checks"] == []


def test_soft_hide_warning_stack():
    assert soft_hide_reason({"zone_downgrade": "warning_stack"}) == "warning_stack"
    assert soft_hide_reason({"trade_state_reason": "WAIT · warning stack (CHURN)"}) == (
        "warning_stack"
    )
    assert soft_hide_reason({"zone_downgrade": "compromised_lock"}) is None


def test_threshold_sensitivity_b_stricter_on_cheap():
    # Gap 7 pts: passes A (floor 6) but blocked by B (floor 10)
    sens = evaluate_threshold_sensitivity(
        is_long=True,
        entry=100.0,
        ema5=100.0,
        ema10=93.0,
        price=100.0,
        lot=50,
        atr_pts=4.0,  # C floor max(0.3, 1.0)=1.0 → pass
    )
    assert sens["A"]["would_block"] is False
    assert sens["B"]["would_block"] is True
    assert sens["B_stricter_than_A"] is True
    assert sens["C"]["would_block"] is False


def test_live_option_default_b(monkeypatch):
    monkeypatch.delenv("READY_DWELL_ENTRY_OPTION", raising=False)
    from backend.services.ready_dwell_entry_shadow import live_distance_option

    assert live_distance_option() == "B"


def test_live_option_env_override(monkeypatch):
    monkeypatch.setenv("READY_DWELL_ENTRY_OPTION", "A")
    from backend.services.ready_dwell_entry_shadow import live_distance_option

    assert live_distance_option() == "A"

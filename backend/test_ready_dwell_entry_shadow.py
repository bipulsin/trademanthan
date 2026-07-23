"""Unit tests for READY dwell + entry distance shadow helpers (no live flip)."""
from datetime import datetime
from unittest.mock import MagicMock

import pytz

from backend.services.daily_checklist_trade_state import (
    STATE_READY,
    STATE_WAIT,
)
from backend.services.ready_dwell_entry_shadow import (
    ENTRY_EMA5_WARN_PCT,
    apply_ready_dwell_entry_live,
    evaluate_entry_distance_guard,
    evaluate_threshold_sensitivity,
    min_gap_pts,
    ready_dwell_entry_live_enabled,
    soft_hide_reason,
)

IST = pytz.timezone("Asia/Kolkata")


def _stock(**kwargs):
    base = {
        "symbol": "TEST",
        "direction": "LONG",
        "in_lock": True,
        "_pre_stack_state": STATE_READY,
        "trade_state": STATE_READY,
        "trade_entry": 100.0,
        "trade_sl": 90.0,
        "trade_risk_inr": 500,
        "trade_rr": 2.0,
        "trade_lot": 50,
        "live_candle_ema5": 100.0,
        "live_candle_ema10": 90.0,
        "live_candle_price": 100.0,
        "trade_entry_window_open": True,
        "gate_badges": [],
    }
    base.update(kwargs)
    return base


def _apply(monkeypatch, stocks, *, since=None, now=None, live=True):
    if live:
        monkeypatch.setenv("READY_DWELL_ENTRY_LIVE", "1")
        monkeypatch.setenv("READY_DWELL_ENTRY_OPTION", "B")
    else:
        monkeypatch.delenv("READY_DWELL_ENTRY_LIVE", raising=False)
    monkeypatch.setattr(
        "backend.services.ready_dwell_entry_shadow._load_shadow_since",
        lambda *_a, **_k: since,
    )
    monkeypatch.setattr(
        "backend.services.ready_dwell_entry_shadow._upsert_shadow_state",
        lambda *_a, **_k: None,
    )
    monkeypatch.setattr(
        "backend.services.daily_checklist_trade_state.entry_window_open_ist",
        lambda: True,
    )
    clock = now or IST.localize(datetime(2026, 7, 22, 11, 0, 0))
    return apply_ready_dwell_entry_live(
        stocks,
        db=MagicMock(),
        session_date="2026-07-22",
        candle_cache={},
        lot_cache={"TEST": 50},
        atr_pct_map={"TEST": 1.0},
        nifty_pct=0.0,
        now=clock,
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


def test_live_starts_dwell_when_ready(monkeypatch):
    s = _stock()
    stats = _apply(monkeypatch, [s], since=None)
    assert stats["dwell_started"] == 1
    assert s["card_visible"] is True
    assert s["trade_state"] == STATE_READY
    assert s["trade_take_enabled"] is True
    assert s.get("ready_visible_since")


def test_live_soft_warning_stack_holds_card_disables_take(monkeypatch):
    since = IST.localize(datetime(2026, 7, 22, 10, 55, 0))
    now = IST.localize(datetime(2026, 7, 22, 11, 0, 0))  # 5m elapsed
    s = _stock(
        trade_state=STATE_WAIT,
        zone_downgrade="warning_stack",
        trade_state_reason="WAIT · warning stack (CHURN+REGIME UNSTABLE)",
        trade_take_enabled=False,
    )
    stats = _apply(monkeypatch, [s], since=since, now=now)
    assert stats["dwell_soft_kept"] == 1
    assert s["card_visible"] is True
    assert s["trade_state"] == STATE_READY
    assert s["trade_take_enabled"] is False
    assert s.get("dwell_soft_hold") is True


def test_live_distance_mid_dwell_keeps_card(monkeypatch):
    """Post-live under-10m vanish cause: distance must not remove card inside floor."""
    since = IST.localize(datetime(2026, 7, 22, 10, 55, 0))
    now = IST.localize(datetime(2026, 7, 22, 11, 0, 0))
    # Gap too thin for Option B (500/lot=10 on lot 50 → floor 10; gap=5)
    s = _stock(
        live_candle_ema5=100.0,
        live_candle_ema10=95.0,
        live_candle_price=100.0,
        trade_entry=100.0,
        trade_sl=95.0,
    )
    stats = _apply(monkeypatch, [s], since=since, now=now)
    assert stats["distance_dwell_held"] == 1
    assert s["card_visible"] is True
    assert s["trade_state"] == STATE_READY
    assert s["trade_take_enabled"] is False
    assert s.get("zone_downgrade") == "entry_distance"


def test_live_distance_first_poll_still_suppresses(monkeypatch):
    s = _stock(
        live_candle_ema5=100.0,
        live_candle_ema10=95.0,
        live_candle_price=100.0,
        trade_entry=100.0,
        trade_sl=95.0,
    )
    stats = _apply(monkeypatch, [s], since=None)
    assert stats["distance_blocked"] == 1
    assert s["card_visible"] is False
    assert s["trade_state"] == STATE_WAIT


def test_live_natural_leave_inside_floor_keeps_card(monkeypatch):
    since = IST.localize(datetime(2026, 7, 22, 10, 55, 0))
    now = IST.localize(datetime(2026, 7, 22, 11, 0, 0))
    s = _stock(
        trade_state=STATE_WAIT,
        _pre_stack_state=STATE_WAIT,
        trade_state_reason="WAIT · grade decay",
        trade_take_enabled=False,
    )
    stats = _apply(monkeypatch, [s], since=since, now=now)
    assert stats["dwell_natural_kept"] == 1
    assert s["card_visible"] is True
    assert s["trade_state"] == STATE_READY
    assert s["trade_take_enabled"] is False


def test_live_ema10_hard_hides_inside_floor(monkeypatch):
    """Confirmed EMA10 close reverse: hide early (misleading to keep READY NOW)."""
    since = IST.localize(datetime(2026, 7, 22, 10, 55, 0))
    now = IST.localize(datetime(2026, 7, 22, 11, 0, 0))
    s = _stock()
    monkeypatch.setattr(
        "backend.services.ready_dwell_entry_shadow.hard_invalidate_reason",
        lambda *a, **k: ("ema10_close", {"beyond": True}),
    )
    stats = _apply(monkeypatch, [s], since=since, now=now)
    assert s["card_visible"] is False
    assert s["trade_state"] == STATE_WAIT
    assert s.get("ready_visible_since") is None
    assert stats.get("dwell_soft_kept", 0) == 0


def test_live_soft_after_floor_allows_removal(monkeypatch):
    since = IST.localize(datetime(2026, 7, 22, 10, 45, 0))
    now = IST.localize(datetime(2026, 7, 22, 11, 0, 0))  # 15m elapsed
    s = _stock(
        trade_state=STATE_WAIT,
        zone_downgrade="warning_stack",
        trade_state_reason="WAIT · warning stack",
        trade_take_enabled=False,
    )
    _apply(monkeypatch, [s], since=since, now=now)
    assert s["card_visible"] is False
    assert s["trade_state"] == STATE_WAIT

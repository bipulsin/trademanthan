"""Unit tests for ATR READY display suppress + DI override shadow."""
from backend.services.atr_ready_suppress import (
    STATE_WATCHING,
    apply_atr_ready_suppress_display,
    atr_ready_suppress_live_enabled,
    evaluate_atr_ready_suppress,
    evaluate_would_override_di,
    progression_increasing,
)


def test_progression_increasing_matches_v2_noise_floor():
    # Rising >0.5pp vs prior
    assert progression_increasing([80.0, 85.5], [1.0, 1.2], "LONG") is True
    # Flat / noise
    assert progression_increasing([85.0, 85.3], [1.0, 1.0], "LONG") is False
    # Rising but signed move against SHORT
    assert progression_increasing([80.0, 90.0], [1.0, 1.5], "SHORT") is False
    # Rising with short-aligned signed move
    assert progression_increasing([80.0, 90.0], [-1.0, -1.5], "SHORT") is True
    # 3-bar: cur > hist[-3]+0.5 and cur >= prev
    assert progression_increasing([70.0, 80.0, 80.5], [0.5, 1.0, 1.1], "LONG") is True


def test_evaluate_suppress_85_not_progressing():
    d = evaluate_atr_ready_suppress(
        atr_consumed_pct=85.11,
        hist_ac=[85.0, 85.11],  # +0.11pp < 0.5 noise → not progressing
        hist_signed=[1.0, 1.0],
        direction="LONG",
        threshold_pct=85.0,
    )
    assert d["atr_ready_suppress_would"] is True
    assert d["atr_progression_increasing"] is False
    assert d["atr_consumed_pct"] == 85.11


def test_evaluate_keep_ready_when_progressing():
    d = evaluate_atr_ready_suppress(
        atr_consumed_pct=90.0,
        hist_ac=[80.0, 90.0],
        hist_signed=[1.0, 1.5],
        direction="LONG",
        threshold_pct=85.0,
    )
    assert d["atr_ready_suppress_would"] is False
    assert d["atr_progression_increasing"] is True


def test_apply_display_toggle(monkeypatch):
    stock = {
        "trade_state": "READY",
        "trade_take_enabled": True,
        "trade_entry": 100.0,
        "trade_sl": 98.0,
        "trade_risk_inr": 200,
    }
    decision = {
        "atr_ready_suppress_would": True,
        "atr_consumed_pct": 88.0,
        "atr_ready_suppress_threshold_pct": 85.0,
    }
    monkeypatch.setenv("ATR_READY_SUPPRESS_LIVE", "0")
    assert atr_ready_suppress_live_enabled() is False
    assert apply_atr_ready_suppress_display(stock, decision) is False
    assert stock["trade_state"] == "READY"
    assert decision["atr_ready_suppress_fired"] is False

    monkeypatch.setenv("ATR_READY_SUPPRESS_LIVE", "1")
    decision2 = dict(decision)
    assert apply_atr_ready_suppress_display(stock, decision2) is True
    assert stock["trade_state"] == STATE_WATCHING
    assert stock["trade_take_enabled"] is False
    assert decision2["atr_ready_suppress_fired"] is True


def test_would_override_di_only():
    out = evaluate_would_override_di(
        rendered_state="READY",
        grade="A",
        trade_take_enabled=False,
        trade_state_reason="BULL direction unstable",
        zone_downgrade="direction_imbalance",
    )
    assert out["would_override_di"] is True

    blocked = evaluate_would_override_di(
        rendered_state="READY",
        grade="A",
        trade_take_enabled=False,
        trade_state_reason="WAIT · warning stack (CHURN+REGIME UNSTABLE)",
        zone_downgrade="warning_stack",
    )
    assert blocked["would_override_di"] is False
    assert blocked.get("would_override_di_skip") == "warning_stack"

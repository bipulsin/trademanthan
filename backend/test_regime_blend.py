"""Unit tests for live NIFTY + signed-VWAP regime blend (warning_stack)."""
from backend.services.regime_blend import (
    apply_regime_blend_to_stocks,
    compute_regime_blend,
    nifty_regime_component,
    signed_stock_vwap_slope_component,
)


def test_nifty_component():
    assert nifty_regime_component("TREND") == 1.0
    assert nifty_regime_component("TRANSITION") == 0.0
    assert nifty_regime_component("CHOP") == 0.0


def test_signed_stock_long_short():
    # LONG needs +signed
    c, ok, _ = signed_stock_vwap_slope_component(
        direction="LONG", slope_score=25.0, signed_slope_atr=0.1
    )
    assert ok is True
    assert c == 0.5
    c2, ok2, _ = signed_stock_vwap_slope_component(
        direction="LONG", slope_score=25.0, signed_slope_atr=-0.1
    )
    assert ok2 is False
    assert c2 == 0.0
    # SHORT needs -signed
    c3, ok3, _ = signed_stock_vwap_slope_component(
        direction="SHORT", slope_score=50.0, signed_slope_atr=-0.2
    )
    assert ok3 is True
    assert c3 == 1.0


def test_blend_transition_steep_clears_unstable():
    # TRANSITION + steep LONG → blend = 0.8 >= 0.40 → stable
    info = compute_regime_blend(
        direction="LONG",
        market_regime="TRANSITION",
        slope_score=50.0,
        signed_slope_atr=0.2,
    )
    assert info["regime_blend"] == 0.8
    assert info["regime_unstable_for_stack"] is False


def test_blend_transition_flat_stays_unstable():
    info = compute_regime_blend(
        direction="LONG",
        market_regime="TRANSITION",
        slope_score=0.0,
        signed_slope_atr=0.0,
    )
    assert info["regime_blend"] == 0.0
    assert info["regime_unstable_for_stack"] is True


def test_apply_removes_badge_when_blend_clears():
    stocks = [
        {
            "symbol": "TIINDIA",
            "direction": "SHORT",
            "trade_state": "READY",
            "gate_badges": ["REGIME UNSTABLE", "CHURN 10"],
            "vwap_quality": {"slope_score": 26.42, "signed_slope_atr": -0.1321},
            "regime_context": {"market_regime": "TRANSITION"},
        }
    ]
    stats = apply_regime_blend_to_stocks(stocks, market_regime="TRANSITION")
    assert stats["badge_removed"] == 1
    assert "REGIME UNSTABLE" not in stocks[0]["gate_badges"]
    assert stocks[0]["regime_unstable_for_stack"] is False
    assert stocks[0]["regime_blend"] >= 0.40


def test_apply_keeps_badge_when_blend_fails():
    stocks = [
        {
            "symbol": "FLAT",
            "direction": "LONG",
            "gate_badges": ["CHURN 8"],
            "vwap_quality": {"slope_score": 2.0, "signed_slope_atr": 0.01},
            "regime_context": {"market_regime": "TRANSITION"},
        }
    ]
    apply_regime_blend_to_stocks(stocks, market_regime="TRANSITION")
    assert "REGIME UNSTABLE" in stocks[0]["gate_badges"]
    assert stocks[0]["regime_unstable_for_stack"] is True


def test_warning_stack_clears_when_blend_removes_regime_flag():
    from backend.services.daily_checklist_trade_state import (
        STATE_READY,
        apply_warning_stack_downgrades,
    )

    stocks = [
        {
            "symbol": "TIINDIA",
            "direction": "SHORT",
            "trade_state": STATE_READY,
            "trade_take_enabled": True,
            "pullback_count": 1,
            "gate_badges": ["REGIME UNSTABLE", "CHURN 10"],
            "vwap_quality": {"slope_score": 26.42, "signed_slope_atr": -0.1321},
            "regime_context": {"market_regime": "TRANSITION"},
        }
    ]
    apply_regime_blend_to_stocks(stocks, market_regime="TRANSITION")
    n = apply_warning_stack_downgrades(stocks)
    assert n == 0
    assert stocks[0]["trade_state"] == STATE_READY
    assert stocks[0]["trade_take_enabled"] is True

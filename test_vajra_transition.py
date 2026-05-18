"""Unit tests for Vajra TPS / transition pipeline."""
from backend.services.vajra.engine import compute_ecs_rating, sort_vajra_rows, TRADE_TYPE_SORT_ORDER
from backend.services.vajra.transition import (
    EARLY_LONG,
    compute_pullback_quality,
    compute_tps,
    classify_early_transition,
    validate_execution_5m,
)
from test_vajra_engine import _synthetic_candles


def test_compute_tps_returns_scores():
    c30 = _synthetic_candles(120, trend=0.4)
    tps = compute_tps(c30, market_phase="COMPRESSION")
    assert tps is not None
    assert 0 <= tps.tps_bull <= 100
    assert 0 <= tps.tps_bear <= 100
    assert tps.transition_state


def test_early_transition_before_a_plus():
    c30 = _synthetic_candles(120, trend=0.6)
    c60 = _synthetic_candles(80, trend=0.4)
    ecs = compute_ecs_rating(c30, c60)
    assert ecs is not None
    tps = compute_tps(c30, market_phase=ecs.market_phase)
    assert tps is not None
    early = classify_early_transition(
        tps,
        ecs_trade_type=ecs.trade_type,
        ecs_bull=ecs.bull_score,
        ecs_bear=ecs.bear_score,
    )
    if early:
        assert early in (EARLY_LONG, "EARLY SHORT TRANSITION")


def test_sort_prioritizes_early():
    rows = [
        {"trade_type": "LONG  [A+]", "confidence": 90, "tps_score": 40, "security": "A"},
        {"trade_type": EARLY_LONG, "confidence": 55, "tps_score": 72, "security": "B"},
    ]
    sorted_rows = sort_vajra_rows(rows, discovery_first=True)
    assert sorted_rows[0]["trade_type"] == EARLY_LONG


def test_execution_validation_runs():
    c5 = _synthetic_candles(80, trend=0.3)
    ex = validate_execution_5m(c5, bull_dir=True)
    assert ex.steps_passed >= 0


def test_pullback_quality_bounded():
    c = _synthetic_candles(90, trend=0.2)
    closes = [float(x["close"]) for x in c]
    opens = [float(x["open"]) for x in c]
    highs = [float(x["high"]) for x in c]
    lows = [float(x["low"]) for x in c]
    from backend.services.vajra.indicators import ema_series

    ema5 = ema_series(closes, 5)
    q = compute_pullback_quality(opens, highs, lows, closes, ema5, 1.0, bull_dir=True)
    assert 0 <= q <= 100

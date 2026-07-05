"""Unit tests for momentum ignition historical validation helpers."""
from backend.services.kavach_momentum_ignition_validate import (
    _aggregate_precision,
    _analyze_symbol_candles,
    _empty_hits,
    _moved_favorably,
    format_backtest_plain_text,
    oi_triangulation_from_candles,
)


def _make_candles(n=80, trend=0.05):
    candles = []
    for i in range(n):
        base = 100 + i * trend
        candles.append({
            "timestamp": f"2026-06-01T{9 + i // 12:02d}:{(i * 5) % 60:02d}:00+05:30",
            "open": base,
            "high": base + 0.6,
            "low": base - 0.1,
            "close": base + 0.4,
            "volume": 1000 + i * 50,
            "oi": 50000 + i * 100,
        })
    return candles


def test_moved_favorably_bull():
    assert _moved_favorably(0.2, "BULL") is True
    assert _moved_favorably(0.05, "BULL") is False


def test_oi_triangulation_from_candles_long_buildup():
    candles = _make_candles(10)
    score, label = oi_triangulation_from_candles(candles, "BULL")
    assert score >= 0
    assert label


def test_analyze_symbol_candles_counts_samples():
    candles = _make_candles(80)
    hits, samples, favorable, _pb = _analyze_symbol_candles(candles, "BULL", {})
    assert samples > 0
    assert favorable >= 0
    assert hits["vwap_slope_signals"] >= 0


def test_ignition_component_weights_bull_zero_pullback():
    from backend.services.kavach_momentum_ignition import ignition_component_weights
    from backend.services.rs_conviction_config import DEFAULTS

    w_bull = ignition_component_weights("BULL", DEFAULTS)
    w_bear = ignition_component_weights("BEAR", DEFAULTS)
    assert w_bull["pullback"] == 0.0
    assert w_bear["pullback"] == 0.10
    assert w_bull["oi"] == 0.03
    assert w_bear["oi"] == 0.03


def test_wilson_ci_and_credibility():
    from backend.services.kavach_momentum_ignition_validate import (
        _credibility_label,
        _wilson_ci,
    )

    lo, hi = _wilson_ci(50, 100)
    assert lo is not None and hi is not None and lo < hi
    assert _credibility_label(50, 100, 0.20) == "credible_positive"
    assert _credibility_label(20, 100, 0.30) == "credible_negative"


def test_aggregate_precision_order_flow_na():
    per = {"RELIANCE": _empty_hits()}
    agg = _aggregate_precision(per, baseline_rate=0.25)
    assert agg["order_flow_imbalance"]["status"] == "not_applicable"
    assert agg["order_flow_imbalance"]["precision_3bar"] is None


def test_lift_fields_positive_when_precision_beats_baseline():
    from backend.services.kavach_momentum_ignition_validate import _lift_fields

    lifts = _lift_fields(0.30, 0.20)
    assert lifts["lift_pp"] == 0.10
    assert lifts["lift_ratio"] == 1.5


def test_format_backtest_plain_text_includes_baseline():
    text = format_backtest_plain_text({
        "started_at": "2026-06-01T10:00:00",
        "finished_at": "2026-06-01T10:05:00",
        "parameters": {"days": 10, "symbols": 20, "side": "BULL"},
        "symbols_with_data": 5,
        "universe_requested": 20,
        "bar_samples": 100,
        "baseline": {"bar_samples": 100, "favorable_moves": 25, "favorable_rate_3bar": 0.25},
        "components": _aggregate_precision({}, baseline_rate=0.25),
    })
    assert "Baseline (unconditional" in text
    assert "Order-flow imbalance" in text
    assert "OI-price-volume" in text

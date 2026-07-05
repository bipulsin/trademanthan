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
    hits, samples = _analyze_symbol_candles(candles, "BULL", {})
    assert samples > 0
    assert hits["vwap_slope_signals"] >= 0


def test_aggregate_precision_order_flow_na():
    per = {"RELIANCE": _empty_hits()}
    agg = _aggregate_precision(per)
    assert agg["order_flow_imbalance"]["status"] == "not_applicable"
    assert agg["order_flow_imbalance"]["precision_3bar"] is None


def test_format_backtest_plain_text_includes_components():
    text = format_backtest_plain_text({
        "started_at": "2026-06-01T10:00:00",
        "finished_at": "2026-06-01T10:05:00",
        "parameters": {"days": 10, "symbols": 20, "side": "BULL"},
        "symbols_with_data": 5,
        "universe_requested": 20,
        "bar_samples": 100,
        "components": _aggregate_precision({}),
    })
    assert "Order-flow imbalance" in text
    assert "OI-price-volume" in text

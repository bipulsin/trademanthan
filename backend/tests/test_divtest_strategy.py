from backend.services.divtest.candles import generate_demo_candles
from backend.services.divtest.strategy import run_strategy, analyze_trades, compute_macd


def test_macd_histogram_finite():
    closes = [100 + i * 0.1 + ((-1) ** i) for i in range(80)]
    m = compute_macd(closes)
    ready = [v for v in m["histogram"] if v is not None]
    assert len(ready) > 30
    assert all(v == v for v in ready)  # not NaN


def test_run_strategy_on_demo():
    candles = generate_demo_candles("2026-08", "15min", 7)
    out = run_strategy(candles, {"instrument": "X", "timeframe": "15min"})
    assert out["meta"]["candle_count"] > 100
    assert isinstance(out["trades"], list)
    # planted pattern should produce at least one trade
    assert len(out["trades"]) >= 1
    a = analyze_trades(out["trades"])
    assert a["total_trades"] == len(out["trades"])

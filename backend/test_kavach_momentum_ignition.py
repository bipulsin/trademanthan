"""Tests for Kavach Momentum Ignition Phase 1 scoring helpers."""
from backend.services.kavach_momentum_ignition import (
    classify_oi_price_volume,
    coincident_confirmation,
    get_fii_dii_multiplier,
)
from backend.services.upstox_market_feed import (
    _depth_from_mff,
    _extract_feed_fields,
)


def test_depth_imbalance_bullish():
    mff = {"marketLevel": {"bidAskQuote": [{"bidQ": 500, "askQ": 100}, {"bidQ": 300, "askQ": 150}]}}
    bid, ask, ratio = _depth_from_mff(mff)
    assert bid == 800
    assert ask == 250
    assert ratio > 1.0


def test_extract_feed_fields_tbq_tsq():
    feed = {
        "fullFeed": {
            "marketFF": {
                "oi": 100000,
                "ltpc": {"ltp": 250.5},
                "tbq": 9000,
                "tsq": 3000,
                "marketLevel": {"bidAskQuote": [{"bidQ": 400, "askQ": 200}]},
            }
        }
    }
    parsed = _extract_feed_fields(feed)
    assert parsed["oi"] == 100000
    assert parsed["ltp"] == 250.5
    assert parsed["tbq"] == 9000
    assert parsed["tsq"] == 3000
    assert parsed["pressure_ratio"] == 3.0


def test_classify_long_buildup():
    label, score = classify_oi_price_volume(0.25, 500, 1.5, "BULL")
    assert label == "LONG_BUILDUP"
    assert score >= 85


def test_fii_multiplier_defaults():
    assert get_fii_dii_multiplier("2099-01-01") == 1.0


def test_coincident_confirmation_bull():
    candles = []
    for i in range(8):
        base = 100 + i * 0.1
        candles.append({
            "open": base, "high": base + 0.5, "low": base - 0.1,
            "close": base + 0.45, "volume": 1000 + i * 200,
        })
    score, meta = coincident_confirmation(candles, "BULL")
    assert score > 0
    assert meta.get("one_sided_bars", 0) >= 1

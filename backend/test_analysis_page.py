"""Tests for Analysis page trend, RSI, RS, and patterns."""
from backend.services.analysis_page.compute import (
    classify_trend,
    detect_bullish_engulfing,
    detect_hammer,
    detect_piercing,
    rs_vs_ma50,
    rsi_zone,
    weekly_rsi_zone,
    wilder_rsi,
)


def _c(ts, o, h, l, cl):
    return {"timestamp": ts, "open": o, "high": h, "low": l, "close": cl}


def test_trend_bullish_hh_hl():
    prior = [_c(f"2026-01-{d:02d}", 100, 101, 99, 100.5) for d in range(1, 6)]
    recent = [_c(f"2026-01-{d:02d}", 102, 108, 101.5, 107) for d in range(6, 11)]
    assert classify_trend(prior + recent) == "Bullish"


def test_trend_bearish_lh_ll():
    prior = [_c(f"2026-01-{d:02d}", 100, 110, 95, 105) for d in range(1, 6)]
    recent = [_c(f"2026-01-{d:02d}", 90, 92, 80, 85) for d in range(6, 11)]
    assert classify_trend(prior + recent) == "Bearish"


def test_trend_sideways_approx_equal():
    bars = [_c(f"2026-01-{d:02d}", 100, 101, 99, 100) for d in range(1, 11)]
    assert classify_trend(bars) == "Sideways"


def test_rsi_zones():
    assert rsi_zone(66) == "OverBought"
    assert rsi_zone(34) == "OverSold"
    assert rsi_zone(35) == "Mid"
    assert rsi_zone(65) == "Mid"
    assert rsi_zone(50) == "Mid"


def test_wilder_rsi_rising_series_overbought():
    closes = [100.0 + i for i in range(20)]
    v = wilder_rsi(closes)
    assert v is not None and v > 65
    rsi, zone = weekly_rsi_zone(
        [_c(f"2025-{(i // 28) + 1:02d}-{(i % 28) + 1:02d}", c, c, c, c) for i, c in enumerate(closes)]
    )
    assert zone == "OverBought"


def test_rs_above_below_mid():
    idx = [_c(f"2026-03-{(i % 28) + 1:02d}", 100, 100, 100, 100) for i in range(55)]
    # fix dates properly
    from datetime import date, timedelta

    d0 = date(2026, 1, 2)
    idx = []
    stock_above = []
    stock_below = []
    stock_mid = []
    for i in range(55):
        d = (d0 + timedelta(days=i)).isoformat()
        idx.append(_c(d, 100, 100, 100, 100.0))
        stock_above.append(_c(d, 10, 10, 10, 10.0 + i * 0.05))
        stock_below.append(_c(d, 10, 10, 10, 10.0 - i * 0.02))
        stock_mid.append(_c(d, 10, 10, 10, 10.0))
    _, _, lab_a = rs_vs_ma50(stock_above, idx)
    _, _, lab_b = rs_vs_ma50(stock_below, idx)
    _, _, lab_m = rs_vs_ma50(stock_mid, idx)
    assert lab_a == "Above"
    assert lab_b == "Below"
    assert lab_m == "Mid"


def test_bullish_engulfing():
    prev = _c("2026-01-01", 110, 111, 100, 101)
    curr = _c("2026-01-02", 99, 120, 98, 115)
    assert detect_bullish_engulfing(prev, curr) is True


def test_hammer_and_not_engulfing():
    hammer = _c("2026-01-02", 102, 103, 90, 101)
    assert detect_hammer(hammer) is True
    prev = _c("2026-01-01", 100, 101, 99, 100.2)
    assert detect_bullish_engulfing(prev, hammer) is False


def test_piercing():
    prev = _c("2026-01-01", 110, 111, 100, 101)
    curr = _c("2026-01-02", 99, 108, 98, 107)
    assert detect_piercing(prev, curr) is True

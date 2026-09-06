"""Tests for Analysis page trend, RSI, RS, and patterns."""
from backend.services.analysis_page.compute import (
    ACTION_BUY,
    ACTION_NO_TRADE,
    ACTION_SELL,
    ACTION_SELL_EXHAUSTION,
    ACTION_WATCH,
    PATTERN_BEARISH_ENGULFING,
    PATTERN_BEARISH_HARAMI,
    PATTERN_BULLISH_ENGULFING,
    PATTERN_BULLISH_HARAMI,
    PATTERN_DARK_CLOUD,
    PATTERN_HAMMER,
    PATTERN_PIERCING,
    PATTERN_SHOOTING_STAR,
    classify_action,
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


def test_trend_0p1pct_band_treats_0p38pct_as_lower_high():
    """SONACOMS-style: 821.50 vs 824.60 is ~0.38% — not equal at 0.1% band."""
    prior = [_c(f"2026-01-{d:02d}", 820, 824.60, 810, 822) for d in range(1, 6)]
    recent = [_c(f"2026-01-{d:02d}", 818, 821.50, 800, 805) for d in range(6, 11)]
    assert classify_trend(prior + recent) == "Bearish"


def test_trend_within_0p1pct_still_approx_equal():
    prior = [_c(f"2026-01-{d:02d}", 100, 100.00, 99.00, 100) for d in range(1, 6)]
    recent = [_c(f"2026-01-{d:02d}", 100, 100.05, 99.05, 100) for d in range(6, 11)]
    assert classify_trend(prior + recent) == "Sideways"


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


def _act(**kwargs):
    defaults = dict(
        weekly_trend="Bullish",
        daily_trend="Bullish",
        weekly_rsi_zone="Mid",
        rs_ma50="Above",
        patterns=[PATTERN_BULLISH_ENGULFING],
    )
    defaults.update(kwargs)
    return classify_action(**defaults)


def test_action_no_pattern_always_no_trade():
    assert _act(patterns=[]) == ACTION_NO_TRADE
    assert _act(patterns=["—"]) == ACTION_NO_TRADE
    assert _act(weekly_trend="Bullish", rs_ma50="Below", patterns=[]) == ACTION_NO_TRADE


def test_action_buy():
    assert _act(patterns=[PATTERN_HAMMER]) == ACTION_BUY
    assert _act(patterns=[PATTERN_PIERCING]) == ACTION_BUY
    assert _act(patterns=[PATTERN_BULLISH_ENGULFING, PATTERN_DARK_CLOUD]) == ACTION_BUY


def test_action_sell():
    assert (
        _act(
            weekly_trend="Bearish",
            daily_trend="Bearish",
            rs_ma50="Below",
            patterns=[PATTERN_SHOOTING_STAR],
        )
        == ACTION_SELL
    )


def test_action_sell_exhaustion():
    assert (
        _act(
            weekly_trend="Bullish",
            weekly_rsi_zone="OverBought",
            rs_ma50="Above",
            patterns=[PATTERN_BEARISH_ENGULFING],
        )
        == ACTION_SELL_EXHAUSTION
    )


def test_action_watch_harami_and_bull_rs_down():
    assert _act(patterns=[PATTERN_BULLISH_HARAMI], rs_ma50="Mid") == ACTION_WATCH
    assert _act(patterns=[PATTERN_BEARISH_HARAMI], rs_ma50="Above") == ACTION_WATCH
    assert (
        _act(
            weekly_trend="Bullish",
            rs_ma50="Below",
            weekly_rsi_zone="Mid",
            patterns=[PATTERN_DARK_CLOUD],
        )
        == ACTION_WATCH
    )


def test_action_else_no_trade():
    assert (
        _act(
            weekly_trend="Sideways",
            daily_trend="Bullish",
            rs_ma50="Mid",
            patterns=[PATTERN_DARK_CLOUD],
        )
        == ACTION_NO_TRADE
    )


def test_action_ifs_order_buy_before_watch():
    assert (
        _act(
            weekly_trend="Bullish",
            rs_ma50="Above",
            patterns=[PATTERN_HAMMER, PATTERN_BULLISH_HARAMI],
        )
        == ACTION_BUY
    )


def test_should_refresh_weekly_friday_only():
    from datetime import datetime

    from backend.services.analysis_page.job import should_refresh_weekly
    from backend.services.market_holiday import IST

    fri = IST.localize(datetime(2026, 9, 4, 17, 0))
    mon = IST.localize(datetime(2026, 9, 7, 17, 0))
    thu = IST.localize(datetime(2026, 9, 3, 17, 0))
    assert should_refresh_weekly(now=fri) is True
    assert should_refresh_weekly(now=mon) is False
    assert should_refresh_weekly(now=thu) is False


def test_compute_row_weekday_skips_weekly_recompute():
    from backend.services.analysis_page.job import compute_row

    daily = [_c(f"2026-01-{d:02d}", 100, 101, 99, 100.5) for d in range(1, 6)] + [
        _c(f"2026-01-{d:02d}", 102, 108, 101.5, 107) for d in range(6, 11)
    ]
    weekly_would_be_bear = [_c(f"2025-01-{d:02d}", 100, 110, 95, 105) for d in range(1, 6)] + [
        _c(f"2025-02-{d:02d}", 90, 92, 80, 85) for d in range(1, 6)
    ]
    meta = {"symbol": "ABC", "sector": "IT", "sector_index": "NIFTY IT"}
    prior = {"weekly_trend": "Bullish", "weekly_rsi": 55.0, "weekly_rsi_zone": "Mid"}
    row = compute_row(
        meta=meta,
        daily=daily,
        weekly=weekly_would_be_bear,
        index_daily=[],
        refresh_weekly=False,
        prior_weekly=prior,
    )
    assert row["weekly_trend"] == "Bullish"
    assert row["weekly_rsi"] == 55.0
    assert row["weekly_rsi_zone"] == "Mid"
    assert row["daily_trend"] == "Bullish"


def test_compute_row_friday_recomputes_weekly():
    from backend.services.analysis_page.job import compute_row

    daily = [_c(f"2026-01-{d:02d}", 100, 101, 99, 100.5) for d in range(1, 6)] + [
        _c(f"2026-01-{d:02d}", 102, 108, 101.5, 107) for d in range(6, 11)
    ]
    weekly_bear = [_c(f"2025-01-{d:02d}", 100, 110, 95, 105) for d in range(1, 6)] + [
        _c(f"2025-02-{d:02d}", 90, 92, 80, 85) for d in range(1, 6)
    ]
    meta = {"symbol": "ABC"}
    prior = {"weekly_trend": "Bullish", "weekly_rsi": 55.0, "weekly_rsi_zone": "Mid"}
    row = compute_row(
        meta=meta,
        daily=daily,
        weekly=weekly_bear,
        index_daily=[],
        refresh_weekly=True,
        prior_weekly=prior,
    )
    assert row["weekly_trend"] == "Bearish"
    assert row["daily_trend"] == "Bullish"


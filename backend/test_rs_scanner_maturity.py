"""Tests for RS Scanner move-maturity classification and metrics."""
from backend.services.rs_scanner_maturity import (
    MATURITY_CONTINUING,
    MATURITY_EXTENDED,
    MATURITY_FRESH,
    MATURITY_STRETCHED,
    build_maturity_record,
    classify_maturity_tag,
    compute_consecutive_days,
    compute_yesterday_range_metrics,
)


def _daily_candle(date: str, o: float, h: float, l: float, c: float) -> dict:
    return {"timestamp": f"{date}T00:00:00+05:30", "open": o, "high": h, "low": l, "close": c}


def test_consecutive_days_increments_same_direction():
    assert compute_consecutive_days(True, 2) == 3


def test_consecutive_days_resets_on_direction_flip():
    assert compute_consecutive_days(False, 5) == 1


def test_fresh_on_first_appearance():
    assert classify_maturity_tag(1, 2.0) == MATURITY_FRESH


def test_extended_on_day2_large_range():
    assert classify_maturity_tag(2, 1.6) == MATURITY_EXTENDED


def test_continuing_on_day2_normal_range():
    assert classify_maturity_tag(2, 1.2) == MATURITY_CONTINUING


def test_stretched_overrides_continuing_at_day4():
    assert classify_maturity_tag(4, 1.0) == MATURITY_STRETCHED


def test_stretched_overrides_extended_at_day4():
    assert classify_maturity_tag(5, 2.0) == MATURITY_STRETCHED


def test_build_maturity_record_first_day_no_history():
    rec = build_maturity_record(
        symbol="RELIANCE",
        direction="bullish",
        rs_pct=1.5,
        yesterday_row=None,
        daily_range_pct=2.0,
        atr14_pct=1.0,
        range_vs_atr_ratio=2.0,
        session_date="2026-07-01",
    )
    assert rec["consecutive_days_on_list"] == 1
    assert rec["maturity_tag"] == MATURITY_FRESH


def test_build_maturity_record_increments_from_yesterday():
    rec = build_maturity_record(
        symbol="RELIANCE",
        direction="bullish",
        rs_pct=1.5,
        yesterday_row={"direction": "bullish", "consecutive_days_on_list": 1},
        daily_range_pct=2.0,
        atr14_pct=1.0,
        range_vs_atr_ratio=2.0,
        session_date="2026-07-02",
    )
    assert rec["consecutive_days_on_list"] == 2
    assert rec["maturity_tag"] == MATURITY_EXTENDED


def test_build_maturity_record_resets_on_direction_flip():
    rec = build_maturity_record(
        symbol="RELIANCE",
        direction="bearish",
        rs_pct=-1.5,
        yesterday_row={"direction": "bullish", "consecutive_days_on_list": 3},
        daily_range_pct=1.0,
        atr14_pct=1.0,
        range_vs_atr_ratio=1.0,
        session_date="2026-07-02",
    )
    assert rec["consecutive_days_on_list"] == 1
    assert rec["maturity_tag"] == MATURITY_FRESH


def test_compute_yesterday_range_metrics():
    candles = []
    for i in range(20):
        d = f"2026-06-{(i + 1):02d}"
        base = 100 + i
        candles.append(_daily_candle(d, base, base + 3, base - 2, base + 1))
    # Today's in-progress bar — should be ignored for "yesterday".
    candles.append(_daily_candle("2026-07-01", 120, 125, 118, 122))

    dr, atr_pct, ratio = compute_yesterday_range_metrics(candles, as_of_date="2026-07-01")
    assert dr > 0
    assert atr_pct > 0
    assert ratio > 0


def test_compute_yesterday_range_insufficient_history():
    candles = [_daily_candle("2026-07-01", 100, 105, 95, 102)]
    dr, atr_pct, ratio = compute_yesterday_range_metrics(candles, as_of_date="2026-07-01")
    assert dr == 0.0 and atr_pct == 0.0 and ratio == 0.0

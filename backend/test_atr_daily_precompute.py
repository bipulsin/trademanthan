"""Unit tests for ATR daily precompute helpers (no live Upstox / DB)."""
from backend.services.atr_daily_precompute import (
    next_nse_session_date,
    try_compute_yesterday_range_metrics,
)
from backend.services.rs_scanner_maturity import build_maturity_record
from datetime import date


def _daily_candle(d: str, o: float, h: float, l: float, c: float) -> dict:
    return {"timestamp": f"{d}T00:00:00+05:30", "open": o, "high": h, "low": l, "close": c}


def test_try_compute_returns_none_on_empty():
    assert try_compute_yesterday_range_metrics([], as_of_date="2026-07-24") is None


def test_try_compute_returns_none_on_short_history():
    candles = [_daily_candle("2026-07-23", 100, 105, 95, 102)]
    assert try_compute_yesterday_range_metrics(candles, as_of_date="2026-07-24") is None


def test_try_compute_positive_atr():
    candles = []
    for i in range(20):
        d = f"2026-06-{(i + 1):02d}"
        base = 100 + i
        candles.append(_daily_candle(d, base, base + 3, base - 2, base + 1))
    candles.append(_daily_candle("2026-07-01", 120, 125, 118, 122))
    out = try_compute_yesterday_range_metrics(candles, as_of_date="2026-07-01")
    assert out is not None
    dr, atr_pct, ratio = out
    assert dr > 0 and atr_pct > 0 and ratio > 0


def test_build_maturity_record_null_atr_not_zero():
    rec = build_maturity_record(
        symbol="WIPRO",
        direction="bullish",
        rs_pct=1.5,
        yesterday_row=None,
        daily_range_pct=None,
        atr14_pct=None,
        range_vs_atr_ratio=None,
        session_date="2026-07-24",
    )
    assert rec["atr14_pct"] is None
    assert rec["daily_range_pct"] is None
    assert rec["range_vs_atr_ratio"] is None
    assert rec["maturity_tag"] == "FRESH"


def test_build_maturity_record_rejects_zero_atr_sentinel():
    rec = build_maturity_record(
        symbol="WIPRO",
        direction="bullish",
        rs_pct=1.5,
        yesterday_row=None,
        daily_range_pct=0.0,
        atr14_pct=0.0,
        range_vs_atr_ratio=0.0,
        session_date="2026-07-24",
    )
    assert rec["atr14_pct"] is None


def test_next_nse_session_skips_weekend():
    # Friday 2026-07-24 -> Monday 2026-07-27 (unless holiday table lists it)
    nxt = next_nse_session_date(date(2026, 7, 24))
    assert nxt.weekday() < 5
    assert nxt > date(2026, 7, 24)

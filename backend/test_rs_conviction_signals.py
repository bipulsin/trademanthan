"""Unit tests for RS conviction signal components."""
from datetime import datetime, timedelta

import pytz

from backend.services.kavach_volume import last_closed_bar_index
from backend.services.rs_conviction_config import DEFAULTS
from backend.services.rs_conviction_signals import (
    accumulation_signal,
    compute_symbol_signals,
    normalized_vwap_slope,
    whipsaw_cross_count,
)

IST = pytz.timezone("Asia/Kolkata")


def _bar(day: str, minutes_from_open: int, close: float, volume: float = 1000.0) -> dict:
    base = IST.localize(datetime.strptime(day, "%Y-%m-%d").replace(hour=9, minute=15))
    dt = base + timedelta(minutes=minutes_from_open)
    o = close * 0.999
    h = close * 1.001
    l = close * 0.998
    return {
        "timestamp": dt.isoformat(),
        "open": o,
        "high": h,
        "low": l,
        "close": close,
        "volume": volume,
    }


def _session_candles(day: str, n_today: int, *, close_start: float = 100.0, drift: float = 0.0) -> list:
    """Prior-day padding + today's session bars (all closed if test runs after last bar)."""
    y, m, d = map(int, day.split("-"))
    prev_dt = datetime(y, m, d) - timedelta(days=1)
    prev_day = prev_dt.strftime("%Y-%m-%d")
    candles = []
    for i in range(40):
        candles.append(_bar(prev_day, i * 5, 100.0 + i * 0.01, 800))
    price = close_start
    for i in range(n_today):
        price += drift
        candles.append(_bar(day, i * 5, price, 1200 if i % 3 == 0 else 900))
    return candles


def test_compute_symbol_signals_empty():
    out = compute_symbol_signals(None, side="BULL", atr_daily_pct=1.5, cfg=DEFAULTS)
    assert out["slope_component"] == 0.0
    assert out["accum_active"] is False
    assert out["whipsaw_cross_count"] == 0


def test_normalized_vwap_slope_positive_on_uptrend():
    day = datetime.now(IST).strftime("%Y-%m-%d")
    candles = _session_candles(day, 30, close_start=100.0, drift=0.15)
    closed = last_closed_bar_index(candles)
    assert closed >= 0
    slope = normalized_vwap_slope(candles, atr_daily_pct=1.0, cfg=DEFAULTS)
    assert slope > 0.0


def test_whipsaw_cross_count_zero_on_smooth_trend():
    day = datetime.now(IST).strftime("%Y-%m-%d")
    candles = _session_candles(day, 24, close_start=100.0, drift=0.05)
    assert whipsaw_cross_count(candles) == 0


def test_accumulation_signal_two_of_three():
    day = datetime.now(IST).strftime("%Y-%m-%d")
    candles = _session_candles(day, 20, close_start=100.0, drift=0.0)
    # Spike volume on recent bars for rel-vol hit
    for c in candles[-8:]:
        c["volume"] = 5000
    score, active, low_conf = accumulation_signal(candles, "BULL", DEFAULTS)
    assert score >= 40.0
    assert low_conf is True

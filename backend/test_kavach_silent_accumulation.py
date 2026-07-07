"""Tests for standalone silent accumulation signal."""
from backend.services.kavach_silent_accumulation import (
    detect_silent_accumulation_1m,
    walk_forward_first_fire,
)


def _bar(i: int, close: float, oi: int, vol: int = 1000) -> dict:
    return {
        "candle_time": f"2026-07-07T09:{i:02d}:00+05:30",
        "open": close,
        "high": close + 0.05,
        "low": close - 0.05,
        "close": close,
        "oi_open": oi,
        "oi_close": oi,
        "volume": vol,
    }


def test_silent_accumulation_fires_on_flat_price_rising_oi():
    bars = []
    oi = 1_000_000
    for i in range(20):
        oi += 5000 if i > 0 else 0
        bars.append(_bar(i, 1000.0 + (i % 2) * 0.02, oi, vol=1200))
    res = detect_silent_accumulation_1m(bars, atr_daily_pct=2.0, window=15)
    assert res["checks"]["price_quiet"] is True
    assert res["checks"]["oi_rising"] is True
    assert res["active"] is True


def test_silent_accumulation_rejects_large_price_move():
    bars = []
    oi = 1_000_000
    for i in range(20):
        oi += 8000
        bars.append(_bar(i, 1000.0 + i * 5.0, oi, vol=1500))
    res = detect_silent_accumulation_1m(bars, atr_daily_pct=2.0, window=15)
    assert res["active"] is False


def test_walk_forward_returns_first_fire():
    bars = []
    oi = 1_000_000
    for i in range(25):
        if i > 0:
            oi += 6000
        bars.append(_bar(i, 500.0, oi, vol=1100))
    hit = walk_forward_first_fire(bars, atr_daily_pct=2.0, window=15)
    assert hit is not None
    assert hit.get("active") is True

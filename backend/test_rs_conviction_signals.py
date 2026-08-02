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


def test_ema10_10min_seeded_from_prior_session_no_cold_start():
    """Prior-session seed → EMA10 available before 10 same-session 10m bars."""
    from backend.services.rs_conviction_signals import ema10_10min
    from backend.services.vajra.indicators import ema_series

    day = "2026-07-30"
    # 40 prior 5m bars → 20 prior 10m closes (≥10); only 4 today 5m → 2 today 10m
    candles = _session_candles(day, 4, close_start=5000.0, drift=1.0)
    e = ema10_10min(candles)
    assert e is not None

    # Without prior history, same today bars alone must still return None (legacy)
    today_only = [c for c in candles if c["timestamp"].startswith(day)]
    assert ema10_10min(today_only) is None

    # Seeded value equals recursive continuation from prior final EMA10
    from backend.services.rs_conviction_signals import (
        _aggregate_10m_closes,
        _ema_seeded,
        _prior_session_10m_closes,
        _today_slice,
    )

    today, first = _today_slice(candles)
    prior = _prior_session_10m_closes(candles, first)
    seed = ema_series(prior, 10)[-1]
    expected = _ema_seeded(_aggregate_10m_closes(today), 10, seed)[-1]
    assert abs(e - expected) < 1e-9


def test_ema10_10min_cold_start_when_no_prior():
    from backend.services.rs_conviction_signals import ema10_10min

    day = "2026-07-30"
    # 20 today 5m → 10 today 10m, no prior
    candles = []
    for i in range(20):
        candles.append(_bar(day, i * 5, 100.0 + i * 0.1, 1000))
    e = ema10_10min(candles)
    assert e is not None
    assert ema10_10min(candles[:10]) is None  # only 5 ten-min bars

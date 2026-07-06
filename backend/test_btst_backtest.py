"""Unit tests for BTST gate and indicator pure functions."""
from datetime import date

from backend.services.btst_backtest.gates import (
    check_cpr_gate,
    check_hull_gate,
    check_liquidity_gate,
    check_rsi_gate,
    check_supertrend_gate,
)
from backend.services.btst_backtest.indicators import compute_cpr


def test_cpr_formula():
    pivot, tc, bc = compute_cpr(100, 80, 90)
    assert pivot == (100 + 80 + 90) / 3
    assert bc == 90
    assert tc == pivot + (pivot - bc)


def test_cpr_gate_bullish():
    ohlc = {"high": 110, "low": 90, "close": 100}
    pivot, _, _ = compute_cpr(110, 90, 100)
    passed, _, _tc, _bc = check_cpr_gate("bullish", pivot + 1, ohlc)
    assert passed is True
    passed2, _, _, _ = check_cpr_gate("bullish", pivot - 1, ohlc)
    assert passed2 is False


def test_cpr_gate_bearish():
    ohlc = {"high": 110, "low": 90, "close": 100}
    pivot, _, _ = compute_cpr(110, 90, 100)
    assert check_cpr_gate("bearish", pivot - 1, ohlc)[0] is True
    assert check_cpr_gate("bearish", pivot + 1, ohlc)[0] is False


def test_liquidity_gate():
    assert check_liquidity_gate(600_000, 500_000) is True
    assert check_liquidity_gate(400_000, 500_000) is False


def _m5_bar(trade_date: date, hh: int, mm: int, close: float, vol: float = 1000) -> dict:
    return {
        "timestamp": f"{trade_date.isoformat()}T{hh:02d}:{mm:02d}:00+05:30",
        "open": close,
        "high": close + 1,
        "low": close - 1,
        "close": close,
        "volume": vol,
    }


def test_rsi_gate_bullish_band():
    d = date(2026, 7, 1)
    candles = []
    price = 100.0
    for i in range(20):
        mm = 15 + (i * 5) % 60
        hh = 9 + (15 + i * 5) // 60
        price += 0.3
        candles.append(_m5_bar(d, hh, mm, price))
    passed, rsi = check_rsi_gate("bullish", candles, d, "14:45", bull_min=50, bull_max=80)
    assert rsi is not None
    assert isinstance(passed, bool)


def test_supertrend_and_hull_smoke():
    d = date(2026, 7, 1)
    candles = []
    p = 50.0
    for i in range(40):
        mm = (15 + i * 5) % 60
        hh = 9 + (15 + i * 5) // 60
        p += 0.2
        candles.append(_m5_bar(d, hh, mm, p))
    st_pass, _ = check_supertrend_gate(candles, d, "15:15", period=10, multiplier=3.0)
    hull_pass, _, rising = check_hull_gate(candles, d, "15:15", length=32)
    assert isinstance(st_pass, bool)
    assert isinstance(hull_pass, bool)
    assert isinstance(rising, bool)

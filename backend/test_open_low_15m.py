"""Unit tests for Open-Low 15m backtest logic."""
from __future__ import annotations

from datetime import date

import pytz

from backend.services.open_low_15m.simulate import detect_setup, simulate_trade

IST = pytz.timezone("Asia/Kolkata")


def _bar(ts: str, o, h, l, c, v=1000):
    return {"timestamp": ts, "open": o, "high": h, "low": l, "close": c, "volume": v}


def _warmup_bars(session: date, n: int = 30):
    """Prior-session 15m bars for indicator warmup (uptrend)."""
    out = []
    base = 90.0
    for i in range(n):
        p = base + i * 0.3
        minute = 15 + (i % 4) * 15
        out.append(_bar(f"2026-07-21T09:{minute:02d}:00+05:30", p, p + 1.2, p - 0.2, p + 0.8))
    return out


def test_open_low_setup_and_tp1_exit():
    session = date(2026, 7, 23)
    warmup = _warmup_bars(session)
    # First 15m: open=low at 100, high 102
    first = _bar("2026-07-23T09:15:00+05:30", 100, 102, 100, 101.5)
    # Entry bar breaks high
    entry_bar = _bar("2026-07-23T09:30:00+05:30", 101, 103, 100.5, 102.5)
    exit_bar = _bar("2026-07-23T09:45:00+05:30", 102.5, 104, 102, 103.5)
    candles = warmup + [first, entry_bar, exit_bar]

    setup = detect_setup(
        symbol="TEST",
        future_symbol="TEST FUT",
        instrument_key="NSE_FO|TEST",
        session_date=session,
        candles_15m=candles,
        prev_close=99.0,
        daily_closes_before=[90 + i * 0.5 for i in range(15)],
        lot_size=1000,
    )
    assert setup is not None
    assert setup["sl_type"] == "primary"
    assert setup["entry_trigger"] == 102.0

    trade = simulate_trade(setup, candles, "TP1")
    assert trade is not None
    assert trade["tp_hit"] is True
    assert trade["r_realized"] == 1.0


def test_gap_exclusion():
    session = date(2026, 7, 23)
    first = _bar("2026-07-23T09:15:00+05:30", 110, 112, 110, 111)
    setup = detect_setup(
        symbol="TEST",
        future_symbol="TEST FUT",
        instrument_key="NSE_FO|TEST",
        session_date=session,
        candles_15m=[first],
        prev_close=100.0,
        daily_closes_before=[95] * 15,
        lot_size=200,
    )
    assert setup is None

"""HA Momentum engine: SL skip, large-candle stop, EMA cross long."""
from datetime import datetime, timedelta

import pandas as pd
import pytz

from backtest.engine import add_indicators, long_signal, simulate_trade, short_signal


IST = pytz.timezone("Asia/Kolkata")


def _bar(ts, o, h, l, c):
    return {
        "timestamp": ts.isoformat(),
        "open": o,
        "high": h,
        "low": l,
        "close": c,
        "volume": 1,
    }


def test_large_candle_uses_prev_low_and_skips_if_sl_over_5k():
    start = IST.localize(datetime(2026, 8, 19, 10, 0, 0))
    rows = []
    px = 100.0
    for i in range(80):
        ts = start + timedelta(minutes=15 * i)
        rows.append(_bar(ts, px, px + 0.2, px - 0.2, px))
        px += 0.05
    # previous small, current large (>0.3%)
    ts = start + timedelta(minutes=15 * 80)
    rows.append(_bar(ts, 200.0, 200.2, 199.8, 200.0))
    ts2 = start + timedelta(minutes=15 * 81)
    rows.append(_bar(ts2, 200.0, 210.0, 190.0, 205.0))  # ~10% range
    df = pd.DataFrame(rows)
    df["ts"] = pd.to_datetime(df["timestamp"], utc=True).dt.tz_convert("Asia/Kolkata")
    df["ema5"] = df["ema15"] = df["ema50"] = 1.0
    df["macd_hist"] = 1.0
    df["adx14"] = 25.0
    i = len(df) - 1
    sim = simulate_trade(df, i, "LONG", lot_qty=1000)
    assert sim["sl_used_prev_candle"] is True
    assert sim["skipped"] is True
    assert sim["reason"] == "SL_EXCEEDS_5K"


def test_small_candle_sl_is_entry_low():
    start = IST.localize(datetime(2026, 8, 19, 10, 0, 0))
    rows = []
    for i in range(5):
        ts = start + timedelta(minutes=15 * i)
        rows.append(_bar(ts, 100.0, 100.1, 99.95, 100.05))
    df = pd.DataFrame(rows)
    df["ts"] = pd.to_datetime(df["timestamp"], utc=True).dt.tz_convert("Asia/Kolkata")
    sim = simulate_trade(df, 4, "LONG", lot_qty=1)
    assert sim["sl_used_prev_candle"] is False
    assert sim["sl_price"] == 99.95
    assert sim["skipped"] is False


def test_long_signal_requires_ema5_cross_up():
    prev = pd.Series(
        {"ema5": 10.0, "ema15": 10.1, "ema50": 9.0, "macd_hist": 0.1, "adx14": 21.0, "adx14_prev": 20.0}
    )
    row = pd.Series(
        {"ema5": 10.2, "ema15": 10.1, "ema50": 9.0, "macd_hist": 0.1, "adx14": 22.0, "adx14_prev": 21.0}
    )
    assert long_signal(row, prev) is True
    assert short_signal(row, prev) is False
    row2 = row.copy()
    row2["macd_hist"] = -0.1
    assert long_signal(row2, prev) is False


def test_add_indicators_has_required_columns():
    start = IST.localize(datetime(2026, 7, 17, 9, 15, 0))
    rows = []
    px = 100.0
    for i in range(90):
        ts = start + timedelta(minutes=15 * i)
        rows.append(_bar(ts, px, px + 1, px - 1, px + 0.2))
        px += 0.1
    df = pd.DataFrame(rows)
    out = add_indicators(df)
    for col in ("ema5", "ema15", "ema50", "macd_hist", "adx14", "adx14_prev"):
        assert col in out.columns
        assert out[col].notna().sum() > 10

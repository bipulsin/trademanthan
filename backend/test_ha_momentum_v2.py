"""HA Momentum v2: fixed % SL, cutoff skip, Nifty VWAP maps."""
from datetime import datetime, timedelta

import pandas as pd
import pytz

from backtest.engine_v2 import nifty_session_vwap, simulate_trade

IST = pytz.timezone("Asia/Kolkata")


def _bar(ts, o, h, l, c, vol=1):
    return {"timestamp": ts.isoformat(), "open": o, "high": h, "low": l, "close": c, "volume": vol}


def test_fixed_pct_sl_long_no_skip():
    start = IST.localize(datetime(2026, 8, 19, 10, 0, 0))
    rows = []
    for i in range(5):
        ts = start + timedelta(minutes=15 * i)
        rows.append(_bar(ts, 100.0, 100.1, 99.95, 100.05))
    df = pd.DataFrame(rows)
    df["ts"] = pd.to_datetime(df["timestamp"], utc=True).dt.tz_convert("Asia/Kolkata")
    sim = simulate_trade(
        df, 4, "LONG", lot_qty=20000, rr_t1=1.5, rr_t2=2.0, use_fixed_sl=True, fixed_sl_pct=0.004, sl_cap=5000
    )
    assert sim["skipped"] is False
    assert sim["sl_logic_used"] == "FIXED_PCT"
    entry = sim["entry_price"]
    assert abs(sim["sl_price"] - round(entry * 0.996, 2)) < 0.02
    assert sim["sl_rs"] > 5000


def test_nifty_vwap_resets_daily():
    d1 = IST.localize(datetime(2026, 8, 18, 9, 15, 0))
    d2 = IST.localize(datetime(2026, 8, 19, 9, 15, 0))
    candles = [
        _bar(d1, 100, 110, 90, 100, vol=10),
        _bar(d1 + timedelta(minutes=15), 100, 100, 100, 100, vol=10),
        _bar(d2, 200, 210, 190, 200, vol=10),
    ]
    closes, vwaps, used = nifty_session_vwap(candles)
    assert used is True
    ts_d2 = pd.Timestamp(d2)
    assert abs(vwaps[ts_d2] - 200.0) < 1.0

"""Unit tests for chart_choppiness Condition A/B."""
from __future__ import annotations

from datetime import datetime, timedelta

import pytz

from backend.services.chart_choppiness import (
    evaluate_chart_choppiness,
    body_cross_event,
    session_10m_ohlcv_vwap_ema5,
)

IST = pytz.timezone("Asia/Kolkata")


def _ts(h: int, m: int, day: str = "2026-07-31") -> str:
    y, mo, d = map(int, day.split("-"))
    return IST.localize(datetime(y, mo, d, h, m)).isoformat()


def _5m(ts: str, o: float, h: float, l: float, c: float, v: float = 1000.0) -> dict:
    return {"timestamp": ts, "open": o, "high": h, "low": l, "close": c, "volume": v}


def _pair_day_bars(ohlc_10m: list, day: str = "2026-07-31") -> list:
    """Build 5m pairs from 10m OHLC starting 09:15."""
    out = []
    base = IST.localize(datetime(int(day[:4]), int(day[5:7]), int(day[8:10]), 9, 15))
    for i, (o, h, l, c) in enumerate(ohlc_10m):
        t0 = base + timedelta(minutes=10 * i)
        t1 = t0 + timedelta(minutes=5)
        mid = (o + c) / 2
        out.append(_5m(t0.isoformat(), o, h, l, mid, 1000))
        out.append(_5m(t1.isoformat(), mid, h, l, c, 1000))
    return out


def test_body_cross_intra():
    assert body_cross_event(100, 110, 105) == "bullish"
    assert body_cross_event(110, 100, 105) == "bearish"
    assert body_cross_event(100, 102, 105) is None
    assert body_cross_event(110, 108, 105) is None


def test_bootstrap_both_directions_flags():
    # Force VWAP mid via equal volume around 100 — fabricate crosses by straddling ~100
    # Bar0: open 98 close 102 (bull), bar1: open 102 close 98 (bear) → bootstrap ON
    bars = [
        (98, 103, 97, 102),
        (102, 103, 97, 98),
        (99, 100, 98, 99),
        (99, 100, 98, 99),
        (99, 100, 98, 99),
    ]
    candles = _pair_day_bars(bars)
    ev = evaluate_chart_choppiness(candles, session_date="2026-07-31", symbol="T")
    assert ev.bootstrap_flagged is True
    assert len(ev.bootstrap_crosses) >= 2


def test_bootstrap_same_direction_does_not_flag():
    # Two bullish straddles only
    bars = [
        (98, 103, 97, 102),
        (98, 103, 97, 102),
        (102, 104, 101, 103),
        (103, 105, 102, 104),
        (104, 106, 103, 105),
    ]
    candles = _pair_day_bars(bars)
    ev = evaluate_chart_choppiness(candles, session_date="2026-07-31", symbol="T")
    assert ev.bootstrap_flagged is False


def test_same_dir_bootstrap_does_not_leak_into_rolling():
    """Same-direction bootstrap must not flip A ON right after bar 4 via lookback."""
    bars = [
        (98, 103, 97, 102),  # bull
        (98, 103, 97, 102),  # bull
        (102, 104, 101, 103),
        (103, 105, 102, 104),
        # post-bootstrap: stay above, no new crosses
        (104, 106, 103, 105),
        (105, 107, 104, 106),
        (106, 108, 105, 107),
        (107, 109, 106, 108),
    ]
    candles = _pair_day_bars(bars)
    ev = evaluate_chart_choppiness(candles, session_date="2026-07-31", symbol="T")
    assert ev.bootstrap_flagged is False
    # No Condition A ON after bootstrap from leaked same-dir crosses
    post = [t for t in ev.timeline if t.bar_idx >= 4]
    assert all(not t.cond_a_on for t in post), [t.note for t in post]


def test_rolling_exit_after_five_same_side():
    # Bootstrap: one bull + one bear → ON, then 5 same-side no-cross → OFF
    bars = [
        (98, 103, 97, 102),  # bull straddle
        (102, 103, 97, 98),  # bear straddle
        (90, 91, 89, 90),
        (90, 91, 89, 90),
        # after bootstrap — continue below VWAP
        (90, 91, 89, 90),
        (90, 91, 89, 90),
        (90, 91, 89, 90),
        (90, 91, 89, 90),
        (90, 91, 89, 90),
        (90, 91, 89, 90),
    ]
    candles = _pair_day_bars(bars)
    ev = evaluate_chart_choppiness(candles, session_date="2026-07-31", symbol="T")
    assert ev.bootstrap_flagged is True
    # After enough same-side bars, Condition A should turn off (unless B)
    a_states = [t.cond_a_on for t in ev.timeline]
    assert any(a_states[:4])  # ON during/after bootstrap
    assert a_states[-1] is False or ev.cond_b_final  # exited A, unless B holds


def test_session_bars_build():
    bars = [(100, 101, 99, 100.5)] * 6
    candles = _pair_day_bars(bars)
    s = session_10m_ohlcv_vwap_ema5(candles, "2026-07-31")
    assert len(s) == 6
    assert "vwap" in s[0] and "ema5" in s[0]

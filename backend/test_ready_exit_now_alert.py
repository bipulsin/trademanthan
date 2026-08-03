"""Unit tests for READY-card EXIT NOW alert (VWAP or EMA10 violation)."""
from __future__ import annotations

from datetime import datetime

import pytz

from backend.services.ready_exit_now_alert import evaluate_exit_now_alert

IST = pytz.timezone("Asia/Kolkata")


def _5m(h: int, m: int, o: float, hi: float, lo: float, c: float, v: float = 1000.0) -> dict:
    return {
        "timestamp": IST.localize(datetime(2026, 8, 3, h, m)).isoformat(),
        "open": o,
        "high": hi,
        "low": lo,
        "close": c,
        "volume": v,
    }


def _climb_then_dump():
    # Build enough 10m bars for EMA10 seed from same-session (≥10 pairs = 20×5m)
    bars = []
    px = 100.0
    # 09:15 .. through many bars climbing
    t = IST.localize(datetime(2026, 8, 3, 9, 15))
    for i in range(22):  # 11 ten-minute pairs
        from datetime import timedelta

        ts = t + timedelta(minutes=5 * i)
        o = px
        c = px + 0.5
        bars.append(
            {
                "timestamp": ts.isoformat(),
                "open": o,
                "high": c + 0.2,
                "low": o - 0.2,
                "close": c,
                "volume": 1000.0,
            }
        )
        px = c
    # Dump last pair below VWAP and well below EMA10
    from datetime import timedelta

    ts_a = t + timedelta(minutes=5 * 22)
    ts_b = t + timedelta(minutes=5 * 23)
    bars.append(
        {
            "timestamp": ts_a.isoformat(),
            "open": px,
            "high": px,
            "low": 95.0,
            "close": 95.5,
            "volume": 5000.0,
        }
    )
    bars.append(
        {
            "timestamp": ts_b.isoformat(),
            "open": 95.5,
            "high": 95.5,
            "low": 94.0,
            "close": 94.5,
            "volume": 5000.0,
        }
    )
    return bars


def test_long_exit_when_close_below_vwap_or_ema10():
    candles = _climb_then_dump()
    # bar_end of last pair = last 5m open + 5m
    now = IST.localize(datetime(2026, 8, 3, 11, 15, 1))
    r = evaluate_exit_now_alert("LONG", candles, now=now)
    assert r["active"] is True
    assert r["reason"] in ("vwap", "ema10", "both")
    assert "EXIT NOW" in r["banner"]


def test_long_inactive_when_above_both():
    candles = [
        _5m(9, 15, 100, 101, 99, 100.5),
        _5m(9, 20, 100.5, 102, 100, 101.5),
        _5m(9, 25, 101.5, 103, 101, 102),
        _5m(9, 30, 102, 104, 102, 103),
    ]
    # Not enough for EMA10 — only VWAP check; close above VWAP → inactive if no EMA
    now = IST.localize(datetime(2026, 8, 3, 9, 35, 1))
    r = evaluate_exit_now_alert("LONG", candles, now=now)
    # Without EMA10, VWAP-only: last close 103 should be above session VWAP
    assert r["active"] is False or r["reason"] is None


def test_short_exit_when_close_above_vwap():
    candles = [
        _5m(9, 15, 100, 101, 99, 100),
        _5m(9, 20, 100, 100, 98, 98),
        _5m(9, 25, 98, 99, 97, 97.5),
        _5m(9, 30, 97.5, 98, 96, 96.5),
        _5m(9, 35, 96.5, 99, 96, 98.5),
        _5m(9, 40, 98.5, 101, 98, 100.5),
    ]
    now = IST.localize(datetime(2026, 8, 3, 9, 45, 1))
    r = evaluate_exit_now_alert("SHORT", candles, now=now)
    assert r["active"] is True
    assert r["reason"] in ("vwap", "ema10", "both")

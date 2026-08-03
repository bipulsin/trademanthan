"""Unit tests for hard VWAP-side READY gate (last closed 10m vs VWAP)."""
from __future__ import annotations

from datetime import datetime

import pytz

from backend.services.vwap_side_gate import (
    apply_vwap_side_gate,
    opposite_direction,
    vwap_side_ok,
)

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


def test_long_ok_when_closed_10m_above_vwap():
    # Pairs: 09:15+09:20 (bar_end 09:25), 09:25+09:30 (bar_end 09:35)
    candles = [
        _5m(9, 15, 100, 101, 99, 100.5),
        _5m(9, 20, 100.5, 102, 100, 101.5),
        _5m(9, 25, 101.5, 103, 101, 102),
        _5m(9, 30, 102, 103, 101.5, 102.5),
    ]
    now = IST.localize(datetime(2026, 8, 3, 9, 35, 1))
    r = vwap_side_ok("LONG", candles, now=now)
    assert r["ok"] is True
    assert r["detail"]["close"] == 102.5


def test_long_reject_when_closed_10m_below_vwap():
    # Dump on 09:35+09:40 pair; at 09:45:19 that 10m is closed (bar_end=09:45)
    candles = [
        _5m(9, 15, 100, 101, 99, 100),
        _5m(9, 20, 100, 102, 100, 101),
        _5m(9, 25, 101, 103, 101, 102),
        _5m(9, 30, 102, 104, 102, 103),
        _5m(9, 35, 103, 103, 99, 99.5),
        _5m(9, 40, 99.5, 100, 98, 98.5),
    ]
    now = IST.localize(datetime(2026, 8, 3, 9, 45, 19))
    r = vwap_side_ok("LONG", candles, now=now)
    assert r["ok"] is False
    assert r["reason"] == "vwap_side_gate_reject"
    assert r["detail"]["close"] == 98.5
    assert r["detail"]["close"] < r["detail"]["vwap"]


def test_short_ok_below_vwap():
    candles = [
        _5m(9, 15, 100, 101, 99, 100),
        _5m(9, 20, 100, 100, 98, 98),
        _5m(9, 25, 98, 99, 97, 97.5),
        _5m(9, 30, 97.5, 98, 96, 96.5),
    ]
    now = IST.localize(datetime(2026, 8, 3, 9, 35, 1))
    r = vwap_side_ok("SHORT", candles, now=now)
    assert r["ok"] is True


def test_apply_demotes_ready():
    candles = [
        _5m(9, 15, 100, 101, 99, 100),
        _5m(9, 20, 100, 102, 100, 101),
        _5m(9, 25, 101, 103, 101, 102),
        _5m(9, 30, 102, 104, 102, 103),
        _5m(9, 35, 103, 103, 99, 99.5),
        _5m(9, 40, 99.5, 100, 98, 98.5),
    ]
    now = IST.localize(datetime(2026, 8, 3, 9, 45, 19))
    stock = {
        "symbol": "TEST",
        "direction": "LONG",
        "trade_state": "READY",
        "trade_take_enabled": True,
    }
    out = apply_vwap_side_gate(stock, candles, now=now)
    assert out["demoted"] is True
    assert stock["trade_state"] == "WAIT FOR PULLBACK"
    assert stock["zone_downgrade"] == "vwap_side_gate_reject"


def test_opposite_direction():
    assert opposite_direction("LONG") == "SHORT"
    assert opposite_direction("SHORT") == "LONG"

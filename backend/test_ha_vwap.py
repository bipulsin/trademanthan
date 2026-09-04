"""HA, VWAP cross, top-2 volume, TP, dual exit, 09:45–12:45 window."""
from __future__ import annotations

from datetime import date, datetime

import pytest
import pytz

from backend.services.ha_vwap.indicators import crossed_above, heikin_ashi, macd_hist_series, session_vwap
from backend.services.ha_vwap.simulate import (
    in_signal_window,
    is_signal,
    select_top_volume,
    simulate_session,
)

IST = pytz.timezone("Asia/Kolkata")
SESSION = date(2026, 8, 3)


def _bar(hm: str, o, h, l, c, v=1000, ha=None, vwap=None, ema=None, hist=1.0):
    hour, minute = map(int, hm.split(":"))
    ts = IST.localize(datetime(SESSION.year, SESSION.month, SESSION.day, hour, minute))
    ha_c = ha if ha is not None else c
    return {
        "timestamp": ts.isoformat(),
        "open": o,
        "high": h,
        "low": l,
        "close": c,
        "volume": v,
        "bar_start": ts,
        "ha_close": ha_c,
        "vwap": vwap if vwap is not None else (o + h + l + c) / 4,
        "ema20": ema if ema is not None else ha_c - 1,
        "macd_hist": hist,
    }


def test_heikin_ashi_recursion():
    o, h, l, c = [10, 12], [12, 14], [9, 11], [11, 13]
    ho, hh, hl, hc = heikin_ashi(o, h, l, c)
    assert hc[0] == pytest.approx((10 + 12 + 9 + 11) / 4)
    assert ho[0] == pytest.approx((10 + 11) / 2)
    assert ho[1] == pytest.approx((ho[0] + hc[0]) / 2)
    assert hc[1] == pytest.approx((12 + 14 + 11 + 13) / 4)


def test_crossed_above_not_merely_above():
    assert crossed_above(100, 101, 102, 101) is True
    assert crossed_above(102, 101, 103, 101) is False  # already above
    assert crossed_above(100, 101, 100.5, 101) is False


def test_session_vwap_resets():
    h = [2, 2, 4]
    l = [1, 1, 3]
    c = [1.5, 1.5, 3.5]
    v = [10, 10, 10]
    sids = ["d1", "d1", "d2"]
    vw = session_vwap(h, l, c, v, sids)
    assert vw[0] == pytest.approx((2 + 1 + 1.5) / 3)
    assert vw[2] == pytest.approx((4 + 3 + 3.5) / 3)


def test_signal_window_945_to_1245():
    assert in_signal_window(_bar("09:35", 1, 1, 1, 1)) is False
    assert in_signal_window(_bar("09:45", 1, 1, 1, 1)) is True
    assert in_signal_window(_bar("12:45", 1, 1, 1, 1)) is True
    assert in_signal_window(_bar("12:55", 1, 1, 1, 1)) is False


def test_is_signal_requires_cross_ema_macd():
    prev = _bar("09:35", 100, 101, 99, 100, ha=100, vwap=101, ema=99, hist=1)
    cur = _bar("09:45", 100, 103, 100, 102, ha=102, vwap=101, ema=101, hist=0.5)
    assert is_signal(prev, cur) is True
    cur2 = dict(cur)
    cur2["macd_hist"] = 0
    assert is_signal(prev, cur2) is False
    cur3 = dict(cur)
    cur3["ha_close"] = 100.5
    cur3["ema20"] = 101
    assert is_signal(prev, cur3) is False


def test_top_2_by_volume():
    sigs = [
        {"symbol": "A", "volume": 10},
        {"symbol": "B", "volume": 50},
        {"symbol": "C", "volume": 30},
    ]
    top = select_top_volume(sigs, 2)
    assert [x["symbol"] for x in top] == ["B", "C"]


def test_tp_on_high_touch():
    a = [
        _bar("09:35", 100, 100, 100, 100, v=1, ha=99, vwap=100, ema=98, hist=1),
        _bar("09:45", 100, 100.2, 99.9, 100, v=5000, ha=101, vwap=100, ema=100, hist=1),
        _bar("09:55", 100.1, 101.0, 100.0, 100.5, v=10, ha=101, vwap=100, ema=100, hist=1),
        _bar("15:15", 100.5, 100.5, 100.5, 100.5, v=1, ha=101, vwap=100, ema=100, hist=1),
    ]
    trades = simulate_session(
        {"AAA": a},
        lots={"AAA": 1},
        instruments={"AAA": "fut"},
        keys={"AAA": "NSE_FO|AAA"},
        session_date=SESSION,
    )
    assert len(trades) == 1
    assert trades[0]["reason"] == "tp"
    entry = 100 * 1.0003
    assert trades[0]["entry"] == pytest.approx(round(entry, 4))
    assert trades[0]["exit"] == pytest.approx(round(entry * 1.008, 4))


def test_dual_exit_ha_below_vwap_and_ema():
    a = [
        _bar("09:35", 100, 100, 100, 100, v=1, ha=99, vwap=100, ema=98, hist=1),
        _bar("09:45", 100, 100.2, 99.9, 100, v=5000, ha=101, vwap=100, ema=100, hist=1),
        _bar("09:55", 100, 100.2, 99.0, 99.5, v=10, ha=99, vwap=100, ema=100, hist=1),
        _bar("15:15", 99.5, 99.5, 99.5, 99.5, v=1, ha=99, vwap=100, ema=100, hist=1),
    ]
    trades = simulate_session(
        {"AAA": a},
        lots={"AAA": 50},
        instruments={"AAA": "fut"},
        keys={"AAA": "k"},
        session_date=SESSION,
    )
    assert trades[0]["reason"] == "vwap_ema_exit"
    assert trades[0]["exit"] == pytest.approx(99.5)
    assert trades[0]["qty"] == 50
    assert trades[0]["pnl"] == pytest.approx((99.5 - 100 * 1.0003) * 50)


def test_max_two_concurrent_top_volume():
    def series(vol, high_later=100.2):
        return [
            _bar("09:35", 100, 100, 100, 100, v=1, ha=99, vwap=100, ema=98, hist=1),
            _bar("09:45", 100, 100.1, 99.9, 100, v=vol, ha=101, vwap=100, ema=100, hist=1),
            _bar("15:15", 100, high_later, 99.5, 100, v=1, ha=101, vwap=100, ema=100, hist=1),
        ]

    by = {
        "LOW": series(10),
        "MID": series(100),
        "HIGH": series(999),
    }
    trades = simulate_session(
        by,
        lots={k: 1 for k in by},
        instruments={k: "fut" for k in by},
        keys={k: k for k in by},
        session_date=SESSION,
    )
    syms = {t["symbol"] for t in trades}
    assert "HIGH" in syms and "MID" in syms
    assert "LOW" not in syms
    assert len(trades) == 2


def test_no_entry_before_945():
    a = [
        _bar("09:15", 100, 100, 100, 100, v=1, ha=99, vwap=100, ema=98, hist=1),
        _bar("09:25", 100, 100, 100, 100, v=9000, ha=101, vwap=100, ema=100, hist=1),
        _bar("15:15", 100, 100, 100, 100, v=1, ha=101, vwap=100, ema=100, hist=1),
    ]
    trades = simulate_session(
        {"AAA": a},
        lots={"AAA": 1},
        instruments={"AAA": "fut"},
        keys={"AAA": "k"},
        session_date=SESSION,
    )
    assert trades == []


def test_skip_entry_when_lot_missing():
    a = [
        _bar("09:35", 100, 100, 100, 100, v=1, ha=99, vwap=100, ema=98, hist=1),
        _bar("09:45", 100, 100.2, 99.9, 100, v=5000, ha=101, vwap=100, ema=100, hist=1),
        _bar("15:15", 100, 100.2, 99.5, 100, v=1, ha=101, vwap=100, ema=100, hist=1),
    ]
    trades = simulate_session(
        {"AAA": a},
        lots={},
        instruments={"AAA": "cash"},
        keys={"AAA": "NSE_EQ|AAA"},
        session_date=SESSION,
    )
    assert trades == []


def test_cash_qty_uses_fut_lot():
    a = [
        _bar("09:35", 100, 100, 100, 100, v=1, ha=99, vwap=100, ema=98, hist=1),
        _bar("09:45", 100, 100.2, 99.9, 100, v=5000, ha=101, vwap=100, ema=100, hist=1),
        _bar("09:55", 100.1, 101.0, 100.0, 100.5, v=10, ha=101, vwap=100, ema=100, hist=1),
        _bar("15:15", 100.5, 100.5, 100.5, 100.5, v=1, ha=101, vwap=100, ema=100, hist=1),
    ]
    trades = simulate_session(
        {"RELIANCE": a},
        lots={"RELIANCE": 250},
        instruments={"RELIANCE": "cash"},
        keys={"RELIANCE": "NSE_EQ|RELIANCE"},
        session_date=SESSION,
    )
    assert trades[0]["qty"] == 250
    entry = 100 * 1.0003
    assert trades[0]["pnl"] == pytest.approx((entry * 1.008 - entry) * 250, abs=0.02)


def test_macd_hist_series_length():
    closes = [float(i) for i in range(1, 120)]
    h = macd_hist_series(closes, 104, 48, 36)
    assert len(h) == len(closes)

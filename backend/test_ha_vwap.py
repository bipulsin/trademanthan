"""HA, CSV bar mapping, TP, VWAP+EMA exit, SuperTrend(10,3) exit."""
from __future__ import annotations

from datetime import date, datetime, time

import pytest
import pytz

from backend.services.ha_vwap.indicators import heikin_ashi, session_vwap, supertrend_series
from backend.services.ha_vwap.signals import bar_start_containing, parse_csv_datetime
from backend.services.ha_vwap.simulate import simulate_session

IST = pytz.timezone("Asia/Kolkata")
SESSION = date(2026, 8, 3)


def _bar(hm: str, o, h, l, c, v=1000, ha=None, vwap=None, ema=None, st=None):
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
        "st": st if st is not None else 0.0,
    }


def test_heikin_ashi_recursion():
    o, h, l, c = [10, 12], [12, 14], [9, 11], [11, 13]
    ho, hh, hl, hc = heikin_ashi(o, h, l, c)
    assert hc[0] == pytest.approx((10 + 12 + 9 + 11) / 4)
    assert ho[0] == pytest.approx((10 + 11) / 2)
    assert ho[1] == pytest.approx((ho[0] + hc[0]) / 2)
    assert hc[1] == pytest.approx((12 + 14 + 11 + 13) / 4)


def test_session_vwap_resets():
    h = [2, 2, 4]
    l = [1, 1, 3]
    c = [1.5, 1.5, 3.5]
    v = [10, 10, 10]
    sids = ["d1", "d1", "d2"]
    vw = session_vwap(h, l, c, v, sids)
    assert vw[0] == pytest.approx((2 + 1 + 1.5) / 3)
    assert vw[2] == pytest.approx((4 + 3 + 3.5) / 3)


def test_csv_time_maps_to_session_10m_bar_start():
    assert bar_start_containing(parse_csv_datetime("15-07-2026 9:45 am")) == time(9, 45)
    assert bar_start_containing(parse_csv_datetime("15-07-2026 11:35 am")) == time(11, 35)
    assert bar_start_containing(datetime(2026, 7, 15, 9, 48)) == time(9, 45)
    assert bar_start_containing(datetime(2026, 7, 15, 9, 54)) == time(9, 45)
    assert bar_start_containing(datetime(2026, 7, 15, 9, 55)) == time(9, 55)
    assert bar_start_containing(datetime(2026, 7, 15, 9, 15)) == time(9, 15)


def test_tp_on_high_touch_after_csv_entry():
    a = [
        _bar("09:45", 100, 100.2, 99.9, 100, v=5000, ha=101, vwap=100, ema=100, st=90),
        _bar("09:55", 100.1, 101.0, 100.0, 100.5, v=10, ha=101, vwap=100, ema=100, st=90),
        _bar("15:15", 100.5, 100.5, 100.5, 100.5, v=1, ha=101, vwap=100, ema=100, st=90),
    ]
    trades = simulate_session(
        {"AAA": a},
        lots={"AAA": 1},
        instruments={"AAA": "fut"},
        keys={"AAA": "NSE_FO|AAA"},
        session_date=SESSION,
        csv_entries=[("AAA", time(9, 45))],
    )
    assert len(trades) == 1
    assert trades[0]["reason"] == "tp"
    entry = 100 * 1.0003
    assert trades[0]["entry"] == pytest.approx(round(entry, 4))
    assert trades[0]["exit"] == pytest.approx(round(entry * 1.008, 4))


def test_dual_exit_ha_below_vwap_and_ema():
    a = [
        _bar("09:45", 100, 100.2, 99.9, 100, v=5000, ha=101, vwap=100, ema=100, st=90),
        _bar("09:55", 100, 100.2, 99.0, 99.5, v=10, ha=99, vwap=100, ema=100, st=90),
        _bar("15:15", 99.5, 99.5, 99.5, 99.5, v=1, ha=99, vwap=100, ema=100, st=90),
    ]
    trades = simulate_session(
        {"AAA": a},
        lots={"AAA": 50},
        instruments={"AAA": "fut"},
        keys={"AAA": "k"},
        session_date=SESSION,
        csv_entries=[("AAA", time(9, 45))],
    )
    assert trades[0]["reason"] == "vwap_ema"
    assert trades[0]["exit"] == pytest.approx(99.5)
    assert trades[0]["qty"] == 50
    assert trades[0]["pnl"] == pytest.approx((99.5 - 100 * 1.0003) * 50)


def test_supertrend_exit_uses_raw_close():
    a = [
        _bar("09:45", 100, 100.2, 99.9, 100, v=5000, ha=101, vwap=90, ema=90, st=90),
        _bar("09:55", 100, 100.2, 99.0, 99.4, v=10, ha=101, vwap=90, ema=90, st=99.5),
        _bar("15:15", 99.4, 99.4, 99.4, 99.4, v=1, ha=101, vwap=90, ema=90, st=99.5),
    ]
    trades = simulate_session(
        {"AAA": a},
        lots={"AAA": 1},
        instruments={"AAA": "fut"},
        keys={"AAA": "k"},
        session_date=SESSION,
        csv_entries=[("AAA", time(9, 45))],
    )
    assert trades[0]["reason"] == "supertrend"
    assert trades[0]["exit"] == pytest.approx(99.4)


def test_supertrend_series_length_and_below():
    n = 30
    highs = [10.0 + i * 0.1 for i in range(n)]
    lows = [9.0 + i * 0.1 for i in range(n)]
    closes = [9.5 + i * 0.1 for i in range(n)]
    # last bars dump so close falls through ST
    highs[-1], lows[-1], closes[-1] = 8.0, 5.0, 6.0
    st = supertrend_series(highs, lows, closes, 10, 3.0)
    assert len(st) == n
    assert st[-1] is not None
    assert closes[-1] < st[-1]


def test_no_scan_without_csv():
    a = [
        _bar("09:45", 100, 100.2, 99.9, 100, v=5000, ha=101, vwap=100, ema=100, st=90),
        _bar("15:15", 100, 100.2, 99.5, 100, v=1, ha=101, vwap=100, ema=100, st=90),
    ]
    trades = simulate_session(
        {"AAA": a},
        lots={"AAA": 1},
        instruments={"AAA": "fut"},
        keys={"AAA": "k"},
        session_date=SESSION,
        csv_entries=[],
    )
    assert trades == []


def test_skip_when_lot_missing():
    a = [
        _bar("09:45", 100, 100.2, 99.9, 100, v=5000, ha=101, vwap=100, ema=100, st=90),
        _bar("15:15", 100, 100.2, 99.5, 100, v=1, ha=101, vwap=100, ema=100, st=90),
    ]
    trades = simulate_session(
        {"AAA": a},
        lots={},
        instruments={"AAA": "fut"},
        keys={"AAA": "k"},
        session_date=SESSION,
        csv_entries=[("AAA", time(9, 45))],
    )
    assert trades == []


def test_time_exit_1515():
    a = [
        _bar("09:45", 100, 100.1, 99.9, 100, v=10, ha=101, vwap=90, ema=90, st=50),
        _bar("15:15", 100.1, 100.2, 100.0, 100.1, v=1, ha=101, vwap=90, ema=90, st=50),
    ]
    trades = simulate_session(
        {"AAA": a},
        lots={"AAA": 1},
        instruments={"AAA": "fut"},
        keys={"AAA": "k"},
        session_date=SESSION,
        csv_entries=[("AAA", time(9, 45))],
    )
    assert trades[0]["reason"] == "time"
    assert trades[0]["exit"] == pytest.approx(100.1)

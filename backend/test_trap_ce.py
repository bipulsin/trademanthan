"""Unit tests for Trap-CE CSV mapping, 1-lot over cap, BE, EMA trail, EOD."""
from __future__ import annotations

from datetime import date, datetime, time
from pathlib import Path

import pytest
import pytz

from backend.services.trap_ce.candles import session_10m_from_5m
from backend.services.trap_ce.config import SKIP_NO_LOT
from backend.services.trap_ce.csv_signals import load_trap_ce_csv, parse_trigger_datetime, trigger_bar_start
from backend.services.trap_ce.simulate import find_trigger_index, simulate_trap_ce_long

IST = pytz.timezone("Asia/Kolkata")
SESSION = date(2026, 8, 3)


def _bar(hm: str, o, h, l, c, v=1000):
    hour, minute = map(int, hm.split(":"))
    ts = IST.localize(datetime(SESSION.year, SESSION.month, SESSION.day, hour, minute))
    return {
        "timestamp": ts.isoformat(),
        "open": o,
        "high": h,
        "low": l,
        "close": c,
        "volume": v,
        "bar_start": ts,
    }


def test_trigger_mapping_915_and_1115():
    assert trigger_bar_start(parse_trigger_datetime("03-08-2026 9:15 am")) == time(9, 15)
    assert trigger_bar_start(parse_trigger_datetime("03-08-2026 11:15 am")) == time(11, 15)
    assert trigger_bar_start(parse_trigger_datetime("31-07-2026 2:15 pm")) == time(14, 15)
    bars = [_bar("09:15", 100, 101, 99, 100.5), _bar("09:25", 100.5, 102, 100, 101)]
    assert find_trigger_index(bars, time(9, 15)) == 0
    assert find_trigger_index(bars, time(11, 15)) == -1


def test_entry_is_next_10m_open():
    bars = [
        _bar("09:15", 100, 101, 99.0, 100.2),
        _bar("09:25", 100.8, 101.2, 100.5, 101.0),
        _bar("09:35", 101.0, 101.1, 100.9, 101.0),
        _bar("15:15", 101.0, 101.0, 101.0, 101.0),
    ]
    t = simulate_trap_ce_long(
        bars, trigger_time=time(9, 15), lot_size=1, session_date=SESSION, symbol="T"
    )
    assert t["taken"] is True
    assert t["entry"] == 100.8
    assert t["sl_initial"] == 99.0
    assert t["risk_inr"] == pytest.approx(1.8)


def test_over_3k_risk_takes_one_lot():
    # 1R = 10 pts, lot 400 → 4000 INR; still take 1 lot
    bars = [
        _bar("09:15", 100, 101, 90.0, 100),
        _bar("09:25", 100.0, 101, 99, 100),
        _bar("15:15", 100, 100, 100, 100),
    ]
    t = simulate_trap_ce_long(
        bars, trigger_time=time(9, 15), lot_size=400, session_date=SESSION
    )
    assert t["taken"] is True
    assert t["qty"] == 400
    assert t["risk_inr"] == 4000.0


def test_be_when_high_touches_1r():
    # entry 100, sl 99, 1R=1 → BE when high >= 101; then stop at 100, next bar low 99.9 exits at 100
    bars = [
        _bar("09:15", 100, 100.5, 99.0, 100.2),
        _bar("09:25", 100.0, 100.2, 99.8, 100.1),
        _bar("09:35", 100.1, 101.05, 100.0, 100.8),
        _bar("09:45", 100.8, 100.9, 99.5, 100.0),
        _bar("15:15", 100.0, 100.0, 100.0, 100.0),
    ]
    t = simulate_trap_ce_long(
        bars, trigger_time=time(9, 15), lot_size=1, session_date=SESSION
    )
    assert t["taken"] is True
    assert t["be_armed"] is True
    assert t["exit_reason"] == "be"
    assert t["exit"] == 100.0
    assert t["r_realized"] == 0.0


def test_trail_confirmed_close_not_wick():
    # 1R=1, 1.5R=1.5. Wick below EMA must not exit; close below does.
    bars = [
        _bar("09:15", 100, 100.4, 99.0, 100.2),
        _bar("09:25", 100.0, 100.2, 99.9, 100.1),
    ]
    # climb so EMA stays below closes, then tag 1.5R
    px = 100.2
    t = IST.localize(datetime(2026, 8, 3, 9, 35))
    for _ in range(8):
        bars.append(
            {
                "timestamp": t.isoformat(),
                "open": px,
                "high": px + 2.0,
                "low": px - 0.05,
                "close": px + 0.4,
                "volume": 1000,
                "bar_start": t,
            }
        )
        t = t.replace(hour=(t.hour + (t.minute + 10) // 60), minute=(t.minute + 10) % 60)
        px += 0.4
    # wick below ema, close above (no exit)
    wick = {
        "timestamp": t.isoformat(),
        "open": px,
        "high": px + 0.2,
        "low": 50.0,
        "close": px + 0.1,
        "volume": 1000,
        "bar_start": t,
    }
    bars.append(wick)
    t2 = t.replace(hour=(t.hour + (t.minute + 10) // 60), minute=(t.minute + 10) % 60)
    close_break = {
        "timestamp": t2.isoformat(),
        "open": px,
        "high": px,
        "low": 90.0,
        "close": 90.0,
        "volume": 1000,
        "bar_start": t2,
    }
    bars.append(close_break)
    bars.append(_bar("15:15", 90, 90, 90, 90))
    tr = simulate_trap_ce_long(
        bars, trigger_time=time(9, 15), lot_size=1, session_date=SESSION
    )
    assert tr["taken"] is True
    assert tr["trail_armed"] is True
    assert tr["exit_reason"] == "trail_ema10_close"
    assert tr["exit"] == 90.0


def test_eod_1515():
    bars = [
        _bar("09:15", 100, 101, 99.5, 100.2),
        _bar("09:25", 100.0, 100.3, 99.8, 100.1),
        _bar("15:15", 100.4, 100.5, 100.3, 100.4),
    ]
    t = simulate_trap_ce_long(
        bars, trigger_time=time(9, 15), lot_size=1, session_date=SESSION
    )
    assert t["taken"] is True
    assert t["exit_reason"] == "eod_1515"
    assert t["exit"] == 100.4


def test_5m_aggregate_bar_starts_at_915():
    c5 = [
        _bar("09:15", 100, 101, 99, 100.5),
        _bar("09:20", 100.5, 102, 100.4, 101.5),
        _bar("09:25", 101.5, 102, 101, 101.8),
        _bar("09:30", 101.8, 102.2, 101.6, 102.0),
    ]
    bars = session_10m_from_5m(c5, SESSION)
    assert [b["bar_start"].strftime("%H:%M") for b in bars] == ["09:15", "09:25"]
    assert bars[0]["open"] == 100
    assert bars[0]["close"] == 101.5
    assert bars[0]["low"] == 99


def test_csv_load_real_file():
    path = Path("/Users/bipulsahay/TradeManthan/data/trap_ce/Backtest_Intraday_Trap_-_CE.csv")
    rows = load_trap_ce_csv(path)
    assert len(rows) >= 120
    assert rows[0]["symbol"] == "ICICIPRULI"
    assert rows[0]["trigger_time"] == time(10, 15)
    assert all(r["direction"] == "LONG" for r in rows)


def test_nearest_json_fut_when_front_month_missing(monkeypatch):
    from datetime import datetime, timezone

    from backend.services.trap_ce import universe as u

    monkeypatch.setattr(u, "resolve_fut", lambda *a, **k: None)
    exp_ms = int(datetime(2026, 9, 24, tzinfo=timezone.utc).timestamp() * 1000)
    fut_by_und = {
        "BEL": [
            {
                "trading_symbol": "BEL26SEPFUT",
                "instrument_key": "NSE_FO|BELTEST",
                "expiry": exp_ms,
                "lot_size": 475,
            }
        ]
    }
    leg = u.resolve_leg("BEL", date(2026, 7, 31), fut_by_und=fut_by_und)
    assert leg is not None
    assert leg.kind == "fut"
    assert leg.instrument_key == "NSE_FO|BELTEST"
    assert leg.lot_size == 475


def test_no_fut_uses_stock_key_and_qty_one(monkeypatch):
    from backend.services.trap_ce import universe as u

    monkeypatch.setattr(u, "resolve_fut", lambda *a, **k: None)
    monkeypatch.setattr(u, "_resolve_nearest_listed_fut", lambda *a, **k: None)
    eq = {"ICICIPRULI": {"trading_symbol": "ICICIPRULI", "instrument_key": "NSE_EQ|INEICICI"}}
    leg = u.resolve_leg(
        "ICICIPRULI",
        date(2026, 7, 31),
        fut_by_und={},
        eq_by_symbol=eq,
    )
    assert leg is not None
    assert leg.kind == "eq"
    assert leg.instrument_key == "NSE_EQ|INEICICI"
    assert leg.lot_size == 1


def test_summarize_separates_stock_bucket():
    from backend.services.trap_ce.backtest import summarize

    s = summarize(
        [
            {
                "taken": True,
                "bucket": "fut",
                "win": True,
                "r_realized": 1.0,
                "pnl_inr": 100,
                "exit_reason": "sl",
                "risk_inr": 500,
            },
            {
                "taken": True,
                "bucket": "stock",
                "win": False,
                "r_realized": -0.5,
                "pnl_inr": -2,
                "exit_reason": "eod_1515",
                "risk_inr": 4,
            },
            {"taken": False, "skip_reason": SKIP_NO_LOT},
        ]
    )
    assert s["trade_count"] == 1
    assert s["stock_trade_count"] == 1
    assert s["skip_count"] == 1
    assert s["sum_pnl_inr"] == 100
    assert s["stock_sum_pnl_inr"] == -2

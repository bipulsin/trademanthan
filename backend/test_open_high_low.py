"""Unit tests for OpenHighLow — synthetic bars, no network."""
from __future__ import annotations

from datetime import date, datetime, time

import pytz

from backend.services.open_high_low.bars import Bar, first_vwap
from backend.services.open_high_low.config import OHLConfig
from backend.services.open_high_low.data import front_month_expiry, monthly_expiry, known_holidays
from backend.services.open_high_low.signals import detect_setup, open_eq_extreme_flag
from backend.services.open_high_low.simulate import simulate_trade

IST = pytz.timezone("Asia/Kolkata")
SESSION = date(2026, 10, 21)


def _ts(h: int, m: int, d: date = SESSION) -> datetime:
    return IST.localize(datetime(d.year, d.month, d.day, h, m, 0))


def _bar(h: int, m: int, o, hi, lo, c, vol=1000.0, d: date = SESSION) -> Bar:
    return Bar(ts=_ts(h, m, d), open=o, high=hi, low=lo, close=c, volume=vol, contract="TEST")


def _session_bars_from(rows):
    """Build a full-ish session path from (h,m,o,h,l,c) tuples starting 09:15."""
    out = []
    for h, m, o, hi, lo, c, *rest in rows:
        vol = rest[0] if rest else 1000.0
        out.append(_bar(h, m, o, hi, lo, c, vol=vol))
    return out


def test_monthly_expiry_oct_nov_2026():
    hol = known_holidays()
    assert monthly_expiry(2026, 10, hol) == date(2026, 10, 27)
    assert monthly_expiry(2026, 11, hol) == date(2026, 11, 23)
    # Including expiry session: Oct 27 still Oct contract
    assert front_month_expiry(date(2026, 10, 27), hol) == date(2026, 10, 27)
    # Next session rolls
    assert front_month_expiry(date(2026, 10, 28), hol) == date(2026, 11, 23)


def test_vwap_first_candle():
    bars = [
        _bar(9, 15, 100, 102, 99, 101, vol=100),
        _bar(9, 20, 101, 103, 100, 102, vol=100),
    ]
    # typical first = (102+99+101)/3 = 100.666..., vol=100
    v = first_vwap(bars, SESSION, time(9, 15))
    assert v is not None
    assert abs(v - (102 + 99 + 101) / 3) < 1e-9


def test_bullish_signal_and_open_eq_extreme():
    # prev high 100; first close 101 > 100 and > vwap; second close > first high
    first = _bar(9, 15, 99, 101.5, 99, 101, vol=1000)  # open==low → YES
    second = _bar(9, 20, 101.5, 103, 101, 102.5, vol=1000)
    third = _bar(9, 25, 102.5, 104, 102, 103, vol=1000)
    bars = [first, second, third]
    setup = detect_setup(bars, SESSION, prev_high=100.0, prev_low=95.0)
    assert setup is not None
    assert setup.side == "BULLISH"
    assert setup.open_eq_extreme == "YES"
    assert open_eq_extreme_flag("BULLISH", first) == "YES"


def test_bearish_signal_and_open_eq_high():
    first = _bar(9, 15, 100, 100, 97, 98, vol=1000)  # red, open==high → YES
    second = _bar(9, 20, 98, 98.5, 96, 96.5, vol=1000)  # close < first.low
    third = _bar(9, 25, 96.5, 97, 95, 95.5, vol=1000)
    bars = [first, second, third]
    setup = detect_setup(bars, SESSION, prev_high=105.0, prev_low=99.0)
    assert setup is not None
    assert setup.side == "BEARISH"
    assert setup.open_eq_extreme == "YES"


def test_gap_through_sl_bullish():
    first = _bar(9, 15, 100, 102, 100, 101.5, vol=1000)
    second = _bar(9, 20, 102, 103.5, 101.5, 103, vol=1000)
    # Entry open gaps below first.low (SL=100)
    third = _bar(9, 25, 99, 99.5, 98, 98.5, vol=1000)
    bars = [first, second, third]
    setup = detect_setup(bars, SESSION, prev_high=100.0, prev_low=90.0)
    assert setup is not None
    trade = simulate_trade(
        setup, bars, symbol="T", contract="C", expiry=date(2026, 10, 27), lot_size=50
    )
    assert trade["exit_reason"] == "SL"
    assert trade["exit_price"] == 99.0  # gap through → exit at open
    # Entry is also third.open, so gap-through on the entry bar is flat (0 pts).
    assert abs(trade["points_pnl"]) < 1e-9
    assert abs(trade["entry_price"] - 99.0) < 1e-9


def test_sl_on_entry_candle():
    first = _bar(9, 15, 100, 102, 100, 101.5, vol=1000)
    second = _bar(9, 20, 102, 103.5, 101.5, 103, vol=1000)
    # Entry opens above SL but wick hits SL
    third = _bar(9, 25, 102, 102.5, 99.5, 100, vol=1000)
    later = _bar(9, 30, 100, 101, 99, 100.5, vol=1000)
    bars = [first, second, third, later]
    setup = detect_setup(bars, SESSION, prev_high=100.0, prev_low=90.0)
    assert setup is not None
    trade = simulate_trade(
        setup, bars, symbol="T", contract="C", expiry=date(2026, 10, 27), lot_size=1
    )
    assert trade["exit_reason"] == "SL"
    assert abs(trade["exit_price"] - 100.0) < 1e-9
    assert trade["holding_minutes"] == 0.0


def test_eod_exit():
    first = _bar(9, 15, 100, 102, 100, 101.5, vol=1000)
    second = _bar(9, 20, 102, 103.5, 101.5, 103, vol=1000)
    third = _bar(9, 25, 103, 104, 102.5, 103.5, vol=1000)
    # Path that never hits SL; square-off at 15:15
    path = [first, second, third]
    t = time(9, 30)
    # Generate bars until 15:15 with rising lows (SL trails up but stays below)
    from datetime import timedelta

    cur = IST.localize(datetime(SESSION.year, SESSION.month, SESSION.day, 9, 30))
    end = IST.localize(datetime(SESSION.year, SESSION.month, SESSION.day, 15, 15))
    px = 103.5
    while cur <= end:
        path.append(
            Bar(
                ts=cur,
                open=px,
                high=px + 1,
                low=px - 0.2,
                close=px + 0.5,
                volume=500,
                contract="T",
            )
        )
        px += 0.1
        cur += timedelta(minutes=5)
    setup = detect_setup([first, second, third], SESSION, prev_high=100.0, prev_low=90.0)
    assert setup is not None
    trade = simulate_trade(
        setup,
        path,
        symbol="T",
        contract="C",
        expiry=date(2026, 10, 27),
        lot_size=10,
        cfg=OHLConfig(square_off=time(15, 15)),
    )
    assert trade["exit_reason"] == "EOD"
    assert "15:15" in trade["exit_time"]


def test_trailing_ratchet_bullish():
    first = _bar(9, 15, 100, 102, 100, 101.5, vol=1000)
    second = _bar(9, 20, 102, 103.5, 101.5, 103, vol=1000)
    third = _bar(9, 25, 103, 105, 102.8, 104, vol=1000)  # entry
    # Next candle low 103.5 → trail SL up from 100 to 103.5? Wait trail uses
    # preceding candle low at start of subsequent: at start of 09:30, SL=max(100, third.low=102.8)
    c930 = _bar(9, 30, 104, 106, 103.0, 105, vol=1000)
    # At start of 09:35, SL=max(102.8, 103.0)=103.0; then this bar hits 102.5 → trailing SL
    c935 = _bar(9, 35, 105, 105.5, 102.5, 103, vol=1000)
    bars = [first, second, third, c930, c935]
    setup = detect_setup([first, second, third], SESSION, prev_high=100.0, prev_low=90.0)
    assert setup is not None
    trade = simulate_trade(
        setup, bars, symbol="T", contract="C", expiry=date(2026, 10, 27), lot_size=1
    )
    assert trade["exit_reason"] == "Trailing SL"
    assert trade["final_sl"] >= trade["initial_sl"]
    assert abs(trade["exit_price"] - trade["final_sl"]) < 1e-9


def test_open_eq_extreme_no():
    first = _bar(9, 15, 100.5, 102, 100, 101.5, vol=1000)  # open != low
    assert open_eq_extreme_flag("BULLISH", first) == "NO"
    first_b = _bar(9, 15, 99.5, 100, 97, 98, vol=1000)  # open != high
    assert open_eq_extreme_flag("BEARISH", first_b) == "NO"

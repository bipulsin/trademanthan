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
    # prev high 100; first high 101.5 > 100 and close > vwap; second close > first high
    first = _bar(9, 15, 99, 101.5, 99, 101, vol=1000)  # open==low → YES
    second = _bar(9, 20, 101.5, 103, 101, 102.5, vol=1000)
    third = _bar(9, 25, 102.5, 104, 102, 103, vol=1000)
    bars = [first, second, third]
    setup = detect_setup(bars, SESSION, prev_high=100.0, prev_low=95.0)
    assert setup is not None
    assert setup.side == "BULLISH"
    assert setup.open_eq_extreme == "YES"
    assert open_eq_extreme_flag("BULLISH", first) == "YES"


def test_bullish_gate_uses_high_not_only_close():
    # high > PDH but close <= PDH; still bullish if VWAP + 2nd-candle confirm pass
    # VWAP typical = (101.5+99+99.5)/3 ≈ 100.0; close 99.5 < PDH 100 but > VWAP? need close > vwap
    # Use high=100.5 > PDH=100, close=99.8 > vwap≈(100.5+99+99.8)/3=99.766..., second closes above first.high
    first = _bar(9, 15, 99, 100.5, 99, 99.8, vol=1000)  # open==low → YES; close <= PDH
    second = _bar(9, 20, 100.5, 102, 100, 101.5, vol=1000)
    third = _bar(9, 25, 101.5, 103, 101, 102, vol=1000)
    bars = [first, second, third]
    assert first.close <= 100.0
    assert first.high > 100.0
    setup = detect_setup(bars, SESSION, prev_high=100.0, prev_low=95.0)
    assert setup is not None
    assert setup.side == "BULLISH"
    assert setup.open_eq_extreme == "YES"


def test_bearish_signal_and_open_eq_high():
    first = _bar(9, 15, 100, 100, 97, 98, vol=1000)  # red, open==high → YES
    second = _bar(9, 20, 98, 98.5, 96, 96.5, vol=1000)  # close < first.low
    third = _bar(9, 25, 96.5, 97, 95, 95.5, vol=1000)
    bars = [first, second, third]
    setup = detect_setup(bars, SESSION, prev_high=105.0, prev_low=99.0)
    assert setup is not None
    assert setup.side == "BEARISH"
    assert setup.open_eq_extreme == "YES"


def test_bearish_gate_uses_low_not_only_close():
    # low < PDL but close >= PDL; still bearish if VWAP + red + 2nd-candle confirm pass
    # PDL=99; low=98.5 < 99, close=99.2 >= 99; red (close < open); close < vwap
    # VWAP typical = (100+98.5+99.2)/3 ≈ 99.233; close 99.2 < vwap
    first = _bar(9, 15, 100, 100, 98.5, 99.2, vol=1000)  # open==high → YES
    second = _bar(9, 20, 99, 99.2, 97.5, 98.0, vol=1000)  # close < first.low
    third = _bar(9, 25, 98, 98.5, 97, 97.5, vol=1000)
    bars = [first, second, third]
    assert first.close >= 99.0
    assert first.low < 99.0
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
    # Path that never hits fixed SL; square-off at 15:15
    path = [first, second, third]
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
                low=px - 0.2,  # stays above fixed SL=100
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
    assert abs(trade["final_sl"] - trade["initial_sl"]) < 1e-9


def test_fixed_sl_bullish_never_ratchets():
    """Price pulls back below prior lows but above first.low — stay in; exit only on fixed SL."""
    first = _bar(9, 15, 100, 102, 100, 101.5, vol=1000)  # SL = 100
    second = _bar(9, 20, 102, 103.5, 101.5, 103, vol=1000)
    third = _bar(9, 25, 103, 105, 102.8, 104, vol=1000)  # entry
    c930 = _bar(9, 30, 104, 106, 103.0, 105, vol=1000)
    # Under trailing rules this would ratchet SL to ~103 and exit; fixed SL stays at 100.
    c935 = _bar(9, 35, 105, 105.5, 102.5, 103, vol=1000)
    c940 = _bar(9, 40, 103, 103.5, 99.5, 100, vol=1000)  # hits fixed SL=100
    bars = [first, second, third, c930, c935, c940]
    setup = detect_setup([first, second, third], SESSION, prev_high=100.0, prev_low=90.0)
    assert setup is not None
    trade = simulate_trade(
        setup, bars, symbol="T", contract="C", expiry=date(2026, 10, 27), lot_size=1
    )
    assert trade["exit_reason"] == "SL"
    assert abs(trade["initial_sl"] - 100.0) < 1e-9
    assert abs(trade["final_sl"] - 100.0) < 1e-9
    assert abs(trade["exit_price"] - 100.0) < 1e-9
    assert "09:40" in trade["exit_time"]  # held through 09:35 pullback


def test_fixed_sl_bearish_never_ratchets():
    """Price rallies above prior highs but below first.high — stay in; exit on fixed SL."""
    first = _bar(9, 15, 100, 100, 97, 98, vol=1000)  # SL = 100 (first.high)
    second = _bar(9, 20, 98, 98.5, 96, 96.5, vol=1000)
    third = _bar(9, 25, 96.5, 97, 95, 95.5, vol=1000)  # entry
    c930 = _bar(9, 30, 95.5, 96.2, 94.5, 95, vol=1000)
    # Under trailing, SL would ratchet down to ~96.2; fixed stays at 100.
    c935 = _bar(9, 35, 95, 97.5, 94.8, 97, vol=1000)
    c940 = _bar(9, 40, 97, 100.5, 96.5, 100, vol=1000)  # high >= fixed SL=100
    bars = [first, second, third, c930, c935, c940]
    setup = detect_setup([first, second, third], SESSION, prev_high=105.0, prev_low=99.0)
    assert setup is not None
    trade = simulate_trade(
        setup, bars, symbol="T", contract="C", expiry=date(2026, 10, 27), lot_size=1
    )
    assert trade["exit_reason"] == "SL"
    assert abs(trade["initial_sl"] - 100.0) < 1e-9
    assert abs(trade["final_sl"] - 100.0) < 1e-9
    assert abs(trade["exit_price"] - 100.0) < 1e-9
    assert "09:40" in trade["exit_time"]


def test_open_eq_extreme_no():
    first = _bar(9, 15, 100.5, 102, 100, 101.5, vol=1000)  # open != low
    assert open_eq_extreme_flag("BULLISH", first) == "NO"
    first_b = _bar(9, 15, 99.5, 100, 97, 98, vol=1000)  # open != high
    assert open_eq_extreme_flag("BEARISH", first_b) == "NO"

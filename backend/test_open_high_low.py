"""Unit tests for OpenHighLow — synthetic bars, no network."""
from __future__ import annotations

from datetime import date, datetime, time

import pytz

from backend.services.open_high_low.bars import Bar, first_vwap
from backend.services.open_high_low.config import OHLConfig
from backend.services.open_high_low.data import front_month_expiry, monthly_expiry, known_holidays
from backend.services.open_high_low.indicators import wilder_atr
from backend.services.open_high_low.signals import detect_setup, open_eq_extreme_flag
from backend.services.open_high_low.simulate import simulate_trade, trail_target_sl

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
    # Path that never hits SL; square-off at 15:15. Keep highs < entry+ATR so trail stays initial.
    atr = 10.0
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
                high=px + 0.5,  # never reaches +1 ATR from entry 103
                low=px - 0.2,  # stays above initial SL=100
                close=px + 0.2,
                volume=500,
                contract="T",
            )
        )
        px += 0.05
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
        atr=atr,
    )
    assert trade["exit_reason"] == "EOD"
    assert "15:15" in trade["exit_time"]
    assert abs(trade["final_sl"] - trade["initial_sl"]) < 1e-9
    assert abs(trade["atr"] - atr) < 1e-9


def test_trail_target_helper_steps():
    # Bull: 1 ATR fav → BE; 2 ATR → entry+1ATR; never loosens
    assert abs(trail_target_sl(side="BULLISH", entry_price=100, atr=5, extreme=104.9, current_sl=98) - 98) < 1e-9
    assert abs(trail_target_sl(side="BULLISH", entry_price=100, atr=5, extreme=105.0, current_sl=98) - 100) < 1e-9
    assert abs(trail_target_sl(side="BULLISH", entry_price=100, atr=5, extreme=110.0, current_sl=100) - 105) < 1e-9
    # Already at 105; extreme backs off conceptually but we pass lower target via current_sl ratchet
    assert abs(trail_target_sl(side="BULLISH", entry_price=100, atr=5, extreme=106.0, current_sl=105) - 105) < 1e-9
    # Bear
    assert abs(trail_target_sl(side="BEARISH", entry_price=100, atr=5, extreme=95.0, current_sl=102) - 100) < 1e-9
    assert abs(trail_target_sl(side="BEARISH", entry_price=100, atr=5, extreme=90.0, current_sl=100) - 95) < 1e-9


def test_breakeven_at_1_atr_then_trailing_sl():
    """After a candle closes with ≥1 ATR favorable extreme, next candle stops at entry (BE)."""
    atr = 5.0
    first = _bar(9, 15, 100, 102, 100, 101.5, vol=1000)  # initial SL=100
    second = _bar(9, 20, 102, 103.5, 101.5, 103, vol=1000)
    third = _bar(9, 25, 103, 104, 102.5, 103.5, vol=1000)  # entry=103; fav < 1 ATR
    # Extreme reaches entry+ATR → after this bar SL → 103 (BE) for next candle
    c930 = _bar(9, 30, 103.5, 108.0, 103.2, 107, vol=1000)
    # Hits prior SL which is now BE=103
    c935 = _bar(9, 35, 106, 106.5, 102.5, 103, vol=1000)
    bars = [first, second, third, c930, c935]
    setup = detect_setup([first, second, third], SESSION, prev_high=100.0, prev_low=90.0)
    assert setup is not None
    trade = simulate_trade(
        setup, bars, symbol="T", contract="C", expiry=date(2026, 10, 27), lot_size=1, atr=atr
    )
    assert trade["exit_reason"] == "Trailing SL"
    assert abs(trade["final_sl"] - 103.0) < 1e-9
    assert abs(trade["exit_price"] - 103.0) < 1e-9
    assert "09:35" in trade["exit_time"]


def test_step_lock_at_2_atr():
    """At 2 ATR favorable, SL locks at entry+1*ATR; stop there on next bar."""
    atr = 5.0
    first = _bar(9, 15, 100, 102, 100, 101.5, vol=1000)
    second = _bar(9, 20, 102, 103.5, 101.5, 103, vol=1000)
    third = _bar(9, 25, 103, 104, 102.5, 103.5, vol=1000)  # entry=103
    # +2 ATR extreme → trail to entry+1*ATR = 108 after this bar
    c930 = _bar(9, 30, 104, 113.0, 103.5, 112, vol=1000)
    c935 = _bar(9, 35, 112, 112.5, 107.5, 108, vol=1000)  # low <= 108
    bars = [first, second, third, c930, c935]
    setup = detect_setup([first, second, third], SESSION, prev_high=100.0, prev_low=90.0)
    assert setup is not None
    trade = simulate_trade(
        setup, bars, symbol="T", contract="C", expiry=date(2026, 10, 27), lot_size=1, atr=atr
    )
    assert trade["exit_reason"] == "Trailing SL"
    assert abs(trade["final_sl"] - 108.0) < 1e-9
    assert abs(trade["exit_price"] - 108.0) < 1e-9


def test_trail_never_loosens():
    """After locking BE, a quieter candle must not pull SL back down."""
    atr = 5.0
    first = _bar(9, 15, 100, 102, 100, 101.5, vol=1000)
    second = _bar(9, 20, 102, 103.5, 101.5, 103, vol=1000)
    third = _bar(9, 25, 103, 104, 102.5, 103.5, vol=1000)
    c930 = _bar(9, 30, 103.5, 108.0, 103.2, 107, vol=1000)  # arms BE
    c935 = _bar(9, 35, 107, 107.5, 105.0, 106, vol=1000)  # quieter; SL stays 103
    c940 = _bar(9, 40, 106, 106.5, 102.8, 103, vol=1000)  # hits BE
    bars = [first, second, third, c930, c935, c940]
    setup = detect_setup([first, second, third], SESSION, prev_high=100.0, prev_low=90.0)
    assert setup is not None
    trade = simulate_trade(
        setup, bars, symbol="T", contract="C", expiry=date(2026, 10, 27), lot_size=1, atr=atr
    )
    assert trade["exit_reason"] == "Trailing SL"
    assert abs(trade["final_sl"] - 103.0) < 1e-9
    assert "09:40" in trade["exit_time"]


def test_initial_sl_before_1_atr():
    """Until 1 ATR favorable is locked on a prior bar, stop remains first-candle SL."""
    atr = 5.0
    first = _bar(9, 15, 100, 102, 100, 101.5, vol=1000)  # SL=100
    second = _bar(9, 20, 102, 103.5, 101.5, 103, vol=1000)
    third = _bar(9, 25, 103, 106, 102.5, 105, vol=1000)  # high-entry=3 < 5 ATR
    c930 = _bar(9, 30, 105, 107, 104, 106, vol=1000)  # extreme still < 1 ATR from entry
    c935 = _bar(9, 35, 106, 106.5, 99.5, 100, vol=1000)  # hits initial 100
    bars = [first, second, third, c930, c935]
    setup = detect_setup([first, second, third], SESSION, prev_high=100.0, prev_low=90.0)
    assert setup is not None
    trade = simulate_trade(
        setup, bars, symbol="T", contract="C", expiry=date(2026, 10, 27), lot_size=1, atr=atr
    )
    assert trade["exit_reason"] == "SL"
    assert abs(trade["final_sl"] - 100.0) < 1e-9
    assert abs(trade["exit_price"] - 100.0) < 1e-9


def test_causal_same_bar_uses_prior_sl():
    """Candle that would arm BE and also hits prior SL exits on prior SL (not trail-first)."""
    atr = 5.0
    first = _bar(9, 15, 100, 102, 100, 101.5, vol=1000)
    second = _bar(9, 20, 102, 103.5, 101.5, 103, vol=1000)
    third = _bar(9, 25, 103, 104, 102.5, 103.5, vol=1000)
    # Same bar: high reaches +1 ATR AND low hits initial SL — stop on prior SL=100 as "SL"
    c930 = _bar(9, 30, 103.5, 108.0, 99.5, 100, vol=1000)
    bars = [first, second, third, c930]
    setup = detect_setup([first, second, third], SESSION, prev_high=100.0, prev_low=90.0)
    assert setup is not None
    trade = simulate_trade(
        setup, bars, symbol="T", contract="C", expiry=date(2026, 10, 27), lot_size=1, atr=atr
    )
    assert trade["exit_reason"] == "SL"
    assert abs(trade["final_sl"] - 100.0) < 1e-9
    assert abs(trade["exit_price"] - 100.0) < 1e-9


def test_atr_missing_falls_back_to_fixed():
    """atr=None / 0 → fixed initial SL only (no trail)."""
    first = _bar(9, 15, 100, 102, 100, 101.5, vol=1000)
    second = _bar(9, 20, 102, 103.5, 101.5, 103, vol=1000)
    third = _bar(9, 25, 103, 110, 102.5, 109, vol=1000)  # big move
    c930 = _bar(9, 30, 109, 112, 108, 110, vol=1000)
    c935 = _bar(9, 35, 110, 110.5, 99.5, 100, vol=1000)  # hits initial
    bars = [first, second, third, c930, c935]
    setup = detect_setup([first, second, third], SESSION, prev_high=100.0, prev_low=90.0)
    assert setup is not None
    for atr in (None, 0.0):
        trade = simulate_trade(
            setup, bars, symbol="T", contract="C", expiry=date(2026, 10, 27), lot_size=1, atr=atr
        )
        assert trade["exit_reason"] == "SL"
        assert abs(trade["final_sl"] - 100.0) < 1e-9
        assert trade["atr"] is None


def test_bearish_trail_breakeven():
    atr = 5.0
    first = _bar(9, 15, 100, 100, 97, 98, vol=1000)  # SL=100
    second = _bar(9, 20, 98, 98.5, 96, 96.5, vol=1000)
    third = _bar(9, 25, 96.5, 97, 95, 95.5, vol=1000)  # entry=96.5
    # 1 ATR favorable: entry - extreme >= 5 → extreme <= 91.5
    c930 = _bar(9, 30, 95, 95.5, 91.5, 92, vol=1000)
    c935 = _bar(9, 35, 92, 97.0, 91.8, 96, vol=1000)  # high >= BE 96.5
    bars = [first, second, third, c930, c935]
    setup = detect_setup([first, second, third], SESSION, prev_high=105.0, prev_low=99.0)
    assert setup is not None
    trade = simulate_trade(
        setup, bars, symbol="T", contract="C", expiry=date(2026, 10, 27), lot_size=1, atr=atr
    )
    assert trade["exit_reason"] == "Trailing SL"
    assert abs(trade["final_sl"] - 96.5) < 1e-9
    assert abs(trade["exit_price"] - 96.5) < 1e-9


def test_wilder_atr_sma_seed():
    # Minimal synthetic: 15 closes so ATR(14) resolves
    highs = [10 + i * 0.1 for i in range(15)]
    lows = [9 + i * 0.1 for i in range(15)]
    closes = [9.5 + i * 0.1 for i in range(15)]
    atr = wilder_atr(highs, lows, closes, period=14)
    assert atr is not None and atr > 0


def test_open_eq_extreme_no():
    first = _bar(9, 15, 100.5, 102, 100, 101.5, vol=1000)  # open != low
    assert open_eq_extreme_flag("BULLISH", first) == "NO"
    first_b = _bar(9, 15, 99.5, 100, 97, 98, vol=1000)  # open != high
    assert open_eq_extreme_flag("BEARISH", first_b) == "NO"

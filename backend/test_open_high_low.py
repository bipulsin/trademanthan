"""Unit tests for OpenHighLow — synthetic bars, no network."""
from __future__ import annotations

from datetime import date, datetime, time, timedelta

import pytz

from backend.services.open_high_low.bars import Bar, first_vwap
from backend.services.open_high_low.config import OHLConfig
from backend.services.open_high_low.data import front_month_expiry, monthly_expiry, known_holidays
from backend.services.open_high_low.features import (
    build_entry_features,
    closes_through_second,
    ema30_at_decision,
)
from backend.services.open_high_low.indicators import ema_last, wilder_atr
from backend.services.open_high_low.selection import (
    passes_move_filter,
    select_top_per_side,
    filter_candidates,
    apply_selection_flags,
)
from backend.services.open_high_low.signals import detect_setup, open_eq_extreme_flag
from backend.services.open_high_low.simulate import simulate_trade, trail_target_sl

IST = pytz.timezone("Asia/Kolkata")
SESSION = date(2026, 10, 21)


def _ts(h: int, m: int, d: date = SESSION) -> datetime:
    return IST.localize(datetime(d.year, d.month, d.day, h, m, 0))


def _bar(h: int, m: int, o, hi, lo, c, vol=1000.0, d: date = SESSION) -> Bar:
    return Bar(ts=_ts(h, m, d), open=o, high=hi, low=lo, close=c, volume=vol, contract="TEST")


def test_monthly_expiry_oct_nov_2026():
    hol = known_holidays()
    assert monthly_expiry(2026, 10, hol) == date(2026, 10, 27)
    assert monthly_expiry(2026, 11, hol) == date(2026, 11, 23)
    assert front_month_expiry(date(2026, 10, 27), hol) == date(2026, 10, 27)
    assert front_month_expiry(date(2026, 10, 28), hol) == date(2026, 11, 23)


def test_vwap_first_candle():
    bars = [
        _bar(9, 15, 100, 102, 99, 101, vol=100),
        _bar(9, 20, 101, 103, 100, 102, vol=100),
    ]
    v = first_vwap(bars, SESSION, time(9, 15))
    assert v is not None
    assert abs(v - (102 + 99 + 101) / 3) < 1e-9


def test_bullish_signal_and_open_eq_extreme():
    first = _bar(9, 15, 99, 101.5, 99, 101, vol=1000)
    second = _bar(9, 20, 101.5, 103, 101, 102.5, vol=1000)
    third = _bar(9, 25, 102.5, 104, 102, 103, vol=1000)
    bars = [first, second, third]
    setup = detect_setup(bars, SESSION, prev_high=100.0, prev_low=95.0)
    assert setup is not None
    assert setup.side == "BULLISH"
    assert setup.open_eq_extreme == "YES"
    assert open_eq_extreme_flag("BULLISH", first) == "YES"


def test_bullish_gate_uses_high_not_only_close():
    first = _bar(9, 15, 99, 100.5, 99, 99.8, vol=1000)
    second = _bar(9, 20, 100.5, 102, 100, 101.5, vol=1000)
    third = _bar(9, 25, 101.5, 103, 101, 102, vol=1000)
    bars = [first, second, third]
    assert first.close <= 100.0
    assert first.high > 100.0
    setup = detect_setup(bars, SESSION, prev_high=100.0, prev_low=95.0)
    assert setup is not None
    assert setup.side == "BULLISH"


def test_bearish_signal_and_open_eq_high():
    first = _bar(9, 15, 100, 100, 97, 98, vol=1000)
    second = _bar(9, 20, 98, 98.5, 96, 96.5, vol=1000)
    third = _bar(9, 25, 96.5, 97, 95, 95.5, vol=1000)
    bars = [first, second, third]
    setup = detect_setup(bars, SESSION, prev_high=105.0, prev_low=99.0)
    assert setup is not None
    assert setup.side == "BEARISH"
    assert setup.open_eq_extreme == "YES"


def test_bearish_gate_uses_low_not_only_close():
    first = _bar(9, 15, 100, 100, 98.5, 99.2, vol=1000)
    second = _bar(9, 20, 99, 99.2, 97.5, 98.0, vol=1000)
    third = _bar(9, 25, 98, 98.5, 97, 97.5, vol=1000)
    bars = [first, second, third]
    assert first.close >= 99.0
    assert first.low < 99.0
    setup = detect_setup(bars, SESSION, prev_high=105.0, prev_low=99.0)
    assert setup is not None
    assert setup.side == "BEARISH"


def test_gap_through_sl_bullish():
    first = _bar(9, 15, 100, 102, 100, 101.5, vol=1000)
    second = _bar(9, 20, 102, 103.5, 101.5, 103, vol=1000)
    third = _bar(9, 25, 99, 99.5, 98, 98.5, vol=1000)
    bars = [first, second, third]
    setup = detect_setup(bars, SESSION, prev_high=100.0, prev_low=90.0)
    assert setup is not None
    trade = simulate_trade(
        setup, bars, symbol="T", contract="C", expiry=date(2026, 10, 27), lot_size=50, sl_mode="fixed"
    )
    assert trade["exit_reason"] == "SL"
    assert trade["exit_price"] == 99.0
    assert abs(trade["points_pnl"]) < 1e-9


def test_sl_on_entry_candle():
    first = _bar(9, 15, 100, 102, 100, 101.5, vol=1000)
    second = _bar(9, 20, 102, 103.5, 101.5, 103, vol=1000)
    third = _bar(9, 25, 102, 102.5, 99.5, 100, vol=1000)
    later = _bar(9, 30, 100, 101, 99, 100.5, vol=1000)
    bars = [first, second, third, later]
    setup = detect_setup(bars, SESSION, prev_high=100.0, prev_low=90.0)
    assert setup is not None
    trade = simulate_trade(
        setup, bars, symbol="T", contract="C", expiry=date(2026, 10, 27), lot_size=1, sl_mode="fixed"
    )
    assert trade["exit_reason"] == "SL"
    assert abs(trade["exit_price"] - 100.0) < 1e-9


def test_eod_exit_fixed():
    first = _bar(9, 15, 100, 102, 100, 101.5, vol=1000)
    second = _bar(9, 20, 102, 103.5, 101.5, 103, vol=1000)
    third = _bar(9, 25, 103, 104, 102.5, 103.5, vol=1000)
    path = [first, second, third]
    cur = IST.localize(datetime(SESSION.year, SESSION.month, SESSION.day, 9, 30))
    end = IST.localize(datetime(SESSION.year, SESSION.month, SESSION.day, 15, 15))
    px = 103.5
    while cur <= end:
        path.append(
            Bar(
                ts=cur,
                open=px,
                high=px + 0.5,
                low=px - 0.2,
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
        sl_mode="fixed",
    )
    assert trade["exit_reason"] == "EOD"
    assert abs(trade["final_sl"] - trade["initial_sl"]) < 1e-9


def test_candle_trail_moves_sl_bullish():
    """After entry candle, SL ratchets to prior candle low; next bar can stop there."""
    first = _bar(9, 15, 100, 102, 100, 101.5, vol=1000)  # initial SL=100
    second = _bar(9, 20, 102, 103.5, 101.5, 103, vol=1000)
    third = _bar(9, 25, 103, 104, 102.0, 103.5, vol=1000)  # entry; after bar SL→max(100,102)=102
    c930 = _bar(9, 30, 103.5, 105, 102.5, 104, vol=1000)  # survives; SL→max(102,102.5)=102.5
    c935 = _bar(9, 35, 104, 104.5, 102.4, 103, vol=1000)  # hits trailed 102.5
    bars = [first, second, third, c930, c935]
    setup = detect_setup([first, second, third], SESSION, prev_high=100.0, prev_low=90.0)
    assert setup is not None
    trade = simulate_trade(
        setup,
        bars,
        symbol="T",
        contract="C",
        expiry=date(2026, 10, 27),
        lot_size=1,
        sl_mode="candle_trail",
        record_sl_path=True,
    )
    assert trade["exit_reason"] == "Trailing SL"
    assert abs(trade["final_sl"] - 102.5) > 1e-9 or abs(trade["final_sl"] - 102.5) < 1e-9
    assert trade["final_sl"] > trade["initial_sl"]
    assert abs(trade["final_sl"] - 102.5) < 1e-9
    assert abs(trade["exit_price"] - 102.5) < 1e-9
    path = trade["sl_path"]
    # Entry bar should update SL after surviving
    assert any(abs(float(p["sl_after"]) - float(p["sl_before"])) > 1e-9 for p in path)


def test_candle_trail_never_loosens_bullish():
    first = _bar(9, 15, 100, 102, 100, 101.5, vol=1000)
    second = _bar(9, 20, 102, 103.5, 101.5, 103, vol=1000)
    third = _bar(9, 25, 103, 104, 102.5, 103.5, vol=1000)  # SL→102.5
    c930 = _bar(9, 30, 103.5, 105, 101.0, 104, vol=1000)  # low 101 < SL but wait — hit check first!
    # Actually low 101 <= SL 102.5 would stop. Use higher low that doesn't hit, but is below prior trail.
    c930 = _bar(9, 30, 103.5, 105, 103.0, 104, vol=1000)  # SL→max(102.5,103)=103
    c935 = _bar(9, 35, 104, 104.5, 102.0, 103, vol=1000)  # quieter low 102 — must NOT loosen; hits 103
    bars = [first, second, third, c930, c935]
    setup = detect_setup([first, second, third], SESSION, prev_high=100.0, prev_low=90.0)
    assert setup is not None
    trade = simulate_trade(
        setup, bars, symbol="T", contract="C", expiry=date(2026, 10, 27), lot_size=1, sl_mode="candle_trail"
    )
    assert trade["exit_reason"] == "Trailing SL"
    assert abs(trade["final_sl"] - 103.0) < 1e-9


def test_candle_trail_bearish():
    first = _bar(9, 15, 100, 100, 97, 98, vol=1000)  # SL=100
    second = _bar(9, 20, 98, 98.5, 96, 96.5, vol=1000)
    third = _bar(9, 25, 96.5, 97.0, 95, 95.5, vol=1000)  # after: SL=min(100,97)=97
    c930 = _bar(9, 30, 95, 96.0, 94, 94.5, vol=1000)  # SL→min(97,96)=96
    c935 = _bar(9, 35, 94.5, 96.5, 94, 96, vol=1000)  # high >= 96 → trailing SL
    bars = [first, second, third, c930, c935]
    setup = detect_setup([first, second, third], SESSION, prev_high=105.0, prev_low=99.0)
    assert setup is not None
    trade = simulate_trade(
        setup, bars, symbol="T", contract="C", expiry=date(2026, 10, 27), lot_size=1, sl_mode="candle_trail"
    )
    assert trade["exit_reason"] == "Trailing SL"
    assert trade["final_sl"] < trade["initial_sl"]
    assert abs(trade["final_sl"] - 96.0) < 1e-9


def test_fixed_mode_never_trails():
    first = _bar(9, 15, 100, 102, 100, 101.5, vol=1000)
    second = _bar(9, 20, 102, 103.5, 101.5, 103, vol=1000)
    third = _bar(9, 25, 103, 110, 102.5, 109, vol=1000)
    c930 = _bar(9, 30, 109, 112, 108, 110, vol=1000)
    c935 = _bar(9, 35, 110, 110.5, 99.5, 100, vol=1000)
    bars = [first, second, third, c930, c935]
    setup = detect_setup([first, second, third], SESSION, prev_high=100.0, prev_low=90.0)
    assert setup is not None
    trade = simulate_trade(
        setup, bars, symbol="T", contract="C", expiry=date(2026, 10, 27), lot_size=1, sl_mode="fixed"
    )
    assert trade["exit_reason"] == "SL"
    assert abs(trade["final_sl"] - 100.0) < 1e-9


def test_trail_target_helper_steps():
    assert abs(trail_target_sl(side="BULLISH", entry_price=100, atr=5, extreme=104.9, current_sl=98) - 98) < 1e-9
    assert abs(trail_target_sl(side="BULLISH", entry_price=100, atr=5, extreme=105.0, current_sl=98) - 100) < 1e-9
    assert abs(trail_target_sl(side="BULLISH", entry_price=100, atr=5, extreme=110.0, current_sl=100) - 105) < 1e-9
    assert abs(trail_target_sl(side="BULLISH", entry_price=100, atr=5, extreme=106.0, current_sl=105) - 105) < 1e-9
    assert abs(trail_target_sl(side="BEARISH", entry_price=100, atr=5, extreme=95.0, current_sl=102) - 100) < 1e-9
    assert abs(trail_target_sl(side="BEARISH", entry_price=100, atr=5, extreme=90.0, current_sl=100) - 95) < 1e-9


def test_breakeven_at_1_atr_then_trailing_sl():
    atr = 5.0
    first = _bar(9, 15, 100, 102, 100, 101.5, vol=1000)
    second = _bar(9, 20, 102, 103.5, 101.5, 103, vol=1000)
    third = _bar(9, 25, 103, 104, 102.5, 103.5, vol=1000)
    c930 = _bar(9, 30, 103.5, 108.0, 103.2, 107, vol=1000)
    c935 = _bar(9, 35, 106, 106.5, 102.5, 103, vol=1000)
    bars = [first, second, third, c930, c935]
    setup = detect_setup([first, second, third], SESSION, prev_high=100.0, prev_low=90.0)
    assert setup is not None
    trade = simulate_trade(
        setup, bars, symbol="T", contract="C", expiry=date(2026, 10, 27), lot_size=1, atr=atr, sl_mode="atr_trail"
    )
    assert trade["exit_reason"] == "Trailing SL"
    assert abs(trade["final_sl"] - 103.0) < 1e-9


def test_step_lock_at_2_atr():
    atr = 5.0
    first = _bar(9, 15, 100, 102, 100, 101.5, vol=1000)
    second = _bar(9, 20, 102, 103.5, 101.5, 103, vol=1000)
    third = _bar(9, 25, 103, 104, 102.5, 103.5, vol=1000)
    c930 = _bar(9, 30, 104, 113.0, 103.5, 112, vol=1000)
    c935 = _bar(9, 35, 112, 112.5, 107.5, 108, vol=1000)
    bars = [first, second, third, c930, c935]
    setup = detect_setup([first, second, third], SESSION, prev_high=100.0, prev_low=90.0)
    assert setup is not None
    trade = simulate_trade(
        setup, bars, symbol="T", contract="C", expiry=date(2026, 10, 27), lot_size=1, atr=atr, sl_mode="atr_trail"
    )
    assert trade["exit_reason"] == "Trailing SL"
    assert abs(trade["final_sl"] - 108.0) < 1e-9


def test_atr_missing_falls_back_to_fixed():
    first = _bar(9, 15, 100, 102, 100, 101.5, vol=1000)
    second = _bar(9, 20, 102, 103.5, 101.5, 103, vol=1000)
    third = _bar(9, 25, 103, 110, 102.5, 109, vol=1000)
    c930 = _bar(9, 30, 109, 112, 108, 110, vol=1000)
    c935 = _bar(9, 35, 110, 110.5, 99.5, 100, vol=1000)
    bars = [first, second, third, c930, c935]
    setup = detect_setup([first, second, third], SESSION, prev_high=100.0, prev_low=90.0)
    assert setup is not None
    for atr in (None, 0.0):
        trade = simulate_trade(
            setup, bars, symbol="T", contract="C", expiry=date(2026, 10, 27), lot_size=1, atr=atr, sl_mode="atr_trail"
        )
        assert trade["exit_reason"] == "SL"
        assert abs(trade["final_sl"] - 100.0) < 1e-9


def test_causal_same_bar_uses_prior_sl_candle_trail():
    """Bar that would raise trail and also hit prior SL exits on prior SL."""
    first = _bar(9, 15, 100, 102, 100, 101.5, vol=1000)
    second = _bar(9, 20, 102, 103.5, 101.5, 103, vol=1000)
    third = _bar(9, 25, 103, 104, 102.5, 103.5, vol=1000)  # would trail to 102.5 after
    # Same bar after entry already done; on 9:30: low hits 100 prior to any update from 9:25...
    # After third completes SL=102.5. On 9:30 hit prior 102.5 AND high raises — stop on prior.
    c930 = _bar(9, 30, 103.5, 106.0, 102.0, 102.2, vol=1000)
    bars = [first, second, third, c930]
    setup = detect_setup([first, second, third], SESSION, prev_high=100.0, prev_low=90.0)
    assert setup is not None
    trade = simulate_trade(
        setup, bars, symbol="T", contract="C", expiry=date(2026, 10, 27), lot_size=1, sl_mode="candle_trail"
    )
    assert trade["exit_reason"] == "Trailing SL"
    assert abs(trade["final_sl"] - 102.5) < 1e-9


def test_wilder_atr_sma_seed():
    highs = [10 + i * 0.1 for i in range(15)]
    lows = [9 + i * 0.1 for i in range(15)]
    closes = [9.5 + i * 0.1 for i in range(15)]
    atr = wilder_atr(highs, lows, closes, period=14)
    assert atr is not None and atr > 0


def test_ema_last_and_no_lookahead_third_candle():
    """EMA at decision must ignore the third (entry) candle close."""
    prev = date(2026, 10, 20)
    # Build 30 prior closes + first two of session
    bars = []
    # 28 bars previous day ending at various times — simplify: 30 flat bars on prev day
    for i in range(30):
        # fake timestamps spaced 5m from 9:15
        mins = 9 * 60 + 15 + i * 5
        h, m = divmod(mins, 60)
        if h >= 15:
            break
        bars.append(_bar(h, m, 100, 101, 99, 100.0 + i * 0.01, d=prev))
    # Ensure we have enough — add more if needed on earlier day
    while len([b for b in bars]) < 30:
        bars.insert(0, _bar(10, 0, 100, 101, 99, 99.5, d=date(2026, 10, 17)))

    first = _bar(9, 15, 100, 102, 100, 101.0, vol=1000)  # close 101
    second = _bar(9, 20, 101, 103, 100.5, 102.0, vol=1000)  # close 102
    third = _bar(9, 25, 102, 150, 102, 149.0, vol=1000)  # huge close — must NOT affect EMA
    day_bars = bars + [first, second, third]

    closes_ok = closes_through_second(day_bars, SESSION)
    assert closes_ok[-1] == 102.0  # second candle, not 149
    assert 149.0 not in closes_ok[-3:]

    ema = ema30_at_decision(day_bars, SESSION, period=30)
    ema_with_third = ema_last(closes_ok + [149.0], period=30)
    assert ema is not None
    assert ema_with_third is not None
    assert abs(ema - ema_with_third) > 1e-6  # third candle would have moved it


def test_move_filter_signed():
    assert passes_move_filter("BULLISH", 2.5) is True
    assert passes_move_filter("BULLISH", 3.0) is True
    assert passes_move_filter("BULLISH", 3.1) is False
    assert passes_move_filter("BEARISH", -2.5) is True
    assert passes_move_filter("BEARISH", -3.0) is True
    assert passes_move_filter("BEARISH", -3.1) is False
    assert passes_move_filter("BEARISH", 1.0) is True  # not extended into short
    assert passes_move_filter("BULLISH", -1.0) is True
    assert passes_move_filter("BULLISH", None) is False


def test_selection_top2_and_composite():
    day = "2026-10-21"
    # 4 bullish + 3 bearish filtered candidates
    cands = []
    for i, (sym, dist, rng) in enumerate(
        [
            ("A", 0.1, 1.5),
            ("B", 0.5, 0.2),
            ("C", 0.2, 0.8),
            ("D", 1.0, 0.1),
        ]
    ):
        cands.append(
            {
                "date": day,
                "symbol": sym,
                "side": "BULLISH",
                "entry_dist_ema30_atr": dist,
                "first_range_atr": rng,
                "move_prev_close_to_entry_pct": 1.0,
            }
        )
    for sym, dist, rng in [("X", 0.05, 2.0), ("Y", 0.4, 0.3), ("Z", 0.9, 0.5)]:
        cands.append(
            {
                "date": day,
                "symbol": sym,
                "side": "BEARISH",
                "entry_dist_ema30_atr": -dist,
                "first_range_atr": rng,
                "move_prev_close_to_entry_pct": -1.0,
            }
        )

    ema_picks = select_top_per_side(cands, rule="ema", top_n=2)
    assert {p["symbol"] for p in ema_picks if p["side"] == "BULLISH"} == {"A", "C"}
    assert {p["symbol"] for p in ema_picks if p["side"] == "BEARISH"} == {"X", "Y"}

    range_picks = select_top_per_side(cands, rule="range", top_n=2)
    assert {p["symbol"] for p in range_picks if p["side"] == "BULLISH"} == {"D", "B"}

    comp = select_top_per_side(cands, rule="composite", top_n=2)
    assert len([p for p in comp if p["side"] == "BULLISH"]) == 2
    assert len([p for p in comp if p["side"] == "BEARISH"]) == 2


def test_selection_filter_excludes_extended():
    rows = [
        {"date": "2026-10-21", "symbol": "OK", "side": "BULLISH", "move_prev_close_to_entry_pct": 2.0},
        {"date": "2026-10-21", "symbol": "EXT", "side": "BULLISH", "move_prev_close_to_entry_pct": 5.0},
    ]
    filtered = filter_candidates(rows, limit_pct=3.0)
    assert [r["symbol"] for r in filtered] == ["OK"]


def test_apply_selection_flags_on_trades():
    trades = []
    for mode in ("fixed", "candle_trail"):
        for sym, side, move, dist, rng in [
            ("A", "BULLISH", 1.0, 0.1, 1.0),
            ("B", "BULLISH", 1.0, 0.2, 0.1),
            ("C", "BULLISH", 5.0, 0.05, 0.05),  # fails filter
            ("X", "BEARISH", -1.0, -0.1, 1.0),
            ("Y", "BEARISH", -1.0, -0.5, 0.2),
        ]:
            trades.append(
                {
                    "date": "2026-10-21",
                    "symbol": sym,
                    "side": side,
                    "sl_mode": mode,
                    "move_prev_close_to_entry_pct": move,
                    "entry_dist_ema30_atr": dist,
                    "first_range_atr": rng,
                    "rupee_pnl": 0.0,
                }
            )
    apply_selection_flags(trades, limit_pct=3.0, top_n=2)
    # C fails filter
    assert all(not t["passed_move_filter"] for t in trades if t["symbol"] == "C")
    # A and B both bullish pass; ema prefers A then B
    assert all(t["selected_ema"] for t in trades if t["symbol"] in ("A", "B", "X", "Y"))
    assert all(not t["selected_ema"] for t in trades if t["symbol"] == "C")


def test_fixed_risk_sizing_column():
    first = _bar(9, 15, 100, 102, 100, 101.5, vol=1000)  # SL=100, risk=3 if entry=103
    second = _bar(9, 20, 102, 103.5, 101.5, 103, vol=1000)
    third = _bar(9, 25, 103, 104, 102.5, 103.5, vol=1000)
    # Hit SL immediately on entry wick
    third = _bar(9, 25, 103, 103.5, 99.5, 100, vol=1000)
    bars = [first, second, third]
    setup = detect_setup(bars, SESSION, prev_high=100.0, prev_low=90.0)
    assert setup is not None
    trade = simulate_trade(
        setup,
        bars,
        symbol="T",
        contract="C",
        expiry=date(2026, 10, 27),
        lot_size=50,
        cfg=OHLConfig(risk_rupees=3000.0),
        sl_mode="fixed",
    )
    # entry=103, sl=100, risk_pts=3 → shares=1000; points=-3 → pnl_fr=-3000
    assert abs(trade["fixed_risk_shares"] - 1000.0) < 1e-6
    assert abs(trade["rupee_pnl_fixed_risk"] - (-3000.0)) < 1e-6
    assert abs(trade["rupee_pnl"] - (-3.0 * 50)) < 1e-6


def test_build_entry_features_basic():
    from backend.services.open_high_low.data import previous_trading_day

    first = _bar(9, 15, 100, 102, 100, 101.5, vol=2000)
    second = _bar(9, 20, 102, 103.5, 101.5, 103, vol=1000)
    third = _bar(9, 25, 103, 104, 102.5, 103.5, vol=1000)
    # prior daily close on calendar previous trading day (Oct 20 2026 is a holiday)
    prev = previous_trading_day(SESSION)
    daily = [
        Bar(
            ts=_ts(15, 30, prev),
            open=98,
            high=101,
            low=97,
            close=100.0,
            volume=1e6,
            contract="T",
        )
    ]
    # enough 5m history for ema
    bars = []
    for i in range(40):
        mins = 9 * 60 + 15 + (i % 60) * 5
        h, m = divmod(mins, 60)
        bars.append(_bar(min(h, 14), m % 60, 99, 100, 98, 99.0 + i * 0.01, d=prev))
    bars.extend([first, second, third])
    setup = detect_setup([first, second, third], SESSION, prev_high=100.0, prev_low=90.0)
    assert setup is not None
    feats = build_entry_features(setup, bars, daily, atr=5.0)
    assert feats["prev_close"] == 100.0
    assert abs(feats["move_prev_close_to_entry_pct"] - 3.0) < 1e-9
    assert feats["ema30_5m"] is not None
    assert feats["first_range_atr"] is not None


def test_open_eq_extreme_no():
    first = _bar(9, 15, 100.5, 102, 100, 101.5, vol=1000)
    assert open_eq_extreme_flag("BULLISH", first) == "NO"
    first_b = _bar(9, 15, 99.5, 100, 97, 98, vol=1000)
    assert open_eq_extreme_flag("BEARISH", first_b) == "NO"

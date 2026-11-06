"""MTF Swing engine, CSV, and dashboard tests. No network."""
from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path

import pytz

from backend.services.mtf_swing.bars import (
    Bar,
    back_adjust_to_last,
    filter_session_bars,
    week_key,
)
from backend.services.mtf_swing.config import MtfSwingConfig
from backend.services.mtf_swing.csv_store import append_trades, read_rows
from backend.services.mtf_swing.engine import run_symbol
from backend.services.mtf_swing.levels import (
    WeekStats,
    candidate_side,
    pick_swing_high,
    pick_swing_low,
    swing_high_indexes,
    week_stats,
)
from backend.services.mtf_swing.metrics import build_dashboard

IST = pytz.timezone("Asia/Kolkata")


def bar(day: date, o, h, l, c, contract: str = "M1", hh: int = 15, mm: int = 15) -> Bar:
    return Bar(
        ts=IST.localize(datetime(day.year, day.month, day.day, hh, mm)),
        open=o,
        high=h,
        low=l,
        close=c,
        contract=contract,
    )


def days_from(start: date, n: int):
    out = []
    d = start
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


def _wk(low, high, close) -> WeekStats:
    return WeekStats(
        key=(2025, 46),
        low=low,
        high=high,
        close=close,
        last_date=date(2025, 11, 14),
        last_ts=IST.localize(datetime(2025, 11, 14, 15, 15)),
        basis_contract="M",
        adjusted=[],
    )


def test_weekly_aggregation_skips_holiday_session():
    hol = {date(2025, 11, 14)}
    bars = [
        bar(date(2025, 11, 10), 10, 12, 9, 11),
        bar(date(2025, 11, 11), 11, 14, 10, 13),
        bar(date(2025, 11, 12), 13, 13, 8, 9),
        bar(date(2025, 11, 13), 9, 15, 9, 14),
        bar(date(2025, 11, 14), 1, 100, 0.5, 1),
    ]
    kept = filter_session_bars(bars, hol)
    assert [b.session_date for b in kept] == [
        date(2025, 11, 10),
        date(2025, 11, 11),
        date(2025, 11, 12),
        date(2025, 11, 13),
    ]
    stats = week_stats(kept)
    assert stats is not None
    assert stats.low == 8
    assert stats.high == 15
    assert stats.close == 14
    assert stats.last_date == date(2025, 11, 13)


def test_screening_equality_edges():
    w2 = _wk(100, 130, 115)
    assert candidate_side(_wk(90, 120, 106), w2) == "LONG"
    assert candidate_side(_wk(100, 120, 106), w2) is None  # W1.low == W2.low
    assert candidate_side(_wk(90, 120, 100), w2) is None  # close == W2.low
    assert candidate_side(_wk(90, 120, 130), w2) is None  # close == W2.high
    assert candidate_side(_wk(90, 130, 106), w2) == "LONG"  # W1.high == W2.high allowed
    assert candidate_side(_wk(90, 131, 106), w2) is None

    w2s = _wk(100, 120, 110)
    assert candidate_side(_wk(100, 130, 110), w2s) == "SHORT"
    assert candidate_side(_wk(99, 130, 110), w2s) is None  # W1.low < W2.low
    assert candidate_side(_wk(100, 120, 110), w2s) is None  # W1.high == W2.high, not short
    assert candidate_side(_wk(100, 130, 100), w2s) is None  # close == W2.low
    assert candidate_side(_wk(100, 130, 120), w2s) is None  # close == W2.high


def _hl_bars(pairs):
    start = date(2025, 11, 17)
    ds = days_from(start, len(pairs))
    return [bar(ds[i], pairs[i][0], pairs[i][0], pairs[i][1], pairs[i][1]) for i in range(len(pairs))]


def test_swing_cluster_and_no_lookahead():
    clustered = _hl_bars([(10, 9), (12, 10), (11, 8), (12.03, 10), (11, 7)])
    sweep_ts = clustered[-1].ts
    idx = pick_swing_high(clustered, sweep_ts)
    assert idx == 3
    assert clustered[idx].high == 12.03

    recent = _hl_bars([(10, 9), (13, 10), (11, 8), (12, 10), (11, 7)])
    idx = pick_swing_high(recent, recent[-1].ts)
    assert idx == 3
    assert recent[idx].high == 12

    # Exactly 0.3% → take the higher (older) swing. Just outside → most recent.
    exact = _hl_bars([(10, 9), (100.3, 10), (90, 8), (100, 10), (90, 7)])
    idx = pick_swing_high(exact, exact[-1].ts)
    assert idx == 1
    outside = _hl_bars([(10, 9), (100.4, 10), (90, 8), (100, 10), (90, 7)])
    idx = pick_swing_high(outside, outside[-1].ts)
    assert idx == 3

    lows = _hl_bars([(20, 10.03), (19, 12), (20, 10), (19, 11), (18, 9)])
    # Build explicit lows: swing lows at index 1 (lower) and index 3 within 0.3%.
    start = date(2025, 11, 17)
    ds = days_from(start, 5)
    # highs flat-ish, lows: 12, 10.03, 11, 10, 9 (sweep at last)
    # index 1 low 10.03 < 12 and < 11. index 3 low 10 < 11 and < 9? 10 < 9 is false.
    low_bars = [
        bar(ds[0], 20, 20, 12, 15),
        bar(ds[1], 15, 16, 10.03, 14),
        bar(ds[2], 14, 15, 11, 13),
        bar(ds[3], 13, 14, 10, 12),
        bar(ds[4], 12, 13, 11.5, 12),
    ]
    # sweep high is index 0, no swing low before it. Use a later sweep.
    low_bars[-1] = bar(ds[4], 12, 30, 11.5, 20)
    idx = pick_swing_low(low_bars, low_bars[-1].ts)
    # swings at 1 (10.03) and 3 (10). |10.03-10|/10 = 0.003 → take the lower (10).
    assert idx == 3
    assert low_bars[idx].low == 10

    partial = [bar(ds[i], 5, h, 1, 4) for i, h in enumerate((5, 10, 7))]
    assert swing_high_indexes(partial[:2]) == []
    assert swing_high_indexes(partial) == [1]


def _w2_days():
    return days_from(date(2025, 11, 10), 5)


def _w1_long_days():
    return days_from(date(2025, 11, 17), 5)


def long_context_bars():
    """W2 Nov 10-14 and W1 Nov 17-21 that screen a long with trigger 104, SL 90, T1 130."""
    bars = []
    for d in _w2_days():
        bars.append(bar(d, 110, 130, 100, 115))
    w1 = _w1_long_days()
    bars += [
        bar(w1[0], 100, 100, 100, 100),
        bar(w1[1], 100, 104, 99, 103),
        bar(w1[2], 103, 102, 90, 95),
        bar(w1[3], 95, 101, 92, 98),
        bar(w1[4], 98, 103, 96, 106),
    ]
    return bars


def test_rr_direct_and_limit_entries():
    entry_days = days_from(date(2025, 11, 24), 3)
    direct_bars = long_context_bars() + [
        bar(entry_days[0], 100, 104, 100, 103),
        bar(entry_days[1], 103, 108, 100, 105),  # close 105 > 104, rr > 1
        bar(entry_days[2], 107, 109, 106, 108),
    ]
    trades, _, _ = run_symbol(
        direct_bars,
        symbol="AAA",
        active_from=date(2025, 11, 1),
        active_to=date(2025, 11, 30),
        config=MtfSwingConfig(entry_timing="next_open", timeframe="15m"),
        lot_size=10,
        finalize_open=True,
    )
    assert len(trades) == 1
    assert trades[0]["entry_type"] == "direct"
    assert trades[0]["entry_price"] == "107.0000"
    assert trades[0]["entry_time"].startswith("2025-11-26")
    assert float(trades[0]["sl"]) == 90
    assert float(trades[0]["t1"]) == 130

    close_trades, _, _ = run_symbol(
        direct_bars,
        symbol="AAA",
        active_from=date(2025, 11, 1),
        active_to=date(2025, 11, 30),
        config=MtfSwingConfig(entry_timing="close", timeframe="15m"),
        lot_size=10,
        finalize_open=True,
    )
    assert close_trades[0]["entry_type"] == "direct"
    assert close_trades[0]["entry_price"] == "105.0000"
    assert close_trades[0]["entry_time"].startswith("2025-11-25")

    limit_bars = long_context_bars() + [
        bar(entry_days[0], 100, 104, 100, 103),
        bar(entry_days[1], 104, 121, 100, 120),  # rr < 1 → limit 110
        bar(entry_days[2], 115, 118, 109, 112),
    ]
    limit_trades, _, _ = run_symbol(
        limit_bars,
        symbol="AAA",
        active_from=date(2025, 11, 1),
        active_to=date(2025, 11, 30),
        config=MtfSwingConfig(timeframe="60m"),
        lot_size=5,
        finalize_open=True,
    )
    assert limit_trades[0]["entry_type"] == "limit"
    assert limit_trades[0]["entry_price"] == "110.0000"
    assert limit_trades[0]["qty_lots"] == "1"


def test_exits_sl_t1_time_same_bar_and_gap():
    entry_days = days_from(date(2025, 11, 24), 5)
    base = long_context_bars() + [
        bar(entry_days[0], 100, 104, 100, 103),
        bar(entry_days[1], 103, 108, 100, 105),
    ]
    cfg = MtfSwingConfig(entry_timing="next_open", timeframe="15m")

    fill = bar(entry_days[2], 107, 109, 106, 108)
    sl_bars = base + [fill, bar(entry_days[3], 100, 108, 89, 100)]
    sl, _, _ = run_symbol(sl_bars, symbol="AAA", active_from=date(2025, 11, 1), active_to=date(2025, 12, 31), config=cfg, lot_size=2)
    assert sl[0]["exit_reason"] == "SL"
    assert sl[0]["exit_price"] == "90.0000"
    assert sl[0]["sessions_held"] == "2"

    gap_bars = base + [fill, bar(entry_days[3], 80, 88, 79, 85)]
    gap, _, _ = run_symbol(gap_bars, symbol="AAA", active_from=date(2025, 11, 1), active_to=date(2025, 12, 31), config=cfg, lot_size=2)
    assert gap[0]["exit_reason"] == "SL"
    assert gap[0]["exit_price"] == "80.0000"

    both = base + [fill, bar(entry_days[3], 100, 140, 80, 100)]
    both_tr, _, _ = run_symbol(both, symbol="AAA", active_from=date(2025, 11, 1), active_to=date(2025, 12, 31), config=cfg, lot_size=2)
    assert both_tr[0]["exit_reason"] == "SL"
    assert both_tr[0]["exit_price"] == "90.0000"

    t1_bars = base + [fill, bar(entry_days[3], 110, 131, 106, 129)]
    t1, _, _ = run_symbol(t1_bars, symbol="AAA", active_from=date(2025, 11, 1), active_to=date(2025, 12, 31), config=cfg, lot_size=2)
    assert t1[0]["exit_reason"] == "T1"
    assert t1[0]["exit_price"] == "130.0000"

    gap_t1 = base + [fill, bar(entry_days[3], 135, 140, 132, 136)]
    gt, _, _ = run_symbol(gap_t1, symbol="AAA", active_from=date(2025, 11, 1), active_to=date(2025, 12, 31), config=cfg, lot_size=2)
    assert gt[0]["exit_reason"] == "T1"
    assert gt[0]["exit_price"] == "135.0000"

    # Entry Monday close counts as session 1; TIME on the 10th session close.
    calm = []
    d = entry_days[0]
    sessions = []
    while len(sessions) < 10:
        if d.weekday() < 5:
            sessions.append(d)
        d += timedelta(days=1)
    time_bars = long_context_bars()
    for i, day in enumerate(sessions):
        if i == 0:
            time_bars.append(bar(day, 100, 108, 100, 105))
        else:
            time_bars.append(bar(day, 106, 110, 100, 107))
    timed, _, _ = run_symbol(
        time_bars,
        symbol="AAA",
        active_from=date(2025, 11, 1),
        active_to=date(2025, 12, 31),
        config=MtfSwingConfig(entry_timing="close", timeframe="60m"),
        lot_size=1,
    )
    assert timed[0]["exit_reason"] == "TIME"
    assert timed[0]["sessions_held"] == "10"
    assert timed[0]["exit_price"] == "107.0000"
    assert timed[0]["exit_time"].startswith(sessions[-1].isoformat())


def test_short_breakdown_and_roll_gap_is_not_a_fill():
    w2 = days_from(date(2025, 11, 10), 5)
    w1 = days_from(date(2025, 11, 17), 5)
    bars = [bar(d, 110, 120, 100, 110) for d in w2]
    # Swing low 125 on Tue, sweep high 140 on Thu, Friday close inside W2.
    bars += [
        bar(w1[0], 129, 130, 128, 129),
        bar(w1[1], 126, 127, 125, 126),
        bar(w1[2], 126, 129, 128, 128),
        bar(w1[3], 128, 140, 127, 135),
        bar(w1[4], 120, 122, 112, 110),
    ]
    entry = days_from(date(2025, 11, 24), 2)
    monday = bar(entry[0], 124, 124, 120, 122)
    bars.append(monday)
    trades, _, _ = run_symbol(
        bars + [bar(entry[1], 121, 123, 119, 120)],
        symbol="BBB",
        active_from=date(2025, 11, 1),
        active_to=date(2025, 11, 30),
        config=MtfSwingConfig(entry_timing="next_open", timeframe="15m"),
        lot_size=4,
        finalize_open=True,
    )
    assert trades[0]["side"] == "SHORT"
    assert trades[0]["entry_type"] == "direct"
    assert trades[0]["entry_price"] == "121.0000"
    assert float(trades[0]["sl"]) == 140
    assert float(trades[0]["t1"]) == 100
    assert trades[0]["exit_reason"] == "OPEN"

    # Close-entry on Monday, then a roll gap that would pierce the unadjusted SL.
    held, _, state = run_symbol(
        bars,
        symbol="BBB",
        active_from=date(2025, 11, 1),
        active_to=date(2025, 11, 30),
        config=MtfSwingConfig(entry_timing="close", timeframe="15m"),
        lot_size=4,
        finalize_open=False,
    )
    assert held == []
    assert state["position"]["side"] == "SHORT"
    rolled, _, _ = run_symbol(
        bars + [bar(entry[1], 200, 210, 190, 205, contract="M2")],
        symbol="BBB",
        active_from=date(2025, 11, 1),
        active_to=date(2025, 12, 31),
        state=state,
        config=MtfSwingConfig(entry_timing="close", timeframe="15m"),
        lot_size=4,
        finalize_open=True,
    )
    assert rolled[0]["exit_reason"] == "OPEN"
    # Back-adjust removes the jump and leaves the last bar raw.
    stitched = back_adjust_to_last(
        [bar(date(2025, 11, 24), 100, 100, 100, 100, contract="A"), bar(date(2025, 11, 25), 80, 85, 79, 82, contract="B")]
    )
    assert stitched[-1].close == 82
    assert stitched[0].close == 100 + (80 - 100)


def _carry_series():
    """Limit fill on Fri 28 Nov 2025, T1 on Tue 2 Dec."""
    bars = long_context_bars()
    nov = days_from(date(2025, 11, 24), 5)
    bars += [
        bar(nov[0], 100, 104, 100, 103),
        bar(nov[1], 103, 104, 100, 104),
        bar(nov[2], 104, 106, 100, 104),
        bar(nov[3], 104, 121, 100, 120),
        bar(nov[4], 115, 118, 109, 112),
    ]
    bars += [
        bar(date(2025, 12, 1), 112, 120, 111, 118),
        bar(date(2025, 12, 2), 118, 131, 117, 128),
    ]
    return bars


def _sig(trades):
    return [
        (
            t["trade_id"],
            t["entry_time"],
            t["entry_price"],
            t["exit_time"],
            t["exit_price"],
            t["exit_reason"],
            t["month"],
            t["side"],
            t["net_pnl"],
        )
        for t in trades
    ]


def test_month_batch_matches_continuous_and_carries_exit():
    bars = _carry_series()
    cfg = MtfSwingConfig(timeframe="60m")
    continuous, _, _ = run_symbol(
        bars,
        symbol="CCC",
        active_from=date(2025, 11, 1),
        active_to=date(2025, 12, 31),
        config=cfg,
        lot_size=250,
    )
    assert len(continuous) == 1
    assert continuous[0]["month"] == "2025-11"
    assert continuous[0]["entry_time"].startswith("2025-11-28")
    assert continuous[0]["exit_time"].startswith("2025-12-02")
    assert continuous[0]["exit_reason"] == "T1"
    assert continuous[0]["entry_type"] == "limit"
    assert continuous[0]["net_pnl"] == "5000.00"  # (130-110) * 250

    nov_bars = [b for b in bars if b.session_date.month == 11]
    dec_bars = [b for b in bars if b.session_date >= date(2025, 11, 10)]
    t1, d1, state = run_symbol(
        nov_bars,
        symbol="CCC",
        active_from=date(2025, 11, 1),
        active_to=date(2025, 11, 30),
        config=cfg,
        lot_size=250,
        finalize_open=False,
    )
    assert t1 == []
    assert state["position"]["entry_month"] == "2025-11"
    t2, _, _ = run_symbol(
        dec_bars,
        symbol="CCC",
        active_from=date(2025, 12, 1),
        active_to=date(2025, 12, 31),
        state=state,
        config=cfg,
        lot_size=250,
    )
    assert _sig(t1 + t2) == _sig(continuous)
    assert d1  # week expiries or structural discards may exist; entry itself is not a discard


def test_csv_idempotent_append_does_not_touch_prior_bytes(tmp_path: Path):
    cfg_row = {
        "trade_id": "CCC|60m|LONG|2025-11-28T15:15:00+05:30",
        "timeframe": "60m",
        "month": "2025-11",
        "symbol": "CCC",
        "side": "LONG",
        "screen_date": "2025-11-21",
        "entry_time": "2025-11-28T15:15:00+05:30",
        "entry_price": "110.0000",
        "entry_type": "limit",
        "lot_size": "250",
        "qty_lots": "1",
        "sl": "90.0000",
        "t1": "130.0000",
        "exit_time": "2025-12-02T15:15:00+05:30",
        "exit_price": "130.0000",
        "exit_reason": "T1",
        "sessions_held": "3",
        "gross_pnl": "5000.00",
        "charges": "0.00",
        "net_pnl": "5000.00",
        "r_multiple": "1.0000",
        "run_ts": "t1",
    }
    path = tmp_path / "trades.csv"
    assert append_trades(path, [cfg_row]) == 1
    before = path.read_bytes()
    assert append_trades(path, [dict(cfg_row)]) == 0
    assert path.read_bytes() == before
    other = dict(cfg_row)
    other["trade_id"] = "CCC|60m|LONG|2025-12-10T15:15:00+05:30"
    other["entry_time"] = "2025-12-10T15:15:00+05:30"
    other["month"] = "2025-12"
    other["net_pnl"] = "-10.00"
    other["exit_reason"] = "SL"
    assert append_trades(path, [other]) == 1
    after = path.read_bytes()
    assert after.startswith(before)
    rows = read_rows(path)
    assert rows[0]["net_pnl"] == "5000.00"
    assert rows[0]["run_ts"] == "t1"
    dash = build_dashboard(rows, timeframe="60m")
    assert dash["summary"]["trades"] == 2
    assert dash["summary"]["net_pnl"] == 4990
    assert dash["summary"]["profitable"] == 1
    assert dash["summary"]["losing"] == 1
    nov = next(m for m in dash["months"] if m["month"] == "2025-11")
    assert nov["trades"] == 1
    assert nov["net_pnl"] == 5000
    assert dash["months"][0]["month"] == "2025-11"
    assert dash["months"][-1]["month"] == "2026-10"

    open_row = dict(cfg_row)
    open_row["trade_id"] = "CCC|60m|LONG|open"
    open_row["exit_reason"] = "OPEN"
    open_row["exit_time"] = ""
    open_row["exit_price"] = ""
    open_row["net_pnl"] = ""
    open_row["gross_pnl"] = ""
    dash_open = build_dashboard(rows + [open_row], timeframe="60m")
    assert dash_open["summary"]["trades"] == 2
    assert dash_open["open_trades"] == 1
    assert dash_open["charts"]["exit_reasons"]["OPEN"] == 1
    assert dash_open["charts"]["sides"]["LONG"] == 2


def test_expired_contract_payload_uses_dated_key_and_lot():
    from datetime import date as d

    from backend.services.mtf_swing.data import contract_from_response

    payload = {
        "status": "success",
        "data": [
            {
                "instrument_key": "NSE_FO|49078|25-11-2025",
                "lot_size": 500,
                "trading_symbol": "RELIANCE FUT 25 NOV 25",
            }
        ],
    }
    found = contract_from_response(payload, d(2025, 11, 25))
    assert found["instrument_key"] == "NSE_FO|49078|25-11-2025"
    assert found["lot_size"] == 500
    assert contract_from_response({"status": "success", "data": [{"instrument_key": "NSE_FO|RELIANCE25NOVFUT"}]}, d(2025, 11, 25)) is None


def test_30m_pairs_into_60m_without_crossing_sessions():
    from backend.services.mtf_swing.data import aggregate_30m_to_60m

    d1 = date(2025, 11, 10)
    d2 = date(2025, 11, 11)
    bars = [
        bar(d1, 10, 12, 9, 11, hh=9, mm=15),
        bar(d1, 11, 14, 10, 13, hh=9, mm=45),
        bar(d1, 13, 15, 12, 14, hh=10, mm=15),
        bar(d2, 20, 21, 19, 20, hh=9, mm=15),
    ]
    out = aggregate_30m_to_60m(bars)
    assert len(out) == 3
    assert out[0].open == 10 and out[0].high == 14 and out[0].low == 9 and out[0].close == 13
    assert out[0].ts.hour == 9 and out[0].ts.minute == 15
    assert out[2].session_date == d2


def test_drawdown_and_profit_factor_from_csv_rows():
    rows = []
    nets = [10, -4, -3, 2]
    for i, n in enumerate(nets):
        rows.append(
            {
                "trade_id": f"S|15m|LONG|{i}",
                "timeframe": "15m",
                "month": "2025-11",
                "symbol": "S",
                "side": "LONG" if i % 2 == 0 else "SHORT",
                "exit_time": f"2025-11-{10+i:02d}T15:15:00+05:30",
                "exit_reason": "T1" if n > 0 else "SL",
                "net_pnl": f"{n:.2f}",
            }
        )
    dash = build_dashboard(rows, timeframe="15m")
    s = dash["summary"]
    assert s["trades"] == 4
    assert s["net_pnl"] == 5
    assert s["avg_pnl"] == 1.25
    assert s["win_rate"] == 0.5
    assert s["profit_factor"] == 12 / 7
    assert s["max_profit"] == 10
    assert s["max_loss"] == -4
    assert s["max_drawdown"] == 7
    assert s["profitable"] == 2
    assert s["losing"] == 2
    assert week_key(date(2025, 11, 28)) == week_key(date(2025, 11, 24))

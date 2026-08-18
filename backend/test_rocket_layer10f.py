"""Layer 10f Pine scoring — unit tests (does not touch live Rocket/Crash)."""
from datetime import datetime, timedelta

import pytz

from backend.services.rocket_layer10f import (
    attach_forward_outcomes,
    events_from_scored,
    score_bars,
    session_phase,
)
from backend.services.rocket_layer10f_backtest import build_summary_rows

IST = pytz.timezone("Asia/Kolkata")


def _bar(ts: str, o, h, lo, c, v=1000.0):
    return {"timestamp": ts, "open": o, "high": h, "low": lo, "close": c, "volume": v}


def _session_times(day: int, n: int):
    start = IST.localize(datetime(2026, 8, day, 9, 15, 0))
    return [(start + timedelta(minutes=10 * i)).isoformat() for i in range(n)]


def test_session_phase_buckets():
    assert session_phase(1) == "early"
    assert session_phase(5) == "early"
    assert session_phase(6) == "mid"
    assert session_phase(15) == "mid"
    assert session_phase(16) == "late"


def test_sess_bar_and_cum_delta_reset_each_date():
    t1 = _session_times(17, 4)
    t2 = _session_times(18, 4)
    day1 = [_bar(t1[i], 100, 101, 99, 100.8, v=1000) for i in range(4)]
    day2 = [_bar(t2[i], 100, 101, 99, 100.2, v=1000) for i in range(4)]
    rows = score_bars(day1 + day2)
    assert rows[0]["sess_bar_number"] == 1
    assert rows[3]["sess_bar_number"] == 4
    assert rows[4]["session_date"].isoformat() == "2026-08-18"
    assert rows[4]["sess_bar_number"] == 1
    assert rows[4]["cum_delta_session"] < rows[3]["cum_delta_session"]


def test_s1_seller_failure():
    times = _session_times(18, 6)
    rows_in = [_bar(times[i], 100, 101, 99, 100.4, 800) for i in range(5)]
    prev = 100.4
    rows_in.append(_bar(times[5], prev + 0.12, prev + 0.14, prev - 0.10, prev + 0.06, 800))
    scored = score_bars(rows_in)
    last = scored[-1]
    assert last["s1"] is True
    assert last["close"] < last["open"]
    assert last["close"] >= scored[-2]["close"]


def test_over_extended_suppresses_long():
    times = _session_times(18, 20)
    bars = []
    px = 100.0
    for i, ts in enumerate(times):
        if i < 19:
            bars.append(_bar(ts, px, px + 0.1, px - 0.1, px + 0.05, 500))
            px += 0.05
        else:
            bars.append(_bar(ts, px, px + 20.0, px - 0.05, px + 19.0, 500))
    scored = score_bars(bars)
    assert scored[-1]["over_extended"] is True
    assert scored[-1]["score_long"] == 0


def test_s4_volume_wakeup():
    times = _session_times(18, 8)
    bars = [_bar(times[i], 100, 100.4, 99.8, 100.2, v=400) for i in range(7)]
    bars[6] = _bar(times[6], 100, 100.3, 99.9, 100.1, v=300)
    bars.append(_bar(times[7], 100.1, 101.0, 100.05, 100.9, v=2000))
    last = score_bars(bars)[-1]
    assert last["prior_quiet"] is True
    assert last["s4"] is True


def test_bs1_buyer_failure():
    times = _session_times(18, 6)
    rows_in = [_bar(times[i], 100, 101, 99, 100.4, 800) for i in range(5)]
    prev = 100.4
    rows_in.append(_bar(times[5], 100.0, 101.0, 99.5, 100.15, 800))
    last = score_bars(rows_in)[-1]
    assert last["close"] > last["open"]
    assert last["bs1"] is True


def test_forward_returns_same_session_only():
    t1 = _session_times(17, 6)
    t2 = _session_times(18, 3)
    bars = [_bar(t1[i], 100 + i, 101 + i, 99 + i, 100.5 + i, 500) for i in range(6)]
    bars += [_bar(t2[i], 200, 201, 199, 200.5, 500) for i in range(3)]
    rows = score_bars(bars)
    attach_forward_outcomes(rows)
    last_d1 = [r for r in rows if r["session_date"].isoformat() == "2026-08-17"][-1]
    assert last_d1["fwd_ret_1bar"] is None
    first = rows[0]
    assert first["fwd_ret_1bar"] is not None
    expected = (rows[1]["close"] - rows[0]["close"]) / rows[0]["close"]
    assert abs(first["fwd_ret_1bar"] - expected) < 1e-12


def test_events_threshold_and_summary():
    times = _session_times(18, 12)
    bars = []
    for i, ts in enumerate(times):
        v = 400 if i < 11 else 3000
        c = 100.2 if i < 11 else 101.0
        h = 100.4 if i < 11 else 101.2
        o = 100.0 if i < 11 else 100.2
        lo = 99.8 if i < 11 else 100.15
        bars.append(_bar(ts, o, h, lo, c, v))
    rows = score_bars(bars)
    attach_forward_outcomes(rows)
    evs = events_from_scored(rows, "TEST")
    for e in evs:
        assert e["score_long"] >= 2 or e["score_short"] >= 2
        assert e["symbol"] == "TEST"
        assert e["event_id"]
    for e in evs:
        e["adx_at_signal"] = 25.0
    summary = build_summary_rows(evs)
    assert isinstance(summary, list)
    if evs and any(e["score_long"] in (2, 3, 4) or e["score_short"] in (2, 3, 4) for e in evs):
        assert summary


def test_delta_slope_needs_four_session_bars():
    times = _session_times(18, 3)
    bars = [_bar(times[i], 100, 101, 99, 100.8, 1000) for i in range(3)]
    rows = score_bars(bars)
    assert all(r["delta_slope"] is None for r in rows)
    assert all(r["s2"] is False for r in rows)

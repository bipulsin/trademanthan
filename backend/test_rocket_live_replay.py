"""Replay of live compute_rocket_crash on REST OHLCV (no production scorer changes)."""
from datetime import date, datetime, timedelta

import pytz

from backend.services.rocket_live_replay import (
    _fwd_ret,
    _mfe_mae,
    build_summary_rows,
    replay_symbol_sessions,
)
from backend.services.rocket_pre_ignition import compute_rocket_crash
def _bar(o, h, lo, c, v=1000.0):
    return {"open": o, "high": h, "low": lo, "close": c, "volume": v}


def _coil_base(n=20, start=100.0):
    rows = []
    px = start
    for _ in range(n):
        o = px
        c = px + 0.15
        rows.append(_bar(o, c + 0.05, o - 0.40, c, v=800))
        px = c
    return rows

IST = pytz.timezone("Asia/Kolkata")


def _session_times(day: int, n: int):
    start = IST.localize(datetime(2026, 8, day, 9, 15, 0))
    return [(start + timedelta(minutes=10 * i)).isoformat() for i in range(n)]


def test_uses_exact_compute_rocket_crash():
    from backend.services.rocket_live_replay import compute_rocket_crash as imported

    assert imported is compute_rocket_crash


def test_forward_same_session_and_mfe_price_points():
    closes = [100.0, 101.0, 102.0, 99.0, 103.0, 104.0]
    highs = [100.5, 101.5, 102.5, 99.5, 103.5, 104.5]
    lows = [99.5, 100.5, 101.5, 98.5, 102.5, 103.5]
    last = 5
    assert abs(_fwd_ret(closes, 0, 1, last) - 0.01) < 1e-12
    mfe, mae = _mfe_mae(highs, lows, 100.0, 0, last, "long", 5)
    assert abs(mfe - 4.5) < 1e-12
    assert abs(mae - (-1.5)) < 1e-12
    mfe_s, mae_s = _mfe_mae(highs, lows, 100.0, 0, last, "short", 5)
    assert abs(mfe_s - 1.5) < 1e-12
    assert abs(mae_s - (-4.5)) < 1e-12


def test_no_overnight_forward():
    closes = [100.0, 101.0, 102.0, 200.0]
    assert _fwd_ret(closes, 2, 1, last=2) is None
    assert _fwd_ret(closes, 2, 1, last=3) is not None


def test_summary_exclusive_buckets():
    events = [
        {
            "score_bucket": 2,
            "side": "long",
            "session_phase": "late",
            "adx_at_signal": 25.0,
            "fwd_ret_1bar": 0.01,
            "fwd_ret_3bar": 0.02,
            "fwd_ret_5bar": 0.03,
            "fwd_mfe_5bar": 1.0,
            "fwd_mae_5bar": -0.5,
            "fwd_direction_correct_1bar": True,
            "fwd_direction_correct_3bar": True,
        },
        {
            "score_bucket": 3,
            "side": "short",
            "session_phase": "mid",
            "adx_at_signal": 35.0,
            "fwd_ret_1bar": -0.01,
            "fwd_ret_3bar": -0.02,
            "fwd_ret_5bar": 0.01,
            "fwd_mfe_5bar": 2.0,
            "fwd_mae_5bar": -1.0,
            "fwd_direction_correct_1bar": True,
            "fwd_direction_correct_3bar": True,
        },
    ]
    rows = build_summary_rows(events)
    assert len(rows) == 2
    by = {(r["score_bucket"], r["side"]): r for r in rows}
    assert by[(2, "long")]["signal_count"] == 1
    assert by[(3, "short")]["win_rate_1bar"] == 1.0


def test_coil_plus_seller_failure_logs_via_live_scorer():
    times = _session_times(18, 21)
    rows = _coil_base(n=20)
    candles = []
    for i, bar in enumerate(rows):
        candles.append(
            {
                "timestamp": times[i],
                "open": bar["open"],
                "high": bar["high"],
                "low": bar["low"],
                "close": bar["close"],
                "volume": bar["volume"],
            }
        )
    prev_c = candles[-1]["close"]
    candles.append(
        {
            "timestamp": times[20],
            "open": prev_c + 0.4,
            "high": prev_c + 0.5,
            "low": prev_c - 0.2,
            "close": prev_c + 0.05,
            "volume": 900,
        }
    )
    ohlcv = [{k: c[k] for k in ("open", "high", "low", "close", "volume")} for c in candles]
    live = compute_rocket_crash(ohlcv, session_bar_count=21)
    events = replay_symbol_sessions(candles, "TEST", date(2026, 8, 18), date(2026, 8, 18))
    if live["rocket_score"] >= 2 or live["crash_score"] >= 2:
        assert events
        assert events[-1]["rocket_score"] == live["rocket_score"]
        assert events[-1]["crash_score"] == live["crash_score"]
    assert "delta" not in candles[-1]

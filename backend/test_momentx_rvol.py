"""Unit tests for MomentX backtest RVOL (informational columns only)."""
from __future__ import annotations

from datetime import date, datetime, time as dt_time, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pytz

from backend.services.momentx.rvol import (
    BAR_MINUTES_5,
    CandleCache,
    baseline_contract_keys,
    compute_rvol_metrics,
    pick_front_next_for_session,
    prior_sessions,
    session_bars,
)

IST = pytz.timezone("Asia/Kolkata")


def _bar(
    d: date,
    hh: int,
    mm: int,
    *,
    volume: float,
    close: float = 100.0,
    high: Optional[float] = None,
    low: Optional[float] = None,
) -> Dict[str, Any]:
    ts = IST.localize(datetime.combine(d, dt_time(hh, mm)))
    c = float(close)
    return {
        "timestamp": ts.isoformat(),
        "open": c,
        "high": float(high if high is not None else c + 0.5),
        "low": float(low if low is not None else c - 0.5),
        "close": c,
        "volume": float(volume),
    }


def _session_5m_flat(
    d: date,
    *,
    through_T: Tuple[int, int],
    total_vol: float,
    close: float = 100.0,
) -> List[Dict[str, Any]]:
    """Evenly distribute total_vol across 5m bars from 09:15 through T."""
    open_dt = IST.localize(datetime.combine(d, dt_time(9, 15)))
    t_end = IST.localize(datetime.combine(d, dt_time(through_T[0], through_T[1])))
    n = int((t_end - open_dt).total_seconds() // 300)
    assert n > 0
    each = float(total_vol) / float(n)
    out: List[Dict[str, Any]] = []
    for i in range(n):
        ts = open_dt + timedelta(minutes=5 * i)
        out.append(
            _bar(
                d,
                ts.hour,
                ts.minute,
                volume=each,
                close=close + i * 0.01,
            )
        )
    return out


def test_sunpharma_fixture_rvol_cum():
    """VALIDATION: SUNPHARMA 30-Oct-2026 T=11:00 — only 2 prior sessions → rvol null,
    but cum / baseline match; with min_sessions=2 rvol_cum=2.68.
    """
    trade = date(2026, 10, 30)
    T = (11, 0)
    today = _session_5m_flat(trade, through_T=T, total_vol=787_150)
    # extend warm bars for ATR (prior days same series)
    warm = []
    for d in (date(2026, 10, 27), date(2026, 10, 28), date(2026, 10, 29)):
        warm.extend(_session_5m_flat(d, through_T=(15, 30), total_vol=200_000))
    trade_candles = warm + today

    prior = [
        (date(2026, 10, 28), [_session_5m_flat(date(2026, 10, 28), through_T=T, total_vol=322_700)]),
        (date(2026, 10, 29), [_session_5m_flat(date(2026, 10, 29), through_T=T, total_vol=263_900)]),
    ]
    m = compute_rvol_metrics(
        trade_date=trade,
        T=T,
        side="bullish",
        trade_day_candles=trade_candles,
        prior_session_candle_lists=prior,
        bar_minutes=BAR_MINUTES_5,
        min_sessions=3,
    )
    assert abs(m["cum_vol_at_T"] - 787_150) < 1.0
    assert abs(m["baseline_cum_vol"] - 293_300) < 1.0
    assert m["n_sessions_used"] == 2
    assert m["rvol_cum"] is None  # fewer than 3 sessions

    m2 = compute_rvol_metrics(
        trade_date=trade,
        T=T,
        side="bullish",
        trade_day_candles=trade_candles,
        prior_session_candle_lists=prior,
        bar_minutes=BAR_MINUTES_5,
        min_sessions=2,
    )
    assert abs(m2["rvol_cum"] - 2.68) < 0.02


def test_infy_fixture_rvol_cum():
    """VALIDATION: INFY 30-Oct-2026 T=09:30 with min_sessions=2 → rvol_cum≈0.83."""
    trade = date(2026, 10, 30)
    T = (9, 30)
    today = _session_5m_flat(trade, through_T=T, total_vol=1_220_800)
    warm = []
    for d in (date(2026, 10, 27), date(2026, 10, 28), date(2026, 10, 29)):
        warm.extend(_session_5m_flat(d, through_T=(15, 30), total_vol=500_000))
    trade_candles = warm + today
    prior = [
        (date(2026, 10, 28), [_session_5m_flat(date(2026, 10, 28), through_T=T, total_vol=2_252_800)]),
        (date(2026, 10, 29), [_session_5m_flat(date(2026, 10, 29), through_T=T, total_vol=694_000)]),
    ]
    m = compute_rvol_metrics(
        trade_date=trade,
        T=T,
        side="bullish",
        trade_day_candles=trade_candles,
        prior_session_candle_lists=prior,
        bar_minutes=BAR_MINUTES_5,
        min_sessions=2,
    )
    assert abs(m["cum_vol_at_T"] - 1_220_800) < 1.0
    assert abs(m["baseline_cum_vol"] - 1_473_400) < 1.0
    assert abs(m["rvol_cum"] - 0.83) < 0.02


def test_holiday_skipping_in_prior_sessions():
    # Diwali-style: Friday holiday between Thu and Mon
    trade = date(2026, 10, 30)  # Fri
    holidays = {date(2026, 10, 29)}  # treat Thu as holiday
    sess = prior_sessions(trade, n=3, holidays=holidays)
    assert date(2026, 10, 29) not in sess
    assert date(2026, 10, 28) in sess
    assert date(2026, 10, 27) in sess


def test_fewer_than_three_sessions_null_rvol():
    trade = date(2026, 10, 30)
    T = (11, 0)
    today = _session_5m_flat(trade, through_T=T, total_vol=100_000)
    prior = [
        (date(2026, 10, 28), [_session_5m_flat(date(2026, 10, 28), through_T=T, total_vol=50_000)]),
        (date(2026, 10, 29), [_session_5m_flat(date(2026, 10, 29), through_T=T, total_vol=50_000)]),
    ]
    m = compute_rvol_metrics(
        trade_date=trade,
        T=T,
        side="bullish",
        trade_day_candles=today,
        prior_session_candle_lists=prior,
        min_sessions=3,
    )
    assert m["n_sessions_used"] == 2
    assert m["rvol_cum"] is None
    assert m["rvol_slot"] is None


def test_contract_roll_sums_front_and_next():
    """Baseline session uses front+next volumes, not thin next-only series."""
    trade = date(2026, 10, 15)
    T = (11, 0)
    # Synthetic contracts: Oct front + Nov next on 15-Oct (before roll window).
    contracts = [
        {
            "trading_symbol": "SUNPHARMA FUT 29 OCT 26",
            "instrument_key": "NSE_FO|OCT",
            "expiry": int(IST.localize(datetime(2026, 10, 29, 15, 30)).timestamp() * 1000),
        },
        {
            "trading_symbol": "SUNPHARMA FUT 26 NOV 26",
            "instrument_key": "NSE_FO|NOV",
            "expiry": int(IST.localize(datetime(2026, 11, 26, 15, 30)).timestamp() * 1000),
        },
        {
            "trading_symbol": "SUNPHARMA FUT 31 DEC 26",
            "instrument_key": "NSE_FO|DEC",
            "expiry": int(IST.localize(datetime(2026, 12, 31, 15, 30)).timestamp() * 1000),
        },
    ]
    front, nxt = pick_front_next_for_session(
        "SUNPHARMA", date(2026, 10, 14), contracts=contracts, apply_roll_window=True
    )
    assert front == "NSE_FO|OCT"
    assert nxt == "NSE_FO|NOV"
    keys = baseline_contract_keys(
        "SUNPHARMA", date(2026, 10, 14), contracts=contracts
    )
    assert keys == ["NSE_FO|OCT", "NSE_FO|NOV"]

    # After Oct expiry, front is Nov
    front2, nxt2 = pick_front_next_for_session(
        "SUNPHARMA", date(2026, 10, 30), contracts=contracts, apply_roll_window=True
    )
    assert front2 == "NSE_FO|NOV"
    assert nxt2 == "NSE_FO|DEC"

    prior_day = date(2026, 10, 14)
    oct_bars = _session_5m_flat(prior_day, through_T=T, total_vol=300_000)
    nov_bars = _session_5m_flat(prior_day, through_T=T, total_vol=20_000)
    today = _session_5m_flat(trade, through_T=T, total_vol=400_000)
    # If we wrongly used only Nov (20k) as baseline, RVOL would explode.
    m_wrong = compute_rvol_metrics(
        trade_date=trade,
        T=T,
        side="bullish",
        trade_day_candles=today,
        prior_session_candle_lists=[(prior_day, [nov_bars])],
        min_sessions=1,
    )
    m_right = compute_rvol_metrics(
        trade_date=trade,
        T=T,
        side="bullish",
        trade_day_candles=today,
        prior_session_candle_lists=[(prior_day, [oct_bars, nov_bars])],
        min_sessions=1,
    )
    assert m_wrong["baseline_cum_vol"] == 20_000
    assert abs(m_right["baseline_cum_vol"] - 320_000) < 1.0
    assert m_right["rvol_cum"] < m_wrong["rvol_cum"]


def test_candle_cache_avoids_refetch():
    calls = {"n": 0}

    def fetcher(ik, interval):
        calls["n"] += 1
        return _session_5m_flat(date(2026, 10, 30), through_T=(11, 0), total_vol=1000)

    cache = CandleCache()
    a = cache.ensure("NSE_FO|1", "minutes/5", fetcher)
    b = cache.ensure("NSE_FO|1", "minutes/5", fetcher)
    assert calls["n"] == 1
    assert a is b
    assert cache.get("NSE_FO|1", "minutes/5") is not None


def test_session_bars_stops_at_T():
    d = date(2026, 10, 30)
    bars = _session_5m_flat(d, through_T=(15, 30), total_vol=100_000)
    clipped = session_bars(bars, d, bar_minutes=5, through_T=(11, 0))
    assert clipped
    last_ts = clipped[-1]["timestamp"]
    # last bar starts 10:55, closes 11:00
    assert "10:55" in last_ts

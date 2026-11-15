"""Unit tests for the EMA 30 reclaim scan. No network."""
from __future__ import annotations

from datetime import date, datetime, time, timedelta

import pytz

from backend.services.ema_scan.calendar import (
    STUDY_FROM,
    STUDY_TO,
    front_expiry,
    previous_trading_day,
    trading_days,
)
from backend.services.ema_scan.engine import (
    adx_bucket,
    build_report,
    candle_qualifies,
    default_params,
    distance_bucket,
    first_touch_label,
    measure_window,
    scan_contract,
)
from backend.services.ema_scan.indicators import adx_di_series, wilder_atr_series
from backend.services.momentx.indicators import ema_series, wilder_adx, wilder_atr

IST = pytz.timezone("Asia/Kolkata")


def test_study_trading_days_and_front_contract():
    days = trading_days(STUDY_FROM, STUDY_TO)
    assert len(days) == 17
    assert date(2026, 10, 20) not in days  # NSE holiday
    assert date(2026, 11, 10) not in days  # NSE holiday
    assert date(2026, 10, 21) in days
    assert date(2026, 10, 27) in days
    assert date(2026, 10, 28) in days
    assert date(2026, 11, 13) in days
    october = front_expiry(date(2026, 10, 27))
    assert october == date(2026, 10, 27)
    assert front_expiry(date(2026, 10, 21)) == october
    november = front_expiry(date(2026, 10, 28))
    assert november == date(2026, 11, 23)  # 24 Nov 2026 is a holiday
    assert front_expiry(date(2026, 11, 13)) == november
    assert previous_trading_day(date(2026, 10, 21)) == date(2026, 10, 19)


def test_qualifies_all_conditions():
    assert candle_qualifies(99.0, 100.2, 100.0, 28.0, 30.0, 15.0)
    assert not candle_qualifies(100.1, 100.2, 100.0, 28.0, 30.0, 15.0)  # open not below
    assert not candle_qualifies(99.0, 100.0, 100.0, 28.0, 30.0, 15.0)  # close not above
    assert not candle_qualifies(99.0, 100.6, 100.0, 28.0, 30.0, 15.0)  # more than 0.5%
    assert not candle_qualifies(99.0, 100.2, 100.0, 25.0, 30.0, 15.0)  # ADX not above 25
    assert not candle_qualifies(99.0, 100.2, 100.0, 28.0, 10.0, 15.0)  # +DI not above -DI
    assert not candle_qualifies(99.0, 100.2, None, 28.0, 30.0, 15.0)


def test_buckets():
    assert adx_bucket(25.01) == "25–30"
    assert adx_bucket(30) == "30–40"
    assert adx_bucket(39.9) == "30–40"
    assert adx_bucket(40) == "40+"
    assert distance_bucket(0.05) == "0–0.1%"
    assert distance_bucket(0.1) == "0.1–0.25%"
    assert distance_bucket(0.25) == "0.25–0.5%"
    assert distance_bucket(0.5) == "0.25–0.5%"


def test_adx_and_atr_match_momentx_last_value():
    highs, lows, closes = [], [], []
    price = 100.0
    for i in range(80):
        price += 0.4
        closes.append(price)
        highs.append(price + 0.05)
        lows.append(price - 0.05)
    adx, _pdi, _mdi = adx_di_series(highs, lows, closes, 14)
    assert adx[-1] is not None
    assert abs(adx[-1] - wilder_adx(highs, lows, closes, 14)) < 1e-9
    atr = wilder_atr_series(highs, lows, closes, 14)
    assert atr[-1] is not None
    assert abs(atr[-1] - wilder_atr(highs, lows, closes, 14)) < 1e-9
    assert atr[14] is not None
    assert atr[13] is None
    assert adx[26] is None
    assert adx[27] is not None


def test_window_math_and_first_touch_without_lookahead():
    # Entry is the first window bar's open. The signal candle is not in the window.
    bars = []
    start = IST.localize(datetime(2026, 10, 21, 9, 20))
    for i in range(24):
        ts = start + timedelta(minutes=5 * i)
        bars.append(
            {
                "timestamp": ts.isoformat(),
                "open": 100.0 if i == 0 else 100.0 + i,
                "high": 101.0 if i == 3 else 100.2,
                "low": 99.0 if i == 1 else 99.8,
                "close": 100.1,
            }
        )
    # Bar 1 (index 1) low 99 touches -1 ATR (atr=1, entry=100) and is bearish? close 100.1 > open.
    # Force the adverse bar to be the only touch of -1, and bar 3 high 101 touches +1 later.
    out = measure_window(bars, 100.0, 1.0, bar_minutes=5)
    assert abs(out["max_favorable_atr"] - 1.0) < 1e-9
    assert abs(out["max_adverse_atr"] - 1.0) < 1e-9
    assert out["time_to_max_favorable"] == 15
    assert out["first_touch"] == "-1 ATR"
    assert out["hits"]["0.25"] and out["hits"]["1"] and not out["hits"]["1.5"]
    assert abs(out["close_at_2h_atr"] - 0.1) < 1e-9

    both = [
        {
            "open": 100.0,
            "high": 101.5,
            "low": 98.5,
            "close": 101.0,  # bullish path: low first
        }
    ]
    assert first_touch_label(both, 100.0, 1.0) == "-1 ATR"
    both[0]["close"] = 99.0  # bearish path: high first
    assert first_touch_label(both, 100.0, 1.0) == "+1 ATR"
    quiet = [{"open": 100, "high": 100.2, "low": 99.8, "close": 100.1}]
    assert first_touch_label(quiet, 100.0, 1.0) == "neither"


def _session(day: date, close_at, slope: float, n: int = 75):
    bars = []
    daily_high = None
    daily_low = None
    first_open = None
    last_close = None
    price = close_at
    for i in range(n):
        ts = IST.localize(datetime.combine(day, time(9, 15))) + timedelta(minutes=5 * i)
        price = price + slope
        high = price + 0.02
        low = price - 0.02
        bar = {
            "timestamp": ts.isoformat(),
            "open": price - 0.01,
            "high": high,
            "low": low,
            "close": price,
        }
        bars.append(bar)
        daily_high = high if daily_high is None else max(daily_high, high)
        daily_low = low if daily_low is None else min(daily_low, low)
        if first_open is None:
            first_open = bar["open"]
        last_close = price
    daily = {
        "timestamp": day.isoformat(),
        "open": first_open,
        "high": daily_high,
        "low": daily_low,
        "close": last_close,
    }
    return bars, daily, price


def test_scan_finds_reclaim_and_ignores_signal_candle_high():
    days = trading_days(date(2026, 8, 3), date(2026, 10, 21))
    assert date(2026, 10, 21) in days
    bars = []
    daily = []
    price = 100.0
    for day in days:
        session, day_bar, price = _session(day, price, 0.03)
        bars.extend(session)
        daily.append(day_bar)

    params = default_params()
    params["date_from"] = "2026-10-21"
    params["date_to"] = "2026-10-21"
    # Place the reclaim on candle 3 (09:25) of 21 Oct by setting open below EMA.
    # Close is left as generated so EMA / ADX stay causal.
    closes = [b["close"] for b in bars]
    ema = ema_series(closes, 30)
    target = None
    for i, bar in enumerate(bars):
        ts = datetime.fromisoformat(bar["timestamp"])
        if ts.date() != date(2026, 10, 21):
            continue
        if ts.hour == 9 and ts.minute == 25:
            target = i
            break
    assert target is not None
    ema_v = ema[target]
    # Pull the close onto the EMA from above, within 0.5%, then recompute.
    bars[target]["close"] = round(ema_v * 1.001, 4)
    bars[target]["high"] = max(bars[target]["high"], bars[target]["close"] + 5.0)  # must NOT enter MFE
    bars[target]["low"] = min(bars[target]["low"], bars[target]["close"] - 0.05)
    closes = [b["close"] for b in bars]
    ema = ema_series(closes, 30)
    ema_v = ema[target]
    assert bars[target]["close"] > ema_v
    assert bars[target]["close"] <= ema_v * 1.005
    bars[target]["open"] = round(ema_v - 0.2, 4)

    sigs, skips = scan_contract(
        symbol="TEST",
        contract="TEST FUT 27 OCT 26",
        instrument_key="NSE_FO|TEST",
        bars_5m=bars,
        bars_daily=daily,
        days=[date(2026, 10, 21)],
        params=params,
    )
    assert skips == []
    assert len(sigs) == 1
    sig = sigs[0]
    assert sig["candle_number"] == 3
    assert sig["time"] == "09:25"
    assert sig["first_touch"] in ("+1 ATR", "-1 ATR", "neither")
    # Signal-candle high is 5 points above close and must not be the favorable excursion.
    assert sig["max_favorable_atr"] < 2.0
    # Entry is the next candle's open, not the signal close.
    nxt = bars[target + 1]
    assert sig["entry"] == round(float(nxt["open"]), 2)

    # A second qualifying candle the same day is ignored when first_only is true.
    later = target + 2
    closes = [b["close"] for b in bars]
    ema2 = ema_series(closes, 30)
    bars[later]["close"] = round(ema2[later] * 1.001, 4)
    bars[later]["open"] = round(ema2[later] - 0.2, 4)
    sigs2, _sk = scan_contract(
        symbol="TEST",
        contract="TEST FUT 27 OCT 26",
        instrument_key="NSE_FO|TEST",
        bars_5m=bars,
        bars_daily=daily,
        days=[date(2026, 10, 21)],
        params=params,
    )
    assert len(sigs2) == 1
    assert sigs2[0]["candle_number"] == 3


def test_missing_forward_bar_is_skipped_and_short_history_is_skipped():
    day = date(2026, 10, 21)
    params = default_params()
    # Only the scan session — not enough history.
    session, daily, _price = _session(day, 100.0, 0.03, n=10)
    sigs, skips = scan_contract(
        symbol="SHORT",
        contract="SHORT FUT",
        instrument_key="NSE_FO|SHORT",
        bars_5m=session,
        bars_daily=[daily],
        days=[day],
        params=params,
    )
    assert sigs == []
    assert skips and "not stable" in skips[0]["reason"]

    # Enough history, but delete one bar inside the 2-hour window of a forced signal.
    days = trading_days(date(2026, 8, 3), day)
    bars = []
    dailies = []
    price = 100.0
    for d in days:
        session, day_bar, price = _session(d, price, 0.03)
        bars.extend(session)
        dailies.append(day_bar)
    closes = [b["close"] for b in bars]
    ema = ema_series(closes, 30)
    target = next(
        i
        for i, b in enumerate(bars)
        if datetime.fromisoformat(b["timestamp"]).date() == day
        and datetime.fromisoformat(b["timestamp"]).strftime("%H:%M") == "09:15"
    )
    bars[target]["close"] = round(ema[target] * 1.001, 4)
    ema = ema_series([b["close"] for b in bars], 30)
    bars[target]["open"] = round(ema[target] - 0.2, 4)
    # Drop the bar 30 minutes after the signal (inside the 24-bar window).
    drop_at = datetime.fromisoformat(bars[target]["timestamp"]) + timedelta(minutes=30)
    bars = [b for b in bars if datetime.fromisoformat(b["timestamp"]) != drop_at]
    sigs, skips = scan_contract(
        symbol="GAP",
        contract="GAP FUT",
        instrument_key="NSE_FO|GAP",
        bars_5m=bars,
        bars_daily=dailies,
        days=[day],
        params=params,
    )
    assert sigs == []
    assert any("forward window" in row["reason"] for row in skips)


def test_report_lists_empty_days_and_summary():
    params = default_params()
    signals = [
        {
            "date": "2026-10-21",
            "symbol": "ABC",
            "contract": "ABC FUT 27 OCT 26",
            "candle_number": 2,
            "time": "09:20",
            "adx": 32.0,
            "pct_above_ema": 0.2,
            "max_favorable_atr": 1.2,
            "max_adverse_atr": 0.4,
            "hits": {"0.25": True, "0.5": True, "0.75": True, "1": True, "1.5": False, "2": False},
            "adx_bucket": "30–40",
            "distance_bucket": "0.1–0.25%",
        }
    ]
    report = build_report(
        signals=signals,
        skipped=[{"date": "2026-10-22", "symbol": "ZZZ", "contract": "", "reason": "no candles"}],
        params=params,
        universe=10,
        contract_rule="front month",
    )
    assert report["header"]["trading_days"] == 17
    assert report["header"]["signals"] == 1
    assert report["header"]["stocks_skipped"] == 1
    assert len(report["days"]) == 17
    empty = [d for d in report["days"] if d["date"] == "2026-10-23"][0]
    assert empty["count"] == 0
    assert report["summary"]["avg_mfe"] == 1.2
    assert report["summary"]["hit_pct"]["1"] == 100.0
    assert report["distribution"]
    assert len(report["breakdowns"]["candle"]) == 6

"""Unit tests for MomentX indicators, Stage-1 filter, EMA order, trigger."""
from __future__ import annotations

from datetime import datetime, timedelta

import pytz

from backend.services.momentx.candles import drop_forming_candle, splits_ohlcv
from backend.services.momentx.config import MomentXConfig
from backend.services.momentx.filter import evaluate_stage1
from backend.services.momentx.indicators import (
    ema_last,
    ema_series,
    is_ema_trigger,
    order_emas_by_value,
    sma,
    wilder_adx,
    wilder_rsi,
    williams_r,
)

IST = pytz.timezone("Asia/Kolkata")


def test_sma_basic():
    assert sma([1, 2, 3, 4, 5], 5) == 3.0
    assert sma([1, 2, 3], 5) is None


def test_ema_series_seed():
    closes = [10.0, 11.0, 12.0, 13.0, 14.0]
    series = ema_series(closes, 3)
    assert len(series) == 5
    assert series[0] == 10.0
    assert ema_last(closes, 3) == series[-1]


def test_wilder_rsi_known_ramp():
    # Strictly rising closes → RSI near 100
    closes = [float(i) for i in range(1, 30)]
    rsi = wilder_rsi(closes, 14)
    assert rsi is not None
    assert rsi > 70


def test_wilder_rsi_flat():
    closes = [50.0] * 30
    rsi = wilder_rsi(closes, 14)
    assert rsi is not None
    assert 45 <= rsi <= 55


def test_williams_r_at_high():
    # Close at period high → Williams %R near 0
    highs = [10 + i * 0.1 for i in range(20)]
    lows = [9 + i * 0.1 for i in range(20)]
    closes = list(highs)
    wr = williams_r(highs, lows, closes, 14)
    assert wr is not None
    assert wr >= -5


def test_williams_r_at_low():
    highs = [100.0] * 20
    lows = [90.0] * 20
    closes = [90.0] * 20
    wr = williams_r(highs, lows, closes, 14)
    assert wr is not None
    assert wr <= -95


def test_adx_trending():
    # Strong uptrend: rising highs/lows/closes
    n = 80
    highs, lows, closes = [], [], []
    for i in range(n):
        c = 100 + i * 0.5
        closes.append(c)
        highs.append(c + 1)
        lows.append(c - 1)
    adx = wilder_adx(highs, lows, closes, 14)
    assert adx is not None
    assert adx > 20


def test_stage1_oscillators_use_futures_series_not_cash():
    """RSI/Williams/ADX from minutes15_oscillators; price from cash minutes15."""
    daily = [_bar(i - 210, 100.0, volume=1_000_000) for i in range(210)]
    # Cash: mild grind; futures: strong trend → higher ADX on osc series.
    m15_cash = []
    m15_fut = []
    base = IST.localize(datetime(2026, 3, 5, 9, 15))
    for i in range(120):
        ts = (base + timedelta(minutes=15 * i)).isoformat()
        cc = 101.0 + (i % 5) * 0.05
        m15_cash.append(
            {
                "timestamp": ts,
                "open": cc,
                "high": cc + 0.3,
                "low": cc - 0.3,
                "close": cc,
                "volume": 50_000,
            }
        )
        fc = 100.0 + i * 0.8
        m15_fut.append(
            {
                "timestamp": ts,
                "open": fc - 0.2,
                "high": fc + 1.0,
                "low": fc - 1.0,
                "close": fc,
                "volume": 50_000,
            }
        )
    cfg = MomentXConfig(
        sma_period=200,
        minutes15_min_bars=50,
        adx_min=0.0,
        rsi_min=0.0,
        williams_min=-100.0,
        pct_change_min=0.0,
        pct_change_max=100.0,
    )
    now = IST.localize(datetime(2026, 3, 10, 12, 0))
    cash_only = evaluate_stage1(
        daily=daily, minutes15=m15_cash, now=now, cfg=cfg, enforce_stale_guards=False
    )
    with_fut = evaluate_stage1(
        daily=daily,
        minutes15=m15_cash,
        minutes15_oscillators=m15_fut,
        now=now,
        cfg=cfg,
        enforce_stale_guards=False,
    )
    assert cash_only.get("skip_reason") is None
    assert with_fut.get("skip_reason") is None
    assert with_fut["price"] == cash_only["price"]
    assert with_fut["adx"] != cash_only["adx"]
    assert with_fut["adx"] > cash_only["adx"]
    # Futures ADX must match wilder_adx on the osc OHLC directly.
    _, fh, fl, fc, _ = splits_ohlcv(drop_forming_candle(m15_fut, 15, now=now))
    assert abs(float(with_fut["adx"]) - float(wilder_adx(fh, fl, fc, 14))) < 1e-9


def test_order_emas_highest_to_lowest():
    ordered = order_emas_by_value(10.0, 12.0, 8.0)
    assert [x[0] for x in ordered] == ["EMA(15)", "EMA(5)", "EMA(50)"]
    assert [x[1] for x in ordered] == [12.0, 10.0, 8.0]


def test_ema_trigger_condition():
    assert is_ema_trigger(12.0, 10.0, 9.0) is True
    assert is_ema_trigger(10.0, 12.0, 9.0) is False
    assert is_ema_trigger(12.0, 10.0, 13.0) is False
    assert is_ema_trigger(None, 10.0, 9.0) is False


def _bar(day_offset: int, close: float, volume: float = 1_000_000, *, minute: int = 0):
    ts = IST.localize(datetime(2026, 3, 10, 9, 15) + timedelta(days=day_offset, minutes=minute))
    return {
        "timestamp": ts.isoformat(),
        "open": close,
        "high": close + 1,
        "low": close - 1,
        "close": close,
        "volume": volume,
    }


def test_stage1_qualifies_when_all_pass():
    # Build 220 completed daily bars ending yesterday; price below SMA200 but +2% day.
    daily = []
    for i in range(220):
        # Flat history around 100, then yesterday close 100
        daily.append(_bar(i - 220, 100.0, volume=1_000_000))
    # Today partial daily
    daily.append(_bar(0, 102.0, volume=6_000_000))

    # 15m bars spanning prior sessions + today; last *closed* at 13:45 when now=14:00.
    m15 = []
    base = IST.localize(datetime(2026, 3, 5, 9, 15))
    for i in range(160):
        c = 95.0 + i * 0.04
        ts = base + timedelta(minutes=15 * i)
        m15.append(
            {
                "timestamp": ts.isoformat(),
                "open": c - 0.1,
                "high": c + 0.5,
                "low": c - 0.5,
                "close": c,
                "volume": 50_000,
            }
        )
    # Ensure a closed bar at 13:45 today with close 102 (+2% vs prev 100).
    target = IST.localize(datetime(2026, 3, 10, 13, 45))
    m15.append(
        {
            "timestamp": target.isoformat(),
            "open": 101.5,
            "high": 103.0,
            "low": 101.0,
            "close": 102.0,
            "volume": 80_000,
        }
    )

    cfg = MomentXConfig(
        sma_period=200,
        rsi_min=50.0,
        adx_min=10.0,
        williams_min=-80.0,
        pct_change_min=1.0,
        pct_change_max=3.0,
        minutes15_min_bars=50,
        volume_sma_days=5,
        time_adjusted_volume=False,
    )
    now = IST.localize(datetime(2026, 3, 10, 14, 0))
    ev = evaluate_stage1(daily=daily, minutes15=m15, now=now, cfg=cfg)
    assert ev.get("skip_reason") is None
    assert ev["pct_change"] is not None
    assert 1.0 <= ev["pct_change"] <= 3.0
    assert "conditions" in ev
    assert ev["conditions"]["pct_in_band"] is True
    # Flat daily SMA ≈ 100; price 102 is ABOVE SMA200.
    assert ev["conditions"]["below_sma200"] is False
    # Price below SMA but negative day change fails the +1..+3 band.
    m15[-1]["close"] = 98.0
    m15[-1]["high"] = 99.0
    m15[-1]["low"] = 97.0
    ev2 = evaluate_stage1(daily=daily, minutes15=m15, now=now, cfg=cfg)
    assert ev2["conditions"]["below_sma200"] is True
    assert ev2["conditions"]["pct_in_band"] is False


def test_stage1_volume_optional_and_bearish_side():
    daily = [_bar(i - 210, 100.0, volume=1_000_000) for i in range(210)]
    # Yesterday close high so price can sit above SMA for bearish.
    daily[-1] = _bar(-1, 120.0, volume=1_000_000)
    daily.append(_bar(0, 110.0, volume=100_000))  # low today volume
    m15 = []
    base = IST.localize(datetime(2026, 3, 5, 9, 15))
    for i in range(100):
        # Falling closes → oversold RSI / Williams for bearish
        c = 130.0 - i * 0.25
        ts = base + timedelta(minutes=15 * i)
        m15.append(
            {
                "timestamp": ts.isoformat(),
                "open": c + 0.2,
                "high": c + 0.8,
                "low": c - 0.8,
                "close": c,
                "volume": 20_000,
            }
        )
    today_base = IST.localize(datetime(2026, 3, 10, 9, 15))
    for i in range(12):  # 09:15 .. 12:00; at now=12:00 last closed is 11:45 (i=10)
        # Hard dump so Williams %R is deeply oversold (<= -70).
        c = 118.0 - i * 1.2
        m15.append(
            {
                "timestamp": (today_base + timedelta(minutes=15 * i)).isoformat(),
                "open": c + 0.4,
                "high": c + 0.6,
                "low": c - 1.2,
                "close": c,
                "volume": 5_000,
            }
        )
    cfg = MomentXConfig(
        sma_period=200,
        rsi_bearish_max=45.0,
        williams_bearish_max=-70.0,
        adx_min=5.0,
        pct_change_bearish_max=-1.0,
        minutes15_min_bars=50,
        volume_sma_days=5,
        time_adjusted_volume=False,
    )
    # At 12:00 the last closed bar is 11:45 (i=10 → close 106).
    now = IST.localize(datetime(2026, 3, 10, 12, 0))
    bear = evaluate_stage1(
        daily=daily, minutes15=m15, now=now, cfg=cfg, side="bearish", require_volume=False
    )
    assert bear.get("skip_reason") is None
    assert bear["conditions"]["above_sma200"] is True
    assert bear["conditions"]["pct_bearish"] is True
    assert bear["core_qualified"] is True
    # Low volume should fail volume_ok but still qualify when require_volume=False
    assert bear["conditions"]["volume_ok"] is False
    assert bear["qualified"] is True
    bear_req = evaluate_stage1(
        daily=daily, minutes15=m15, now=now, cfg=cfg, side="bearish", require_volume=True
    )
    assert bear_req["core_qualified"] is True
    assert bear_req["qualified"] is False


def test_stage1_pct_and_volume_gates():
    daily = [_bar(i - 210, 100.0, volume=1_000_000) for i in range(210)]
    daily.append(_bar(0, 102.0, volume=8_000_000))
    m15 = []
    base = IST.localize(datetime(2026, 3, 5, 9, 15))
    for i in range(110):
        c = 100.0 + (i % 5) * 0.2
        ts = base + timedelta(minutes=15 * i)
        m15.append(
            {
                "timestamp": ts.isoformat(),
                "open": c,
                "high": c + 1,
                "low": c - 1,
                "close": c,
                "volume": 80_000,
            }
        )
    # Put several bars on "today" 2026-03-10 through 11:45 (closed by now=12:00).
    today_base = IST.localize(datetime(2026, 3, 10, 9, 15))
    for i in range(11):  # 09:15 .. 11:45 inclusive
        m15.append(
            {
                "timestamp": (today_base + timedelta(minutes=15 * i)).isoformat(),
                "open": 101.5,
                "high": 102.5,
                "low": 101.0,
                "close": 102.0,
                "volume": 900_000,
            }
        )
    cfg = MomentXConfig(
        sma_period=200,
        rsi_min=0.0,
        adx_min=0.0,
        williams_min=-100.0,
        minutes15_min_bars=50,
        volume_sma_days=5,
        time_adjusted_volume=False,
    )
    now = IST.localize(datetime(2026, 3, 10, 12, 0))
    ev = evaluate_stage1(daily=daily, minutes15=m15, now=now, cfg=cfg)
    assert ev.get("skip_reason") is None
    assert ev["conditions"]["pct_in_band"] is True
    assert ev["today_volume"] is not None
    assert ev["volume_sma5"] is not None
    # Baseline is full-day prior avg (1M); today cumulative from 15m is large → volume_ok.
    assert ev["conditions"]["volume_ok"] is True


def test_stale_guards_flat_prev_close_and_duplicate_fingerprint():
    from backend.services.momentx.filter import evaluate_stage1, metric_fingerprint

    daily = [_bar(i - 210, 100.0, volume=1_000_000) for i in range(210)]
    m15 = []
    base = IST.localize(datetime(2026, 3, 5, 9, 15))
    for i in range(120):
        c = 100.0 + (i % 3) * 0.1
        m15.append(
            {
                "timestamp": (base + timedelta(minutes=15 * i)).isoformat(),
                "open": c,
                "high": c + 1,
                "low": c - 1,
                "close": c,
                "volume": 40_000,
            }
        )
    # Today bars frozen at prev close 100 → pct 0.
    today_base = IST.localize(datetime(2026, 3, 10, 9, 15))
    for i in range(8):
        m15.append(
            {
                "timestamp": (today_base + timedelta(minutes=15 * i)).isoformat(),
                "open": 100.0,
                "high": 100.5,
                "low": 99.5,
                "close": 100.0,
                "volume": 10_000,
            }
        )
    cfg = MomentXConfig(sma_period=200, minutes15_min_bars=50, adx_min=0.0, rsi_min=0.0, williams_min=-100.0)
    now = IST.localize(datetime(2026, 3, 10, 11, 0))  # last closed = 10:45
    ev = evaluate_stage1(daily=daily, minutes15=m15, now=now, cfg=cfg)
    assert ev.get("skip_reason") == "stale_flat_prev_close"

    # Non-flat bar, then duplicate fingerprint rejection.
    m15[-1]["close"] = 101.5
    m15[-1]["high"] = 102.0
    now2 = IST.localize(datetime(2026, 3, 10, 11, 15))
    m15.append(
        {
            "timestamp": IST.localize(datetime(2026, 3, 10, 11, 0)).isoformat(),
            "open": 101.0,
            "high": 102.0,
            "low": 100.5,
            "close": 101.5,
            "volume": 12_000,
        }
    )
    ev_ok = evaluate_stage1(daily=daily, minutes15=m15, now=now2, cfg=cfg, enforce_stale_guards=True)
    assert ev_ok.get("skip_reason") is None
    fp = ev_ok.get("fingerprint")
    assert fp is not None
    ev_dup = evaluate_stage1(
        daily=daily,
        minutes15=m15,
        now=now2,
        cfg=cfg,
        prior_fingerprints={fp},
    )
    assert ev_dup.get("skip_reason") == "stale_duplicate_metrics"
    assert metric_fingerprint(
        price=ev_ok["price"], rsi=ev_ok["rsi"], adx=ev_ok["adx"], today_volume=ev_ok["today_volume"]
    ) == fp


def test_expected_closed_15m_start():
    from backend.services.momentx.filter import expected_closed_15m_start

    now = IST.localize(datetime(2026, 3, 10, 13, 45))
    assert expected_closed_15m_start(now) == IST.localize(datetime(2026, 3, 10, 13, 30))
    now2 = IST.localize(datetime(2026, 3, 10, 9, 30))
    assert expected_closed_15m_start(now2) == IST.localize(datetime(2026, 3, 10, 9, 15))


def test_compute_pnl_bearish_short():
    from backend.services.momentx.persistence import compute_pnl

    assert compute_pnl(100.0, 90.0, 10, side="bullish") == -100.0
    assert compute_pnl(100.0, 90.0, 10, side="bearish") == 100.0


def test_merge_day_cards_keeps_inactive_and_sorts_active_first():
    from backend.services.momentx.day_board import merge_day_cards

    prev = [
        {"stock": "AAA", "status": "active", "filters_ok": True, "trigger": False, "first_seen_at": "t0", "ltp": 10},
        {"stock": "BBB", "status": "active", "filters_ok": True, "trigger": True, "first_seen_at": "t1", "ltp": 20},
    ]
    qualified = [
        {"stock": "CCC", "trigger": False, "ltp": None, "filters_ok": True, "side": "bullish"},
        {"stock": "BBB", "trigger": True, "ltp": 21, "filters_ok": True, "side": "bullish"},
    ]
    stage1 = {"AAA": {"pct_change": 0.5, "conditions": {"pct_in_band": False}}}
    now = IST.localize(datetime(2026, 3, 10, 12, 0))
    out, newly = merge_day_cards(
        prev_cards=prev,
        qualified_cards=qualified,
        stage1_by_stock=stage1,
        now=now,
    )
    by = {c["stock"]: c for c in out}
    assert set(by) == {"AAA", "BBB", "CCC"}
    assert by["AAA"]["status"] == "filters_off"
    assert by["AAA"]["stage1"]["pct_change"] == 0.5
    assert by["AAA"]["ltp"] == 10
    assert by["BBB"]["status"] == "active"
    assert by["BBB"]["ltp"] == 21
    assert by["CCC"]["status"] == "active"
    assert by["CCC"]["first_seen_at"] == now.isoformat()
    assert [c["stock"] for c in newly] == ["CCC"]
    # Active before filters_off; among active, trigger first
    assert [c["stock"] for c in out] == ["BBB", "CCC", "AAA"]


def test_ema_cross_above_from_below():
    from backend.services.momentx.indicators import ema_cross_above_from_below

    assert ema_cross_above_from_below([1, 2], [2, 1.5]) is True
    assert ema_cross_above_from_below([3, 4], [1, 2]) is False  # already above
    assert ema_cross_above_from_below([1, 1], [2, 3]) is False


def test_active_locks_after_five_minutes():
    from backend.services.momentx.status_rules import apply_status_to_card

    now = IST.localize(datetime(2026, 3, 10, 12, 10))
    card = {
        "stock": "AAA",
        "filters_ok": True,
        "status": "active",
        "active_since": IST.localize(datetime(2026, 3, 10, 12, 0)).isoformat(),
    }
    out = apply_status_to_card(card, now=now)
    assert out["status"] == "locked"


def test_lock_all_after_1500():
    from backend.services.momentx.status_rules import apply_status_to_card

    now = IST.localize(datetime(2026, 3, 10, 15, 1))
    card = {"stock": "AAA", "filters_ok": True, "status": "active", "active_since": now.isoformat()}
    out = apply_status_to_card(card, now=now)
    assert out["status"] == "locked"

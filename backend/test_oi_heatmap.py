"""OI heatmap classification, currmth universe filter, and overnight freeze window."""
from __future__ import annotations

from datetime import datetime

import pytz

from backend.services.oi_heatmap import (
    bucket_key_for_oi_signal,
    currmth_future_keys_from_arbitrage_rows,
    group_rows_by_oi_signal,
    in_oi_heatmap_fetch_window,
    is_oi_heatmap_overnight_freeze,
    ist_use_today_only_db_snapshot,
    sort_rows_for_oi_tab,
)
from backend.services.oi_integration import interpret_oi_signal

IST = pytz.timezone("Asia/Kolkata")


def _ist(y, m, d, hh, mm):
    return IST.localize(datetime(y, m, d, hh, mm))


def test_interpret_oi_signal_four_states():
    assert interpret_oi_signal(1.0, 10.0) == "LONG_BUILDUP"
    assert interpret_oi_signal(-1.0, 10.0) == "SHORT_BUILDUP"
    assert interpret_oi_signal(-1.0, -10.0) == "LONG_UNWINDING"
    assert interpret_oi_signal(1.0, -10.0) == "SHORT_COVERING"
    assert interpret_oi_signal(0.0, 10.0) == "NEUTRAL"
    assert interpret_oi_signal(1.0, 0.0) == "NEUTRAL"


def test_htf_oi_window_is_prior_day_not_ten_sessions():
    from backend.services.oi_heatmap_htf import daily_bars_with_oi, htf_oi_from_daily_bars

    candles = []
    for i in range(12):
        candles.append(
            {
                "timestamp": f"2026-08-{17 + i:02d}T00:00:00+05:30",
                "close": 100.0 + i,
                "oi": 1000.0 + i * 10,
            }
        )
    bars = daily_bars_with_oi(candles)
    rec = htf_oi_from_daily_bars(bars, market_closed=True)
    assert rec is not None
    assert rec["bar_latest"].startswith("2026-08-28")
    assert rec["bar_prior"].startswith("2026-08-27")
    assert rec["sessions_back"] == 1
    assert rec["htf_oi_signal"] == "LONG_BUILDUP"
    assert htf_oi_from_daily_bars(bars[:1], market_closed=True) is None


def test_htf_oi_four_way_matches_session_rules():
    from backend.services.oi_heatmap_htf import htf_oi_from_daily_bars

    def pair(c0, oi0, c1, oi1):
        bars = [
            {"timestamp": "2026-08-18T00:00:00+05:30", "close": c0, "oi": oi0},
        ] + [
            {"timestamp": f"2026-08-{19 + i:02d}T00:00:00+05:30", "close": c0, "oi": oi0}
            for i in range(9)
        ]
        bars.append({"timestamp": "2026-08-28T00:00:00+05:30", "close": c1, "oi": oi1})
        return htf_oi_from_daily_bars(bars, market_closed=True)["htf_oi_signal"]

    assert pair(100, 1000, 110, 1100) == "LONG_BUILDUP"
    assert pair(100, 1000, 90, 1100) == "SHORT_BUILDUP"
    assert pair(100, 1000, 90, 900) == "LONG_UNWIND"
    assert pair(100, 1000, 110, 900) == "SHORT_COVER"


def test_htf_skips_bars_missing_oi():
    from backend.services.oi_heatmap_htf import daily_bars_with_oi

    bars = daily_bars_with_oi(
        [
            {"timestamp": "2026-08-18T00:00:00+05:30", "close": 1, "oi": 10},
            {"timestamp": "2026-08-19T00:00:00+05:30", "close": 1},
        ]
    )
    assert len(bars) == 1


def test_writer_grades_bucket_into_ui_tabs():
    """Dashboard tabs are LONG_BUILDUP, SHORT_BUILDUP, LONG_UNWINDING, SHORT_COVERING.

    ``dashboard-oi-heatmap.js`` bucketKey() accepts those four plus the writer
    spellings LONG_UNWIND and SHORT_COVER. A grade the classifier emits must
    land in one of those tabs, and an all-null snapshot must not.
    """
    from backend.services.oi_heatmap import heatmap_rows_have_ui_buckets
    from backend.services.oi_heatmap_bars import classify_buildup

    ui_tabs = {"LONG_BUILDUP", "SHORT_BUILDUP", "LONG_UNWINDING", "SHORT_COVERING"}
    cases = [
        (110, 100, 1100, 1000, "LONG_BUILDUP"),
        (90, 100, 1100, 1000, "SHORT_BUILDUP"),
        (90, 100, 900, 1000, "LONG_UNWIND"),
        (110, 100, 900, 1000, "SHORT_COVER"),
    ]
    for price_now, price_prior, oi_now, oi_prior, expected in cases:
        sig, _price_pct, _oi_pct = classify_buildup(price_now, price_prior, oi_now, oi_prior)
        assert sig == expected
        assert bucket_key_for_oi_signal(sig) in ui_tabs
    assert bucket_key_for_oi_signal("LONG_UNWINDING") == "LONG_UNWINDING"
    assert bucket_key_for_oi_signal("SHORT_COVERING") == "SHORT_COVERING"
    assert bucket_key_for_oi_signal("NEUTRAL") is None
    assert bucket_key_for_oi_signal(None) is None
    blank = [{"underlying_symbol": "AAA", "oi_signal": None} for _ in range(4)]
    assert heatmap_rows_have_ui_buckets(blank) is False
    blank[0]["oi_signal"] = "SHORT_COVER"
    assert heatmap_rows_have_ui_buckets(blank) is True


def test_bucket_aliases_and_group():
    assert bucket_key_for_oi_signal("LONG_UNWIND") == "LONG_UNWINDING"
    assert bucket_key_for_oi_signal("SHORT_COVER") == "SHORT_COVERING"
    assert bucket_key_for_oi_signal("NEUTRAL") is None
    grouped = group_rows_by_oi_signal(
        [
            {"underlying_symbol": "AAA", "oi_signal": "LONG_BUILDUP"},
            {"underlying_symbol": "BBB", "oi_signal": "LONG_UNWIND"},
            {"underlying_symbol": "CCC", "oi_signal": "NEUTRAL"},
        ]
    )
    assert [r["underlying_symbol"] for r in grouped["LONG_BUILDUP"]] == ["AAA"]
    assert [r["underlying_symbol"] for r in grouped["LONG_UNWINDING"]] == ["BBB"]
    assert grouped["SHORT_BUILDUP"] == []
    assert grouped["SHORT_COVERING"] == []


def test_tab_sort_uses_global_rank_then_score():
    rows = [
        {"underlying_symbol": "LOW", "rank": 9, "score": 90, "oi_chg": 1, "oi_chg_pct": 9, "chg_pct": 9},
        {"underlying_symbol": "HIGH", "rank": 2, "score": 10, "oi_chg": 1, "oi_chg_pct": 1, "chg_pct": 1},
        {"underlying_symbol": "TIE_SCORE", "rank": 2, "score": 40, "oi_chg": 5, "oi_chg_pct": 1, "chg_pct": 1},
    ]
    ordered = sort_rows_for_oi_tab(rows)
    assert [r["underlying_symbol"] for r in ordered] == ["TIE_SCORE", "HIGH", "LOW"]


def test_universe_only_currmth_future_keys():
    rows = [
        ("RELIANCE", "NSE_FO|111"),
        ("INFY", ""),
        ("TCS", None),
        ("HDFCBANK", "NSE_FO|222"),
        ("DUP", "NSE_FO|111"),
    ]
    keys = currmth_future_keys_from_arbitrage_rows(rows)
    assert keys == ["NSE_FO|111", "NSE_FO|222"]


def test_fetch_window_and_overnight_freeze(monkeypatch):
    monkeypatch.setattr(
        "backend.services.oi_heatmap.should_skip_scheduled_market_jobs_ist",
        lambda now=None: False,
    )
    friday_open = _ist(2026, 9, 4, 9, 0)
    friday_930 = _ist(2026, 9, 4, 9, 30)
    friday_1600 = _ist(2026, 9, 4, 16, 0)
    friday_1601 = _ist(2026, 9, 4, 16, 1)
    friday_pre = _ist(2026, 9, 4, 8, 59)
    saturday = _ist(2026, 9, 5, 12, 0)

    assert in_oi_heatmap_fetch_window(friday_open) is True
    assert in_oi_heatmap_fetch_window(friday_930) is True
    assert in_oi_heatmap_fetch_window(friday_1600) is True
    assert in_oi_heatmap_fetch_window(friday_1601) is False
    assert in_oi_heatmap_fetch_window(friday_pre) is False
    assert in_oi_heatmap_fetch_window(saturday) is False

    assert is_oi_heatmap_overnight_freeze(friday_1601) is True
    assert is_oi_heatmap_overnight_freeze(friday_pre) is True
    assert is_oi_heatmap_overnight_freeze(saturday) is True
    assert is_oi_heatmap_overnight_freeze(friday_open) is False

    assert ist_use_today_only_db_snapshot(friday_pre) is False
    assert ist_use_today_only_db_snapshot(friday_open) is True
    assert ist_use_today_only_db_snapshot(saturday) is False


def test_closed_market_neutral_uses_previous_signal(monkeypatch):
    monkeypatch.setattr(
        "backend.services.oi_heatmap.should_skip_scheduled_market_jobs_ist",
        lambda now=None: False,
    )
    from backend.services.oi_heatmap import apply_previous_oi_signal_when_closed

    rows = [
        {"instrument_key": "NSE_FO|1", "underlying_symbol": "AAA", "oi_signal": "NEUTRAL"},
        {"instrument_key": "NSE_FO|2", "underlying_symbol": "BBB", "oi_signal": "NEUTRAL"},
        {"instrument_key": "NSE_FO|3", "underlying_symbol": "CCC", "oi_signal": "NEUTRAL"},
    ]
    prev_i = {"NSE_FO|1": "LONG_BUILDUP", "NSE_FO|2": "SHORT_COVER"}
    saturday = _ist(2026, 9, 5, 15, 51)
    out = apply_previous_oi_signal_when_closed(
        rows, now=saturday, by_instrument=prev_i, by_underlying={}
    )
    assert out[0]["oi_signal"] == "LONG_BUILDUP"
    assert out[1]["oi_signal"] == "SHORT_COVER"
    assert out[2]["oi_signal"] == "NEUTRAL"


def test_missing_prev_close_is_not_always_up():
    from backend.services.oi_heatmap import price_dp_and_chg_pct, session_prev_close_from_quote

    lp = 100.0
    empty = {"last_price": lp, "ohlc": {}}
    assert session_prev_close_from_quote(lp, empty) is None
    dp, pct, prev = price_dp_and_chg_pct(lp, empty)
    assert dp == 0.0 and pct == 0.0 and prev is None
    assert interpret_oi_signal(dp, 50.0) == "NEUTRAL"

    down = {"net_change": -2.5, "ohlc": {"open": 99.0}}
    dp, pct, prev = price_dp_and_chg_pct(lp, down)
    assert prev == 102.5
    assert dp < 0
    assert interpret_oi_signal(dp, 10.0) == "SHORT_BUILDUP"
    assert interpret_oi_signal(dp, -10.0) == "LONG_UNWINDING"

    up = {"net_change": 1.5}
    dp, pct, prev = price_dp_and_chg_pct(lp, up)
    assert dp > 0
    assert interpret_oi_signal(dp, 10.0) == "LONG_BUILDUP"
    assert interpret_oi_signal(dp, -10.0) == "SHORT_COVERING"

    dp, pct, prev = price_dp_and_chg_pct(lp, empty, cached_prev=105.0)
    assert prev == 105.0 and dp < 0

    vs_open_trap = {"ohlc": {"open": 90.0, "close": lp}}
    assert session_prev_close_from_quote(lp, vs_open_trap) is None


def test_match_upstox_batch_quote_by_instrument_token():
    from backend.services.upstox_service import match_upstox_batch_quote

    raw = {
        "NSE_FO:IDEA26SEPFUT": {
            "instrument_token": "NSE_FO|68545",
            "last_price": 15.7,
            "net_change": 0.56,
        }
    }
    qd = match_upstox_batch_quote(raw, "NSE_FO|68545")
    assert qd is not None
    assert qd["net_change"] == 0.56
    assert match_upstox_batch_quote(raw, "NSE_FO|999") is None


def test_closed_market_keeps_live_signal_when_oi_changed(monkeypatch):
    monkeypatch.setattr(
        "backend.services.oi_heatmap.should_skip_scheduled_market_jobs_ist",
        lambda now=None: False,
    )
    from backend.services.oi_heatmap import apply_previous_oi_signal_when_closed

    saturday = _ist(2026, 9, 5, 20, 48)
    rows = [
        {
            "instrument_key": "NSE_FO|1",
            "underlying_symbol": "AAA",
            "oi_signal": "NEUTRAL",
            "oi_chg": 100,
        }
    ]
    prev_i = {"NSE_FO|1": "LONG_BUILDUP"}
    out = apply_previous_oi_signal_when_closed(
        rows, now=saturday, by_instrument=prev_i, by_underlying={}
    )
    assert out[0]["oi_signal"] == "NEUTRAL"


def test_live_session_neutral_does_not_use_previous(monkeypatch):
    monkeypatch.setattr(
        "backend.services.oi_heatmap.should_skip_scheduled_market_jobs_ist",
        lambda now=None: False,
    )
    from backend.services.oi_heatmap import apply_previous_oi_signal_when_closed

    friday_930 = _ist(2026, 9, 4, 9, 30)
    friday_1600 = _ist(2026, 9, 4, 16, 0)
    rows = [{"instrument_key": "NSE_FO|1", "underlying_symbol": "AAA", "oi_signal": "NEUTRAL"}]
    prev_i = {"NSE_FO|1": "SHORT_BUILDUP"}
    for now in (friday_930, friday_1600):
        out = apply_previous_oi_signal_when_closed(
            rows, now=now, by_instrument=prev_i, by_underlying={}
        )
        assert out[0]["oi_signal"] == "NEUTRAL"


def test_significant_buildup_keeps_existing_signal_and_thresholds():
    from datetime import date

    from backend.services.oi_buildup_significant import (
        SIGNIFICANT_LONG_BUILDUP,
        SIGNIFICANT_SHORT_BUILDUP,
        completed_daily_bars,
        five_day_significant,
        one_day_oi_chg_pct,
        one_day_price_chg_pct,
        one_day_significant,
        session_volume,
    )

    assert interpret_oi_signal(1.0, 10.0) == "LONG_BUILDUP"
    assert one_day_significant(1.0, 2.0, 110, 100) == SIGNIFICANT_LONG_BUILDUP
    assert one_day_significant(-1.0, 2.0, 110, 100) == SIGNIFICANT_SHORT_BUILDUP
    assert one_day_significant(0.99, 2.0, 110, 100) is None
    assert one_day_significant(-0.99, 2.0, 110, 100) is None
    assert one_day_significant(1.0, 1.99, 110, 100) is None
    assert one_day_significant(1.0, 2.0, 100, 100) is None
    assert one_day_significant(1.0, 2.0, 99, 100) is None
    assert one_day_significant(None, 2.0, 110, 100) is None
    assert one_day_significant(1.0, None, 110, 100) is None
    assert one_day_significant(1.0, 2.0, None, 100) is None
    assert one_day_significant(1.0, 2.0, 110, None) is None
    assert one_day_price_chg_pct(0, -100.0, 100.0) is None
    assert one_day_oi_chg_pct(None, 50) is None
    assert one_day_oi_chg_pct(1000, None) is None
    assert session_volume(None, None) is None
    assert session_volume(0, 0) is None
    assert session_volume(0, 40) == 40
    assert bucket_key_for_oi_signal(SIGNIFICANT_LONG_BUILDUP) is None

    session = date(2026, 8, 23)
    candles = []
    for i, day in enumerate(range(17, 24)):
        candles.append(
            {
                "timestamp": f"2026-08-{day:02d}T00:00:00+05:30",
                "close": 100.0,
                "oi": 1000.0,
                "volume": 100.0 + i,
            }
        )
    bars = completed_daily_bars(candles, session)
    assert [b["date"] for b in bars] == [
        "2026-08-17",
        "2026-08-18",
        "2026-08-19",
        "2026-08-20",
        "2026-08-21",
        "2026-08-22",
    ]
    assert five_day_significant(bars, price_now=102.0, oi_now=1050.0, volume_today=121) == (
        SIGNIFICANT_LONG_BUILDUP
    )
    assert five_day_significant(bars, price_now=98.0, oi_now=1050.0, volume_today=121) == (
        SIGNIFICANT_SHORT_BUILDUP
    )
    assert five_day_significant(bars, price_now=101.9, oi_now=1050.0, volume_today=121) is None
    assert five_day_significant(bars, price_now=102.0, oi_now=1049.0, volume_today=121) is None
    assert five_day_significant(bars, price_now=102.0, oi_now=1050.0, volume_today=103) is None
    assert five_day_significant(bars, price_now=None, oi_now=1050.0, volume_today=121) is None
    assert five_day_significant(bars, price_now=0, oi_now=1050.0, volume_today=121) is None
    hole = [dict(b) for b in bars]
    hole[-3]["volume"] = None
    assert five_day_significant(hole, price_now=102.0, oi_now=1050.0, volume_today=999) is None
    assert five_day_significant(bars[:4], price_now=102.0, oi_now=1050.0, volume_today=999) is None


def test_heatmap_futures_oi_is_not_an_nse_mcp_tool():
    from backend.services.market_data.gateway import resolve_capability
    from backend.services.market_data.nse_mcp_client import VERIFIED_TOOLS

    fut = "NSE_FO|12345"
    oi_cap = resolve_capability(fut, data_type="oi")
    daily_cap = resolve_capability(fut, data_type="candles", interval="days/1")
    assert oi_cap.supported is False
    assert oi_cap.reason == "open_interest"
    assert daily_cap.supported is False
    assert daily_cap.reason == "futures_or_options"
    names = set()
    for group in VERIFIED_TOOLS.values():
        names |= set(group)
    assert "cm_get_stock_quote" in names
    assert "get_stock_history" in names
    for name in names:
        lowered = name.lower()
        assert "open_interest" not in lowered
        assert "future" not in lowered


def _bar(day, hh, mm, close, oi, volume):
    return {
        "timestamp": f"{day}T{hh:02d}:{mm:02d}:00+05:30",
        "close": close,
        "oi": oi,
        "volume": volume,
    }


def test_heatmap_columns_follow_script_bars_not_quote_or_ten_sessions():
    """PNBHOUSING-style: a tiny last-15-minute OI uptick must not set HTF.

    HTF is Friday versus Thursday. A 10-session rise is ignored. The quote
    delta is not an input to this classifier.
    """
    from backend.services.oi_heatmap_bars import classify_heatmap_timeframes

    friday = "2026-10-09"
    # 15-minute bars. OI Signal is 15:15 vs 15:00. Prev-OI is 15:00 vs 14:45.
    bars_15 = [
        _bar(friday, 13, 30, 780, 4_800_000, 1000),
        _bar(friday, 13, 45, 780, 4_800_000, 1000),
        _bar(friday, 14, 0, 780, 4_800_000, 1000),
        _bar(friday, 14, 15, 780, 4_800_000, 1000),
        _bar(friday, 14, 30, 781, 4_810_000, 500),
        _bar(friday, 14, 45, 783, 4_810_000, 500),
        _bar(friday, 15, 0, 782, 4_800_000, 500),
        _bar(friday, 15, 15, 783, 4_801_950, 2000),
    ]
    bars_60 = [
        _bar(friday, 13, 15, 790, 4_700_000, 4000),
        _bar(friday, 14, 15, 790, 4_700_000, 4000),
        _bar(friday, 15, 15, 783, 4_801_950, 2000),
    ]
    # Ten sessions of OI/price rise, then Friday's drop. HTF must be the drop.
    daily = []
    for i, day in enumerate(range(1, 8)):
        daily.append(
            {
                "timestamp": f"2026-10-{day:02d}T00:00:00+05:30",
                "close": 700.0 + i * 20,
                "oi": 3_000_000.0 + i * 200_000,
            }
        )
    daily.append({"timestamp": "2026-10-08T00:00:00+05:30", "close": 900.0, "oi": 5_200_000.0})
    daily.append({"timestamp": "2026-10-09T00:00:00+05:30", "close": 783.0, "oi": 4_801_950.0})

    sunday = _ist(2026, 10, 11, 12, 0)
    got = classify_heatmap_timeframes(
        bars_15=bars_15,
        bars_60=bars_60,
        daily_bars=daily,
        now=sunday,
        market_closed=True,
    )
    assert got["oi_signal"] == "LONG_BUILDUP"
    assert got["prev_oi_signal"] == "LONG_UNWIND"
    assert got["mtf_oi_signal"] == "SHORT_BUILDUP"
    assert got["htf_oi_signal"] == "LONG_UNWIND"
    assert got["oi_chg_pct"] == (4_801_950 / 4_800_000 - 1) * 100
    assert got["volume_arrow"] == "up"


def test_forming_15m_bar_does_not_override_completed_signal():
    from backend.services.oi_heatmap_bars import classify_heatmap_timeframes

    friday = "2026-10-09"
    bars_15 = [
        _bar(friday, 14, 0, 100, 10000, 100),
        _bar(friday, 14, 15, 100, 10000, 100),
        _bar(friday, 14, 30, 100, 10000, 100),
        _bar(friday, 14, 45, 101, 10100, 100),
        _bar(friday, 15, 0, 102, 10200, 100),
        _bar(friday, 15, 15, 90, 5000, 50),
    ]
    # 15:20 is still inside the 15:15 bar (session ends 15:30).
    now = _ist(2026, 10, 9, 15, 20)
    got = classify_heatmap_timeframes(
        bars_15=bars_15,
        bars_60=[
            _bar(friday, 13, 15, 100, 9000, 100),
            _bar(friday, 14, 15, 102, 10200, 100),
            _bar(friday, 15, 15, 90, 5000, 50),
        ],
        daily_bars=[
            {"timestamp": "2026-10-08T00:00:00+05:30", "close": 100, "oi": 10000},
            {"timestamp": "2026-10-09T00:00:00+05:30", "close": 102, "oi": 11000},
        ],
        now=now,
        market_closed=False,
    )
    # Completed 15:00 vs 14:45 is long buildup. The forming crash is ignored.
    assert got["oi_signal"] == "LONG_BUILDUP"
    assert got["prev_oi_signal"] == "LONG_BUILDUP"
    # Forming 15:15 hour is open; MTF is 14:15 vs 13:15.
    assert got["mtf_oi_signal"] == "LONG_BUILDUP"
    # Forming day versus Thursday, not the 15-minute crash.
    assert got["htf_oi_signal"] == "LONG_BUILDUP"
    # Volume uses the forming candle (50) against the previous five 100s.
    assert got["volume_arrow"] == "down"


def test_volume_arrow_flat_and_missing_stay_blank():
    from backend.services.oi_heatmap_bars import classify_heatmap_timeframes, volume_arrow

    assert volume_arrow(100.4, [100, 100, 100, 100, 100]) == "flat"
    assert volume_arrow(100.6, [100, 100, 100, 100, 100]) == "up"
    assert volume_arrow(99.4, [100, 100, 100, 100, 100]) == "down"
    assert volume_arrow(None, [100, 100, 100, 100, 100]) is None
    assert volume_arrow(100, [100, 100, None, 100, 100]) is None
    assert volume_arrow(100, [100, 100, 100, 100]) is None

    friday = "2026-10-09"
    bars = [
        _bar(friday, 14, 0, 10, 100, 100),
        _bar(friday, 14, 15, 10, 100, 100),
        _bar(friday, 14, 30, 10, 100, 100),
        _bar(friday, 14, 45, 10, 100, 100),
        _bar(friday, 15, 0, 10, 100, 100),
        _bar(friday, 15, 15, 10, 100, None),
    ]
    got = classify_heatmap_timeframes(
        bars_15=bars,
        bars_60=[],
        daily_bars=[],
        now=_ist(2026, 10, 11, 12, 0),
        market_closed=True,
    )
    assert got["volume_arrow"] is None
    assert got["oi_signal"] == "NEUTRAL"


def test_missing_oi_is_blank_not_zero():
    from backend.services.oi_heatmap_bars import classify_heatmap_timeframes

    friday = "2026-10-09"
    bars = [
        _bar(friday, 14, 45, 100, 1000, 10),
        _bar(friday, 15, 0, 110, 1100, 10),
        _bar(friday, 15, 15, 120, None, 10),
    ]
    got = classify_heatmap_timeframes(
        bars_15=bars,
        daily_bars=[
            {"timestamp": "2026-10-08T00:00:00+05:30", "close": 100, "oi": 1000},
            {"timestamp": "2026-10-09T00:00:00+05:30", "close": 90, "oi": None},
        ],
        now=_ist(2026, 10, 11, 12, 0),
        market_closed=True,
    )
    assert got["oi_signal"] is None
    assert got["oi_chg_pct"] is None
    assert got["htf_oi_signal"] is None


def test_open_session_without_today_does_not_reuse_prior_day_as_htf():
    from backend.services.oi_heatmap_htf import htf_oi_from_daily_bars

    bars = [
        {"timestamp": "2026-10-08T00:00:00+05:30", "close": 100, "oi": 1000},
        {"timestamp": "2026-10-09T00:00:00+05:30", "close": 110, "oi": 1100},
    ]
    assert (
        htf_oi_from_daily_bars(
            bars,
            now=_ist(2026, 10, 12, 11, 0),
            market_closed=False,
        )
        is None
    )
    forming = bars + [
        {"timestamp": "2026-10-12T00:00:00+05:30", "close": 108, "oi": 1200}
    ]
    rec = htf_oi_from_daily_bars(forming, now=_ist(2026, 10, 12, 11, 0), market_closed=False)
    assert rec["bar_latest"].startswith("2026-10-12")
    assert rec["bar_prior"].startswith("2026-10-09")
    assert rec["htf_oi_signal"] == "SHORT_BUILDUP"
    assert rec["sessions_back"] == 1


def test_open_refresh_returns_before_upstox_batch(monkeypatch):
    """The post-09:00 heatmap fill must not block the single API worker."""
    import threading
    import time

    from backend.services import oi_heatmap as hm
    from backend.services import breakfast_upstox_gate as gate

    started = threading.Event()
    release = threading.Event()
    caller = threading.get_ident()
    seen = {}

    def slow_refresh():
        seen["thread"] = threading.get_ident()
        started.set()
        assert release.wait(2)

    monkeypatch.setattr(hm, "ist_use_today_only_db_snapshot", lambda now=None: True)
    monkeypatch.setattr(hm, "should_skip_scheduled_market_jobs_ist", lambda now=None: False)
    monkeypatch.setattr(hm, "is_oi_heatmap_overnight_freeze", lambda now=None: False)
    monkeypatch.setattr(hm.settings, "OI_HEATMAP_LIVE_ENABLED", True)
    monkeypatch.setattr(hm, "refresh_oi_heatmap_live", slow_refresh)
    monkeypatch.setattr(hm, "_last_api_refresh_attempt_mono", 0.0)
    monkeypatch.setattr(gate, "breakfast_exclusivity_active", lambda now=None: False)

    t0 = time.monotonic()
    hm.maybe_sync_refresh_heatmap_after_open("test")
    assert time.monotonic() - t0 < 0.5
    assert started.wait(1)
    assert seen["thread"] != caller
    release.set()

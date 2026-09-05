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

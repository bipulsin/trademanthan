"""Breakfast daily wick classification and Live Live-tab filter."""
from __future__ import annotations

from datetime import date
from types import SimpleNamespace

from backend.services.breakfast_prev_close import (
    WICK_LONG_DOWN,
    WICK_LONG_UP,
    WICK_NONE,
    classify_daily_wick,
    filter_live_stocks_by_wick,
    filter_sector_members_by_wick,
    required_wick_for_live_direction,
)


def test_wicks_within_5pct_are_none():
    # body=10, upper=5.00, lower=4.80 → |u-l|/max = 0.04 ≤ 0.05
    assert classify_daily_wick(100, 115, 95.2, 110) == WICK_NONE


def test_long_up_wick():
    # open=100 close=110 body=10, high=115 upper=5, low=109 lower=1
    assert classify_daily_wick(100, 115, 109, 110) == WICK_LONG_UP


def test_long_down_wick():
    # open=110 close=100 body=10, high=111 upper=1, low=95 lower=5
    assert classify_daily_wick(110, 111, 95, 100) == WICK_LONG_DOWN


def test_longer_wick_but_under_30pct_of_body_is_none():
    # body=10, upper=2 (20%), lower=0.5
    assert classify_daily_wick(100, 112, 99.5, 110) == WICK_NONE


def test_zero_body_is_none():
    # doji: body=0 even with a long upper wick
    assert classify_daily_wick(100, 120, 99, 100) == WICK_NONE


def test_both_wicks_zero_is_none():
    assert classify_daily_wick(100, 110, 100, 110) == WICK_NONE


def test_required_wick_for_direction():
    assert required_wick_for_live_direction("SHORT") == WICK_LONG_UP
    assert required_wick_for_live_direction("LONG") == WICK_LONG_DOWN
    assert required_wick_for_live_direction("UNKNOWN") is None


def test_filter_live_stocks_short_keeps_only_long_up():
    stocks = [
        {"symbol": "AAA", "wick": WICK_LONG_UP, "move_pct_at_entry": -1},
        {"symbol": "BBB", "wick": WICK_LONG_DOWN, "move_pct_at_entry": -2},
        {"symbol": "CCC", "wick": WICK_NONE, "move_pct_at_entry": -0.5},
    ]
    out = filter_live_stocks_by_wick(stocks, direction="SHORT")
    assert [s["symbol"] for s in out] == ["AAA"]
    assert out[0]["stock_rank"] == 1


def test_filter_live_stocks_long_empty_when_none_match():
    stocks = [
        {"symbol": "AAA", "wick": WICK_LONG_UP},
        {"symbol": "BBB", "wick": WICK_NONE},
    ]
    assert filter_live_stocks_by_wick(stocks, direction="LONG") == []


def test_filter_unknown_direction_empty():
    stocks = [{"symbol": "AAA", "wick": WICK_LONG_DOWN}]
    assert filter_live_stocks_by_wick(stocks, direction="UNKNOWN") == []


def test_filter_sector_members_by_wick():
    members = {
        "sec": [
            {"stock": "AAA"},
            {"stock": "BBB"},
            {"stock": "CCC"},
        ]
    }
    wicks = {"AAA": WICK_LONG_DOWN, "BBB": WICK_LONG_UP, "CCC": WICK_NONE}
    out = filter_sector_members_by_wick(members, wicks, long_side=True)
    assert [m["stock"] for m in out["sec"]] == ["AAA"]
    out_short = filter_sector_members_by_wick(members, wicks, long_side=False)
    assert [m["stock"] for m in out_short["sec"]] == ["BBB"]


def test_live_payload_includes_wick_and_drops_mismatch():
    from datetime import datetime

    import pytz

    from backend.services.breakfast_strategy.live_tick import _build_payload_from_selection

    now = pytz.timezone("Asia/Kolkata").localize(datetime(2026, 9, 1, 9, 20, 5))
    bar = {"open": 100, "high": 102, "low": 99, "close": 101, "volume": 1}

    def _stk(sym):
        row = SimpleNamespace(
            stock=sym,
            display_symbol=sym,
            instrument_label=sym,
            sector="IT",
            lot_size=50,
            instrument_key="NSE_FO|" + sym,
            price_source="futures",
        )
        return SimpleNamespace(
            row=row,
            stock_rank=1,
            move_pct=1.0,
            anchor_bar=bar,
            signal_bar=bar,
        )

    sp = SimpleNamespace(
        sector_key="NSE_INDEX|Nifty IT",
        sector_rank=1,
        sector_move_pct=0.5,
        sector_volume=1,
        stocks=[_stk("AAA"), _stk("BBB")],
    )
    sel = SimpleNamespace(
        long_side=True,
        nifty_bias="positive",
        nifty_bias_pct=1.0,
        ranked_sectors=[("NSE_INDEX|Nifty IT", 0.5, 1)],
        sector_picks=[sp],
    )
    payload = _build_payload_from_selection(
        now=now,
        session_date=date(2026, 9, 1),
        phase="frozen",
        sel=sel,
        nifty_bar=bar,
        tick_minute=20,
        elapsed_sec=1.0,
        data_source="rest_5m",
        nifty_prev_close=100.0,
        wick_by_symbol={"AAA": WICK_LONG_DOWN, "BBB": WICK_LONG_UP},
    )
    stocks = payload["sectors"][0]["stocks"]
    assert [s["symbol"] for s in stocks] == ["AAA"]
    assert stocks[0]["wick"] == WICK_LONG_DOWN

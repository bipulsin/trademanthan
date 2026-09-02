"""Breakfast daily wick classification and Live Live-tab filter."""
from __future__ import annotations

from datetime import date
from types import SimpleNamespace

from backend.services.breakfast_prev_close import (
    WICK_LONG_DOWN,
    WICK_LONG_UP,
    WICK_NONE,
    classify_daily_wick,
    ensure_live_stock_wicks,
    filter_live_stocks_by_first_5m_color,
    filter_live_stocks_by_wick,
    filter_live_stocks_by_wick_and_color,
    filter_sector_members_by_first_5m_color,
    filter_sector_members_by_wick,
    first_5m_color_matches_direction,
    group_wicks_by_sector,
    partition_filled_wicks,
    required_wick_for_live_direction,
    sector_label_for_wicks,
)


def test_ensure_live_stock_wicks_fills_missing():
    sectors = [
        {
            "stocks": [
                {"symbol": "AAA"},
                {"symbol": "BBB", "wick": WICK_LONG_UP},
            ]
        }
    ]
    ensure_live_stock_wicks(sectors, {"AAA": WICK_LONG_DOWN})
    assert sectors[0]["stocks"][0]["wick"] == WICK_LONG_DOWN
    assert sectors[0]["stocks"][1]["wick"] == WICK_LONG_UP
    missing = [{"stocks": [{"symbol": "CCC"}]}]
    ensure_live_stock_wicks(missing, {})
    assert missing[0]["stocks"][0]["wick"] == WICK_NONE


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
    assert stocks[0]["signal_open"] == 100
    assert stocks[0]["signal_close"] == 101


def test_first_5m_color_matches_direction():
    assert first_5m_color_matches_direction(100, 101, "LONG") is True
    assert first_5m_color_matches_direction(100, 99, "LONG") is False
    assert first_5m_color_matches_direction(100, 100, "LONG") is True
    assert first_5m_color_matches_direction(100, 99, "SHORT") is True
    assert first_5m_color_matches_direction(100, 101, "SHORT") is False
    assert first_5m_color_matches_direction(100, 100, "SHORT") is True
    assert first_5m_color_matches_direction(100, 101, "UNKNOWN") is False


def test_filter_live_stocks_by_first_5m_color_long_keeps_green_only():
    stocks = [
        {"symbol": "GRN", "signal_open": 100, "signal_close": 101},
        {"symbol": "RED", "signal_open": 100, "signal_close": 99},
        {"symbol": "DOJ", "signal_open": 100, "signal_close": 100},
        {"symbol": "MISS", "signal_close": 101},
    ]
    out = filter_live_stocks_by_first_5m_color(stocks, direction="LONG")
    assert [s["symbol"] for s in out] == ["GRN", "DOJ"]
    assert out[0]["stock_rank"] == 1
    assert out[1]["is_doji"] is True


def test_filter_live_stocks_by_first_5m_color_short_keeps_red_only():
    stocks = [
        {"symbol": "GRN", "signal_open": 100, "signal_close": 101},
        {"symbol": "RED", "signal_open": 100, "signal_close": 99},
    ]
    out = filter_live_stocks_by_first_5m_color(stocks, direction="SHORT")
    assert [s["symbol"] for s in out] == ["RED"]


def test_filter_wick_and_color_together_long():
    stocks = [
        {
            "symbol": "OK",
            "wick": WICK_LONG_DOWN,
            "signal_open": 100,
            "signal_close": 101,
        },
        {
            "symbol": "WRONG_COLOR",
            "wick": WICK_LONG_DOWN,
            "signal_open": 100,
            "signal_close": 99,
        },
        {
            "symbol": "WRONG_WICK",
            "wick": WICK_LONG_UP,
            "signal_open": 100,
            "signal_close": 101,
        },
        {
            "symbol": "DOJI",
            "wick": WICK_LONG_DOWN,
            "signal_open": 100,
            "signal_close": 100,
        },
    ]
    out = filter_live_stocks_by_wick_and_color(stocks, direction="LONG")
    assert [s["symbol"] for s in out] == ["OK", "DOJI"]
    assert out[1]["is_doji"] is True


def test_filter_wick_and_color_together_short():
    stocks = [
        {
            "symbol": "OK",
            "wick": WICK_LONG_UP,
            "signal_open": 100,
            "signal_close": 99,
        },
        {
            "symbol": "GREEN",
            "wick": WICK_LONG_UP,
            "signal_open": 100,
            "signal_close": 101,
        },
    ]
    out = filter_live_stocks_by_wick_and_color(stocks, direction="SHORT")
    assert [s["symbol"] for s in out] == ["OK"]


def test_filter_wick_and_color_empty_when_none_pass():
    stocks = [
        {"symbol": "A", "wick": WICK_LONG_DOWN, "signal_open": 100, "signal_close": 99},
    ]
    assert filter_live_stocks_by_wick_and_color(stocks, direction="LONG") == []


def test_filter_sector_members_by_first_5m_color():
    members = {"sec": [{"stock": "AAA"}, {"stock": "BBB"}, {"stock": "CCC"}]}
    bars = {
        "AAA": {"open": 100, "close": 101},
        "BBB": {"open": 100, "close": 99},
        "CCC": {"open": 100, "close": 100},
    }
    out = filter_sector_members_by_first_5m_color(members, bars, long_side=True)
    assert [m["stock"] for m in out["sec"]] == ["AAA", "CCC"]
    out_short = filter_sector_members_by_first_5m_color(members, bars, long_side=False)
    assert [m["stock"] for m in out_short["sec"]] == ["BBB", "CCC"]


def test_live_payload_drops_wrong_5m_color_even_with_wick():
    from datetime import datetime

    import pytz

    from backend.services.breakfast_strategy.live_tick import _build_payload_from_selection

    now = pytz.timezone("Asia/Kolkata").localize(datetime(2026, 9, 1, 9, 20, 5))
    green = {"open": 100, "high": 102, "low": 99, "close": 101, "volume": 1}
    red = {"open": 100, "high": 102, "low": 98, "close": 99, "volume": 1}

    def _stk(sym, bar):
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
        stocks=[_stk("AAA", green), _stk("BBB", red)],
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
        nifty_bar=green,
        tick_minute=20,
        elapsed_sec=1.0,
        data_source="rest_5m",
        nifty_prev_close=100.0,
        wick_by_symbol={"AAA": WICK_LONG_DOWN, "BBB": WICK_LONG_DOWN},
    )
    stocks = payload["sectors"][0]["stocks"]
    assert [s["symbol"] for s in stocks] == ["AAA"]


def test_pick_stocks_ranks_long_by_highest_prev_close_pct():
    from backend.services.breakfast_strategy.universe import StockRow, pick_stocks_in_sector

    members = [{"stock": "LOW", "sector": "IT", "sector_index": "IT"}, {"stock": "HIGH", "sector": "IT", "sector_index": "IT"}]
    bars = {
        "LOW": {"open": 100, "close": 101, "volume": 1},
        "HIGH": {"open": 100, "close": 103, "volume": 1},
    }
    pcts = {"LOW": 1.0, "HIGH": 3.0}
    rows = {
        "LOW": StockRow("LOW", "LOW", "LOW", "IT", "IT", "NSE_FO|LOW", 50, "futures"),
        "HIGH": StockRow("HIGH", "HIGH", "HIGH", "IT", "IT", "NSE_FO|HIGH", 50, "futures"),
    }
    out = pick_stocks_in_sector(
        members,
        bars,
        pcts,
        session_date=date(2026, 9, 1),
        fut_by_und={},
        eq_by_symbol={},
        long_side=True,
        top_n=2,
        session_rows=rows,
    )
    assert [r.stock for r in out] == ["HIGH", "LOW"]


def test_pick_stocks_ranks_short_by_lowest_prev_close_pct():
    from backend.services.breakfast_strategy.universe import StockRow, pick_stocks_in_sector

    members = [{"stock": "MILD", "sector": "IT", "sector_index": "IT"}, {"stock": "DEEP", "sector": "IT", "sector_index": "IT"}]
    bars = {
        "MILD": {"open": 100, "close": 99, "volume": 1},
        "DEEP": {"open": 100, "close": 97, "volume": 1},
    }
    pcts = {"MILD": -1.0, "DEEP": -3.0}
    rows = {
        "MILD": StockRow("MILD", "MILD", "MILD", "IT", "IT", "NSE_FO|MILD", 50, "futures"),
        "DEEP": StockRow("DEEP", "DEEP", "DEEP", "IT", "IT", "NSE_FO|DEEP", 50, "futures"),
    }
    out = pick_stocks_in_sector(
        members,
        bars,
        pcts,
        session_date=date(2026, 9, 1),
        fut_by_und={},
        eq_by_symbol={},
        long_side=False,
        top_n=2,
        session_rows=rows,
    )
    assert [r.stock for r in out] == ["DEEP", "MILD"]


def test_partition_filled_wicks_excludes_none_and_sorts_futures():
    rows = [
        {"future_symbol": "ZEE25", "stock": "ZEE", "wick": WICK_LONG_DOWN},
        {"future_symbol": "AAA25", "stock": "AAA", "wick": WICK_LONG_UP},
        {"future_symbol": "BBB25", "stock": "BBB", "wick": WICK_NONE},
        {"future_symbol": "", "stock": "CCC", "wick": WICK_LONG_DOWN},
        {"future_symbol": "MMM25", "stock": "MMM", "wick": WICK_LONG_UP},
        {"stock": "SKIP", "wick": "garbage"},
    ]
    out = partition_filled_wicks(rows)
    assert [r["future_symbol"] for r in out["long_down_wick"]] == ["CCC", "ZEE25"]
    assert [r["wick"] for r in out["long_down_wick"]] == [WICK_LONG_DOWN, WICK_LONG_DOWN]
    assert [r["future_symbol"] for r in out["long_up_wick"]] == ["AAA25", "MMM25"]
    assert all(r["wick"] != WICK_NONE for r in out["long_down_wick"] + out["long_up_wick"])


def test_sector_label_for_wicks_maps_upstox_key():
    assert sector_label_for_wicks("NSE_INDEX|Nifty Auto") == "Nifty Auto"
    assert sector_label_for_wicks("NSE_INDEX|Nifty Pvt Bank") == "Nifty Private Bank"
    assert sector_label_for_wicks("") == "Unmapped"


def test_group_wicks_by_sector_splits_none_and_sides():
    rows = [
        {
            "stock": "tatamotors",
            "sector_index": "NSE_INDEX|Nifty Auto",
            "wick": WICK_LONG_UP,
            "prev_session_close": 900.5,
        },
        {
            "stock": "M&M",
            "sector_index": "NSE_INDEX|Nifty Auto",
            "wick": WICK_LONG_DOWN,
            "prev_session_close": 2800,
        },
        {
            "stock": "BAJAJ-AUTO",
            "sector_index": "NSE_INDEX|Nifty Auto",
            "wick": None,
            "prev_session_close": 8500,
        },
        {
            "stock": "INFY",
            "sector_index": "NSE_INDEX|Nifty IT",
            "wick": WICK_NONE,
            "prev_session_close": 1500,
        },
        {
            "stock": "TCS",
            "sector_index": "NSE_INDEX|Nifty IT",
            "wick": "garbage",
            "prev_session_close": None,
        },
    ]
    out = group_wicks_by_sector(rows)
    assert [s["sector"] for s in out] == ["Nifty Auto", "Nifty IT"]
    auto = out[0]
    assert [r["stock"] for r in auto["long_up_wick"]] == ["TATAMOTORS"]
    assert auto["long_up_wick"][0]["prev_session_close"] == "900.50"
    assert [r["stock"] for r in auto["long_down_wick"]] == ["M&M"]
    assert auto["none"] == ["BAJAJ-AUTO"]
    it = out[1]
    assert it["long_up_wick"] == []
    assert it["long_down_wick"] == []
    assert it["none"] == ["INFY", "TCS"]
    assert it["none"]  # TCS null wick treated as NONE

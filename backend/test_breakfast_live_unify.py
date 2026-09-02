"""Breakfast live selection unify: same-side rank, wick-before-fetch, doji, cascade."""
from __future__ import annotations

from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from backend.services.breakfast_prev_close import (
    WICK_LONG_DOWN,
    WICK_LONG_UP,
    filter_sector_members_by_sign_gate,
    filter_sector_members_by_wick,
)
from backend.services.breakfast_strategy.live_tick import (
    _rank_picked_sectors,
    live_lock_failure_reason,
    try_one_sector_cascade,
)
from backend.services.breakfast_strategy.universe import pick_stocks_in_sector, StockRow, sector_index_key_for_label


def test_no_gainer_loser_books():
    import backend.services.breakfast_strategy.live_tick as m

    assert not hasattr(m, "_gainer_loser_books")
    import backend.services.breakfast_strategy.live as live_mod
    src = open(live_mod.__file__).read()
    assert "_gainer_loser_books" not in src


def test_rank_forming_and_freeze_same_side():
    bank = sector_index_key_for_label("Nifty Private Bank")
    it = sector_index_key_for_label("Nifty IT")
    pharma = sector_index_key_for_label("Nifty Auto")
    session = date(2026, 9, 2)
    nifty = [
        {"timestamp": "2026-09-02T09:15:00+05:30", "open": 100, "high": 101, "low": 99, "close": 100.4, "volume": 1}
    ]
    candles = {
        "NSE_INDEX|Nifty 50": nifty,
        bank: [
            {"timestamp": "2026-09-02T09:15:00+05:30", "open": 100, "high": 104, "low": 99, "close": 103, "volume": 2}
        ],
        it: [
            {"timestamp": "2026-09-02T09:15:00+05:30", "open": 100, "high": 102, "low": 99, "close": 101.5, "volume": 2}
        ],
        pharma: [
            {"timestamp": "2026-09-02T09:15:00+05:30", "open": 100, "high": 101, "low": 96, "close": 97, "volume": 2}
        ],
    }
    stocks = {bank: [{"stock": "A"}], it: [{"stock": "B"}], pharma: [{"stock": "C"}]}
    prev = {"NSE_INDEX|Nifty 50": 100.0, bank: 100.0, it: 100.0, pharma: 100.0}
    with patch(
        "backend.services.breakfast_strategy.live_tick.fo_eligible_sector_keys",
        return_value={bank, it, pharma},
    ):
        forming, long_f = _rank_picked_sectors(
            session_date=session,
            candles_1m=candles,
            stocks_by_sector=stocks,
            fut_by_und={},
            eq_by_symbol={},
            upto_hhmm=(9, 19),
            nifty_prev_close=100.0,
            sector_prev_closes=prev,
        )
        freeze, long_z = _rank_picked_sectors(
            session_date=session,
            candles_1m=candles,
            stocks_by_sector=stocks,
            fut_by_und={},
            eq_by_symbol={},
            upto_hhmm=(9, 20),
            nifty_prev_close=100.0,
            sector_prev_closes=prev,
        )
    assert long_f is True and long_z is True
    assert forming == freeze
    assert forming[0] == bank
    assert pharma not in forming[:2]


def test_rank_short_same_side_when_nifty_negative():
    bank = sector_index_key_for_label("Nifty Private Bank")
    it = sector_index_key_for_label("Nifty IT")
    session = date(2026, 9, 2)
    candles = {
        "NSE_INDEX|Nifty 50": [
            {"timestamp": "2026-09-02T09:15:00+05:30", "open": 100, "high": 100, "low": 99, "close": 99.5, "volume": 1}
        ],
        bank: [
            {"timestamp": "2026-09-02T09:15:00+05:30", "open": 100, "high": 101, "low": 98, "close": 99.2, "volume": 1}
        ],
        it: [
            {"timestamp": "2026-09-02T09:15:00+05:30", "open": 100, "high": 100, "low": 94, "close": 95, "volume": 1}
        ],
    }
    stocks = {bank: [{"stock": "A"}], it: [{"stock": "B"}]}
    prev = {"NSE_INDEX|Nifty 50": 100.0, bank: 100.0, it: 100.0}
    with patch(
        "backend.services.breakfast_strategy.live_tick.fo_eligible_sector_keys",
        return_value={bank, it},
    ):
        ranked, long_side = _rank_picked_sectors(
            session_date=session,
            candles_1m=candles,
            stocks_by_sector=stocks,
            fut_by_und={},
            eq_by_symbol={},
            upto_hhmm=(9, 20),
            nifty_prev_close=100.0,
            sector_prev_closes=prev,
        )
    assert long_side is False
    assert ranked[0] == it


def test_wick_prefilter_before_fetch_order():
    from backend.services.breakfast_strategy.live_tick import _members_for_books

    bank = "NSE_INDEX|Nifty Bank"
    members = {bank: [{"stock": "AAA"}, {"stock": "BBB"}]}
    wicks = {"AAA": WICK_LONG_DOWN, "BBB": WICK_LONG_UP}
    books = [(bank, True)]
    wick_members = _members_for_books(members, wicks, books)
    assert [m["stock"] for m in wick_members[bank]] == ["AAA"]
    assert filter_sector_members_by_wick(members, wicks, long_side=True)[bank][0]["stock"] == "AAA"


def test_sign_gate_excludes_plus_minus_4pct():
    members = {"s": [{"stock": "CAP"}, {"stock": "OK"}, {"stock": "FLAT"}]}
    pcts = {"CAP": 4.0, "OK": 1.5, "FLAT": 0.0}
    out = filter_sector_members_by_sign_gate(members, pcts, long_side=True, move_cap=4.0)
    assert [m["stock"] for m in out["s"]] == ["OK"]
    pcts_s = {"CAP": -4.0, "OK": -1.5, "FLAT": 0.0}
    out_s = filter_sector_members_by_sign_gate(members, pcts_s, long_side=False, move_cap=4.0)
    assert [m["stock"] for m in out_s["s"]] == ["OK"]


def test_pick_stocks_still_excludes_4pct_cap():
    members = [{"stock": "CAP", "sector": "IT", "sector_index": "IT"}]
    bars = {"CAP": {"open": 100, "close": 104, "volume": 10}}
    rows = {"CAP": StockRow("CAP", "CAP", "CAP", "IT", "IT", "NSE_FO|CAP", 50, "futures")}
    out = pick_stocks_in_sector(
        members,
        bars,
        {"CAP": 4.0},
        session_date=date(2026, 9, 2),
        fut_by_und={},
        eq_by_symbol={},
        long_side=True,
        move_cap=4.0,
        top_n=3,
        session_rows=rows,
    )
    assert out == []


def test_cascade_one_swap_only():
    picked = ["A", "B"]
    ranked = ["A", "B", "C", "D"]
    after = {"A": [{"stock": "x"}], "B": []}
    new, frm, to, swapped = try_one_sector_cascade(picked, ranked, after)
    assert swapped is True and frm == "B" and to == "C"
    assert new == ["A", "C"]
    new2, _, _, swapped2 = try_one_sector_cascade(new, ranked, {"A": [{"stock": "x"}], "C": []})
    assert swapped2 is True
    both_empty, _, _, sw = try_one_sector_cascade(["A", "B"], ranked, {"A": [], "B": []})
    assert sw is False
    assert both_empty == ["A", "B"]


def test_lock_reason_wick_vs_color_vs_cascade():
    assert (
        live_lock_failure_reason(
            nifty_unknown=False,
            nifty_bar_missing=False,
            swapped=False,
            n_sectors_with_stocks=0,
            top2_wick_counts=[0, 0],
            top2_after_color_counts=[0, 0],
        )
        == "no_filtered_stocks:wick"
    )
    assert (
        live_lock_failure_reason(
            nifty_unknown=False,
            nifty_bar_missing=False,
            swapped=False,
            n_sectors_with_stocks=0,
            top2_wick_counts=[2, 1],
            top2_after_color_counts=[0, 0],
        )
        == "no_filtered_stocks:color"
    )
    assert (
        live_lock_failure_reason(
            nifty_unknown=False,
            nifty_bar_missing=False,
            swapped=True,
            n_sectors_with_stocks=1,
            top2_wick_counts=[2, 0],
            top2_after_color_counts=[2, 0],
        )
        == "no_filtered_stocks:cascade_exhausted"
    )
    assert (
        live_lock_failure_reason(
            nifty_unknown=True,
            nifty_bar_missing=True,
            swapped=False,
            n_sectors_with_stocks=0,
            top2_wick_counts=[],
            top2_after_color_counts=[],
        )
        == "no_data"
    )
    assert (
        live_lock_failure_reason(
            nifty_unknown=False,
            nifty_bar_missing=False,
            swapped=False,
            n_sectors_with_stocks=1,
            top2_wick_counts=[1, 0],
            top2_after_color_counts=[1, 0],
        )
        is None
    )


def test_payload_keeps_doji_with_flag():
    from datetime import datetime

    import pytz

    from backend.services.breakfast_strategy.live_tick import _build_payload_from_selection

    now = pytz.timezone("Asia/Kolkata").localize(datetime(2026, 9, 2, 9, 20, 5))
    doji = {"open": 100, "high": 101, "low": 99, "close": 100, "volume": 1}

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
            anchor_bar=doji,
            signal_bar=doji,
        )

    sp = SimpleNamespace(
        sector_key="NSE_INDEX|Nifty IT",
        sector_rank=1,
        sector_move_pct=0.5,
        sector_volume=1,
        stocks=[_stk("DOJ")],
        long_side=True,
    )
    sel = SimpleNamespace(
        long_side=True,
        nifty_bias="positive",
        nifty_bias_pct=0.1,
        ranked_sectors=[],
        sector_picks=[sp],
    )
    payload = _build_payload_from_selection(
        now=now,
        session_date=date(2026, 9, 2),
        phase="frozen",
        sel=sel,
        nifty_bar={"open": 100, "high": 101, "low": 99, "close": 100.2, "volume": 1},
        tick_minute=20,
        elapsed_sec=1.0,
        data_source="rest_5m",
        nifty_prev_close=100.0,
        wick_by_symbol={"DOJ": WICK_LONG_DOWN},
    )
    stocks = payload["sectors"][0]["stocks"]
    assert stocks[0]["symbol"] == "DOJ"
    assert stocks[0]["is_doji"] is True

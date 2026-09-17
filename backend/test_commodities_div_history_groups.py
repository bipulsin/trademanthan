"""Unit tests for CommDiv History month grouping / Overall LIVE PnL helpers."""
from backend.services.commodities_div.history_groups import (
    exec_month_key,
    format_month_label,
    group_rows_by_month,
    overall_live_pnl,
    row_pnl,
    sum_pnl,
)


def test_exec_month_key_prefers_exit():
    assert (
        exec_month_key(
            {
                "exit_at": "2026-09-15 14:30:00",
                "trade_taken_at": "2026-08-01 10:00:00",
            }
        )
        == "2026-09"
    )


def test_exec_month_key_falls_back_to_entry():
    assert exec_month_key({"trade_taken_at": "2026-03-02 09:00:00"}) == "2026-03"
    assert exec_month_key({}) == "—"


def test_format_month_label():
    assert format_month_label("2026-09") == "September 2026"
    assert format_month_label("—") == "Date unknown"


def test_row_pnl_bull_bear_formula():
    assert (
        row_pnl(
            {
                "direction": "BULL",
                "entry_price": 100.0,
                "exit_price": 110.0,
                "lot_size": 5,
            }
        )
        == 50.0
    )
    assert (
        row_pnl(
            {
                "direction": "BEAR",
                "entry_price": 100.0,
                "exit_price": 90.0,
                "lot_size": 10,
            }
        )
        == 100.0
    )


def test_row_pnl_prefers_recompute_over_stale_pnl():
    """Stale qty=1 pnl must not win when lot_size is known (COPPER-class bug)."""
    assert (
        row_pnl(
            {
                "direction": "BULL",
                "entry_price": 1387.55,
                "exit_price": 1391.6,
                "lot_size": 2500,
                "pnl": 4.05,
            }
        )
        == 10125.0
    )


def test_row_pnl_falls_back_to_stored_when_lot_missing():
    assert (
        row_pnl(
            {
                "direction": "BULL",
                "entry_price": 100,
                "exit_price": 110,
                "pnl": 42.5,
            }
        )
        == 42.5
    )


def test_sum_pnl_and_overall_live_only():
    rows = [
        {"trade_mode": "LIVE", "pnl": 100.5, "exit_at": "2026-09-01 12:00:00"},
        {"trade_mode": "LIVE", "pnl": -20.25, "exit_at": "2026-09-10 12:00:00"},
        {"trade_mode": "PAPER", "pnl": 999, "exit_at": "2026-09-05 12:00:00"},
        {"trade_mode": "LIVE", "pnl": 10, "exit_at": "2026-08-20 12:00:00"},
    ]
    assert sum_pnl(rows) == 1089.25
    assert overall_live_pnl(rows) == 90.25


def test_group_rows_by_month_newest_first_with_totals():
    rows = [
        {"exit_at": "2026-08-20 12:00:00", "pnl": 10},
        {"exit_at": "2026-09-01 12:00:00", "pnl": 100},
        {"exit_at": "2026-09-10 12:00:00", "pnl": -20},
    ]
    groups = group_rows_by_month(rows)
    assert [g["key"] for g in groups] == ["2026-09", "2026-08"]
    assert groups[0]["label"] == "September 2026"
    assert groups[0]["pnl"] == 80.0
    assert groups[1]["pnl"] == 10.0
    assert len(groups[0]["rows"]) == 2


def test_prod_september_2026_live_history_overall():
    """Fixture matching paperclip LIVE History rows from 2026-09-17 screenshot."""
    live = [
        {
            "trade_mode": "LIVE",
            "direction": "BULL",
            "entry_price": 239650.0,
            "exit_price": 240729.0,
            "lot_size": 5,
            "pnl": 999999,  # stale must be ignored
            "exit_at": "2026-09-17 19:39:34",
        },
        {
            "trade_mode": "LIVE",
            "direction": "BULL",
            "entry_price": 1387.55,
            "exit_price": 1391.6,
            "lot_size": 2500,
            "pnl": 4.05,  # pre-repair qty=1 value
            "exit_at": "2026-09-17 18:58:13",
        },
        {
            "trade_mode": "LIVE",
            "direction": "BULL",
            "entry_price": 9542.0,
            "exit_price": 9547.0,
            "lot_size": 100,
            "exit_at": "2026-09-16 13:29:18",
        },
        {
            "trade_mode": "LIVE",
            "direction": "BULL",
            "entry_price": 233200.0,
            "exit_price": 235363.0,
            "lot_size": 5,
            "exit_at": "2026-09-15 18:25:18",
        },
        {
            "trade_mode": "LIVE",
            "direction": "BULL",
            "entry_price": 1361.5,
            "exit_price": 1364.6,
            "lot_size": 2500,
            "exit_at": "2026-09-15 18:20:44",
        },
        {
            "trade_mode": "PAPER",
            "direction": "BEAR",
            "entry_price": 292.0,
            "exit_price": 291.8,
            "lot_size": 1250,
            "exit_at": "2026-09-17 14:49:17",
        },
    ]
    assert [row_pnl(r) for r in live[:5]] == [5395.0, 10125.0, 500.0, 10815.0, 7750.0]
    assert overall_live_pnl(live) == 34585.0
    groups = group_rows_by_month([r for r in live if r["trade_mode"] == "LIVE"])
    assert len(groups) == 1
    assert groups[0]["pnl"] == 34585.0

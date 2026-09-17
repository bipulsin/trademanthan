"""Unit tests for CommDiv History month grouping / Overall LIVE PnL helpers."""
from backend.services.commodities_div.history_groups import (
    exec_month_key,
    format_month_label,
    group_rows_by_month,
    overall_live_pnl,
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

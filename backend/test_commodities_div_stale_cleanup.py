"""Tests for CommDiv stale Divergence cleanup (3 MCX session trading days)."""
from __future__ import annotations

from datetime import date, datetime
from unittest.mock import MagicMock, patch

import pytz

from backend.services.commodities_div.stale_cleanup import (
    STALE_DIV_REMARK,
    count_trading_days_after,
    is_mcx_session_trading_day,
    is_stale_divergence,
    reject_stale_divergences,
    trading_days_since_div,
)

IST = pytz.timezone("Asia/Kolkata")


def test_mcx_session_trading_day_skips_weekend_and_nse_holiday():
    holidays = {date(2026, 10, 2)}  # Fri holiday example
    assert is_mcx_session_trading_day(date(2026, 10, 1), holidays=holidays)  # Thu
    assert not is_mcx_session_trading_day(date(2026, 10, 2), holidays=holidays)
    assert not is_mcx_session_trading_day(date(2026, 10, 3), holidays=holidays)  # Sat
    assert not is_mcx_session_trading_day(date(2026, 10, 4), holidays=holidays)  # Sun
    assert is_mcx_session_trading_day(date(2026, 10, 5), holidays=holidays)  # Mon


def test_count_trading_days_after_exclusive_start():
    holidays: set[date] = set()
    # Thu Oct 1 → through Tue Oct 6: Fri, Mon, Tue = 3
    assert count_trading_days_after(date(2026, 10, 1), date(2026, 10, 6), holidays=holidays) == 3
    # Younger than 3 trading days (Fri only through Sat)
    assert count_trading_days_after(date(2026, 10, 1), date(2026, 10, 3), holidays=holidays) == 1
    # Mon Oct 5: Fri + Mon = 2
    assert count_trading_days_after(date(2026, 10, 1), date(2026, 10, 5), holidays=holidays) == 2
    assert count_trading_days_after(date(2026, 10, 1), date(2026, 10, 1), holidays=holidays) == 0


def test_div_younger_than_3_trading_days_not_stale():
    holidays: set[date] = set()
    div_at = datetime(2026, 10, 1, 20, 0, 21)  # Thu IST naive
    now = IST.localize(datetime(2026, 10, 5, 12, 0, 0))  # Mon — 2 trading days after
    assert trading_days_since_div(div_at, now, holidays=holidays) == 2
    assert not is_stale_divergence(
        status="Divergence",
        go_received_at=None,
        div_received_at=div_at,
        now=now,
        holidays=holidays,
    )


def test_div_after_3_trading_days_without_go_is_stale():
    holidays: set[date] = set()
    div_at = datetime(2026, 10, 1, 20, 0, 21)
    now = IST.localize(datetime(2026, 10, 6, 8, 15, 0))  # Tue — 3 trading days after
    assert trading_days_since_div(div_at, now, holidays=holidays) == 3
    assert is_stale_divergence(
        status="Divergence",
        go_received_at=None,
        div_received_at=div_at,
        now=now,
        holidays=holidays,
    )


def test_go_present_never_stale_by_this_rule():
    holidays: set[date] = set()
    div_at = datetime(2026, 10, 1, 20, 0, 21)
    now = IST.localize(datetime(2026, 10, 7, 9, 0, 0))
    assert not is_stale_divergence(
        status="Divergence",
        go_received_at=datetime(2026, 10, 2, 10, 0, 0),
        div_received_at=div_at,
        now=now,
        holidays=holidays,
    )
    assert not is_stale_divergence(
        status="Activated",
        go_received_at=None,
        div_received_at=div_at,
        now=now,
        holidays=holidays,
    )
    assert not is_stale_divergence(
        status="In-Trade",
        go_received_at=None,
        div_received_at=div_at,
        now=now,
        holidays=holidays,
    )


def test_reject_stale_divergences_updates_only_stale_rows():
    holidays: set[date] = set()
    now = IST.localize(datetime(2026, 10, 7, 8, 15, 0))
    young = {
        "id": 10,
        "symbol_raw": "CRUDEOILX2026",
        "symbol_mapped": "CRUDEOIL",
        "direction": "BULL",
        "status": "Divergence",
        "div_received_at": datetime(2026, 10, 6, 11, 0, 0),
        "go_received_at": None,
        "contract": "CRUDEOIL FUT",
    }
    stale = {
        "id": 34,
        "symbol_raw": "NATURALGASV2026",
        "symbol_mapped": "NATURALGAS",
        "direction": "BULL",
        "status": "Divergence",
        "div_received_at": datetime(2026, 10, 1, 20, 0, 21),
        "go_received_at": None,
        "contract": "NATURALGAS FUT 27 OCT 26",
    }

    db = MagicMock()
    select_result = MagicMock()
    select_result.mappings.return_value.all.return_value = [young, stale]
    db.execute.return_value = select_result

    with patch(
        "backend.services.commodities_div.stale_cleanup.ensure_commodities_div_tables"
    ), patch(
        "backend.services.commodities_div.stale_cleanup.SessionLocal", return_value=db
    ), patch(
        "backend.services.commodities_div.stale_cleanup._holiday_dates",
        return_value=holidays,
    ):
        out = reject_stale_divergences(now=now, holidays=holidays)

    assert out["ok"] is True
    assert out["rejected_count"] == 1
    assert out["rejected"][0]["id"] == 34
    assert out["rejected"][0]["status_remark"] == STALE_DIV_REMARK
    # SELECT + UPDATE signal + clear webhook_log
    assert db.execute.call_count == 3
    update_sql = str(db.execute.call_args_list[1][0][0])
    assert "Rejected" in update_sql or db.execute.call_args_list[1][0][1]["status"] == "Rejected"
    assert db.execute.call_args_list[1][0][1]["id"] == 34
    assert db.execute.call_args_list[1][0][1]["remark"] == STALE_DIV_REMARK
    db.commit.assert_called_once()

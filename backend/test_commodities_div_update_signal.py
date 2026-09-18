"""Unit tests for CommDiv update_signal (In-Trade edit without exit / qty)."""
from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from backend.services.commodities_div.actions import update_signal


def _update_db(*, signal_id: int = 7, **row_overrides):
    """Mock SessionLocal: SELECT FOR UPDATE, UPDATE, SELECT *."""
    db = MagicMock()
    base = {
        "id": signal_id,
        "symbol_raw": "GOLDPETAL",
        "symbol_mapped": "GOLDPETAL",
        "direction": "BULL",
        "status": "In-Trade",
        "entry_price": 15396.0,
        "exit_price": None,
        "trade_taken_at": datetime(2026, 9, 18, 10, 33, 46),
        "exit_at": None,
        "exit_submitted_at": None,
        "trade_mode": "PAPER",
        "trade_log_id": None,
        "lot_size": 1,
        "contract": "GOLDPETAL26SEPFUT",
        "instrument_key": "MCX_FO|999",
        "div_received_at": datetime(2026, 9, 18, 10, 0, 0),
        "ltp": 15441.0,
        "ltp_updated_at": datetime(2026, 9, 18, 18, 36, 43),
        "created_at": None,
        "updated_at": None,
        "go_received_at": None,
        "go_tv_time_ist": None,
        "div_tv_time_ist": None,
        "exit_signal_received_at": None,
        "exit_tv_time_ist": None,
    }
    base.update(row_overrides)
    after = dict(base)

    lock_result = MagicMock()
    lock_result.mappings.return_value.first.return_value = base
    update_result = MagicMock()
    select_result = MagicMock()

    def execute_side_effect(stmt, params=None):
        call_n = db.execute.call_count
        if call_n == 1:
            return lock_result
        if call_n == 2:
            if params:
                if "ep" in params:
                    after["entry_price"] = params["ep"]
                if "tta" in params:
                    after["trade_taken_at"] = params["tta"]
                if "dir" in params:
                    after["direction"] = params["dir"]
                if "mode" in params:
                    after["trade_mode"] = params["mode"]
                if "lot" in params:
                    after["lot_size"] = params["lot"]
                if "xp" in params:
                    after["exit_price"] = params["xp"]
                if "xa" in params:
                    after["exit_at"] = params["xa"]
                sql = str(stmt)
                if "exit_price = NULL" in sql:
                    after["exit_price"] = None
                    after["exit_at"] = None
            return update_result
        select_result.mappings.return_value.first.return_value = after
        return select_result

    db.execute.side_effect = execute_side_effect
    return db


@patch("backend.services.commodities_div.actions.ensure_commodities_div_tables")
@patch("backend.services.commodities_div.actions.SessionLocal")
def test_update_in_trade_without_exit_stays_in_trade(mock_session, mock_ensure):
    db = _update_db(
        status="In-Trade",
        lot_size=1,
        exit_price=15441.0,
        exit_at=datetime(2026, 9, 18, 12, 0, 0),
    )
    mock_session.return_value = db

    out = update_signal(
        signal_id=7,
        entry_price=15396.0,
        trade_taken_at="2026-09-18 10:33:46",
        direction="BULL",
        trade_mode="PAPER",
        exit_price=None,
        exit_at=None,
        qty=10,
    )

    assert out["status"] == "In-Trade"
    assert out["lot_size"] == 10
    assert out["lot_qty"] == 10
    assert out["exit_price"] is None
    assert out["exit_at"] is None
    # PnL from LTP × new qty: (15441 - 15396) * 10 = 450
    assert out["pnl"] == 450.0
    update_sql = str(db.execute.call_args_list[1][0][0])
    assert "exit_price = NULL" in update_sql
    assert "lot_size = :lot" in update_sql
    assert db.execute.call_args_list[1][0][1]["lot"] == 10


@patch("backend.services.commodities_div.actions.ensure_commodities_div_tables")
@patch("backend.services.commodities_div.actions.SessionLocal")
def test_update_qty_only_on_in_trade(mock_session, mock_ensure):
    db = _update_db(status="In-Trade", lot_size=1, entry_price=100.0, ltp=110.0)
    mock_session.return_value = db

    out = update_signal(signal_id=7, qty=25)

    assert out["status"] == "In-Trade"
    assert out["lot_size"] == 25
    assert out["pnl"] == 250.0  # (110-100)*25


@patch("backend.services.commodities_div.actions.ensure_commodities_div_tables")
@patch("backend.services.commodities_div.actions.SessionLocal")
def test_update_rejects_partial_exit(mock_session, mock_ensure):
    db = _update_db(status="In-Trade")
    mock_session.return_value = db

    with pytest.raises(ValueError, match="exit_price and exit_at"):
        update_signal(signal_id=7, exit_price=15400.0, exit_at=None)

    db2 = _update_db(status="In-Trade")
    mock_session.return_value = db2
    with pytest.raises(ValueError, match="exit_price and exit_at"):
        update_signal(signal_id=7, exit_price=None, exit_at="2026-09-18 15:00:00")


@patch("backend.services.commodities_div.actions.ensure_commodities_div_tables")
@patch("backend.services.commodities_div.actions.SessionLocal")
def test_update_history_with_exit_and_qty(mock_session, mock_ensure):
    db = _update_db(
        status="History",
        entry_price=100.0,
        exit_price=105.0,
        exit_at=datetime(2026, 9, 17, 14, 0, 0),
        lot_size=10,
        ltp=None,
    )
    mock_session.return_value = db

    out = update_signal(
        signal_id=7,
        entry_price=100.0,
        trade_taken_at="2026-09-17 10:00:00",
        direction="BULL",
        exit_price=110.0,
        exit_at="2026-09-17 15:00:00",
        qty=20,
        trade_mode="PAPER",
    )

    assert out["status"] == "History"
    assert out["lot_size"] == 20
    assert out["exit_price"] == 110.0
    assert out["pnl"] == 200.0  # (110-100)*20
    update_params = db.execute.call_args_list[1][0][1]
    assert update_params["xp"] == 110.0
    assert update_params["lot"] == 20
    update_sql = str(db.execute.call_args_list[1][0][0])
    assert "exit_price = NULL" not in update_sql


@patch("backend.services.commodities_div.actions.ensure_commodities_div_tables")
@patch("backend.services.commodities_div.actions.SessionLocal")
def test_update_rejects_bad_qty(mock_session, mock_ensure):
    db = _update_db(status="In-Trade")
    mock_session.return_value = db

    with pytest.raises(ValueError, match="qty"):
        update_signal(signal_id=7, qty=0)

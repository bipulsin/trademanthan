"""Unit tests for CommDiv manual History entry (no live DB / Upstox)."""
from __future__ import annotations

import json
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from backend.services.commodities_div.actions import create_manual_history


def _scalar_then_row(signal_id: int = 42, **row_overrides):
    """Mock SessionLocal: INSERT RETURNING id, then SELECT * row."""
    db = MagicMock()
    insert_result = MagicMock()
    insert_result.scalar.return_value = signal_id
    select_result = MagicMock()
    base = {
        "id": signal_id,
        "symbol_raw": "NATURALGAS",
        "symbol_mapped": "NATURALGAS",
        "direction": "BULL",
        "status": "History",
        "entry_price": 250.0,
        "exit_price": 255.0,
        "trade_taken_at": datetime(2026, 9, 17, 10, 0, 0),
        "exit_at": datetime(2026, 9, 17, 14, 30, 0),
        "exit_submitted_at": datetime(2026, 9, 17, 14, 31, 0),
        "trade_mode": "PAPER",
        "trade_log_id": None,
        "lot_size": 1250,
        "contract": "NATURALGAS25OCTFUT",
        "instrument_key": "MCX_FO|123",
        "div_received_at": datetime(2026, 9, 17, 10, 0, 0),
        "ltp": None,
        "ltp_updated_at": None,
        "created_at": None,
        "updated_at": None,
        "go_received_at": None,
        "go_tv_time_ist": None,
        "div_tv_time_ist": None,
        "exit_signal_received_at": None,
        "exit_tv_time_ist": None,
    }
    base.update(row_overrides)
    select_result.mappings.return_value.first.return_value = base
    db.execute.side_effect = [insert_result, select_result]
    return db


@patch("backend.services.commodities_div.actions.ensure_commodities_div_tables")
@patch("backend.services.commodities_div.actions.attach_instrument_fields")
@patch("backend.services.commodities_div.actions.SessionLocal")
@patch("backend.services.commodities_div.actions.upsert_trade")
@patch("backend.services.commodities_div.actions.ensure_trade_log_table")
def test_manual_paper_skips_trade_log(
    mock_ensure_tl, mock_upsert, mock_session, mock_attach, mock_ensure
):
    mock_attach.return_value = {
        "symbol_mapped": "NATURALGAS",
        "underlying_matched": True,
        "mapping_found": True,
        "instrument_key": "MCX_FO|123",
        "contract": "NATURALGAS25OCTFUT",
        "trading_symbol": "NATURALGAS25OCTFUT",
        "lot_size": 1250,
    }
    db = _scalar_then_row(trade_mode="PAPER", trade_log_id=None)
    mock_session.return_value = db

    out = create_manual_history(
        commodity="NATURALGAS",
        direction="BULL",
        entry_price=250.0,
        trade_taken_at="2026-09-17 10:00:00",
        exit_price=255.0,
        exit_at="2026-09-17 14:30:00",
        trade_mode="PAPER",
    )

    assert out["status"] == "History"
    assert out["trade_mode"] == "PAPER"
    assert out["trade_log_id"] is None
    mock_upsert.assert_not_called()
    mock_ensure_tl.assert_not_called()
    db.commit.assert_called_once()


@patch("backend.services.commodities_div.actions.ensure_commodities_div_tables")
@patch("backend.services.commodities_div.actions.attach_instrument_fields")
@patch("backend.services.commodities_div.actions.SessionLocal")
@patch("backend.services.commodities_div.actions.upsert_trade", return_value=99)
@patch("backend.services.commodities_div.actions.ensure_trade_log_table")
def test_manual_live_upserts_trade_log(
    mock_ensure_tl, mock_upsert, mock_session, mock_attach, mock_ensure
):
    mock_attach.return_value = {
        "symbol_mapped": "CRUDEOIL",
        "underlying_matched": True,
        "mapping_found": True,
        "instrument_key": "MCX_FO|999",
        "contract": "CRUDEOIL25SEPFUT",
        "trading_symbol": "CRUDEOIL25SEPFUT",
        "lot_size": 100,
    }
    db = MagicMock()
    insert_result = MagicMock()
    insert_result.scalar.return_value = 7
    update_result = MagicMock()
    select_result = MagicMock()
    select_result.mappings.return_value.first.return_value = {
        "id": 7,
        "symbol_raw": "CRUDEOIL",
        "symbol_mapped": "CRUDEOIL",
        "direction": "BEAR",
        "status": "History",
        "entry_price": 6200.0,
        "exit_price": 6150.0,
        "trade_taken_at": datetime(2026, 9, 17, 11, 0, 0),
        "exit_at": datetime(2026, 9, 17, 15, 0, 0),
        "exit_submitted_at": datetime(2026, 9, 17, 15, 1, 0),
        "trade_mode": "LIVE",
        "trade_log_id": 99,
        "lot_size": 100,
        "contract": "CRUDEOIL25SEPFUT",
        "instrument_key": "MCX_FO|999",
        "div_received_at": datetime(2026, 9, 17, 11, 0, 0),
        "ltp": None,
        "ltp_updated_at": None,
        "created_at": None,
        "updated_at": None,
        "go_received_at": None,
        "go_tv_time_ist": None,
        "div_tv_time_ist": None,
        "exit_signal_received_at": None,
        "exit_tv_time_ist": None,
    }
    db.execute.side_effect = [insert_result, update_result, select_result]
    mock_session.return_value = db

    out = create_manual_history(
        commodity="CRUDEOIL",
        direction="BEAR",
        entry_price=6200.0,
        trade_taken_at="2026-09-17 11:00:00",
        exit_price=6150.0,
        exit_at="2026-09-17 15:00:00",
        trade_mode="LIVE",
    )

    assert out["trade_log_id"] == 99
    assert out["trade_mode"] == "LIVE"
    mock_ensure_tl.assert_called_once()
    mock_upsert.assert_called_once()
    payload = mock_upsert.call_args[0][1]
    assert payload["source"] == "commodities_div"
    assert payload["direction"] == "SHORT"
    assert payload["qty"] == 100
    assert payload["exit_trigger"] == "commodities_div_manual_entry"
    notes = json.loads(payload["notes"])
    assert notes["trade_mode"] == "LIVE"
    assert notes["commodities_div_manual"] is True
    assert notes["commodities_div_signal_id"] == 7


@patch("backend.services.commodities_div.actions.ensure_commodities_div_tables")
@patch("backend.services.commodities_div.actions.attach_instrument_fields")
@patch("backend.services.commodities_div.actions.SessionLocal")
@patch("backend.services.commodities_div.actions.upsert_trade")
def test_manual_unresolvable_symbol_still_saves(
    mock_upsert, mock_session, mock_attach, mock_ensure
):
    mock_attach.return_value = {
        "symbol_mapped": "MYCUSTOM",
        "underlying_matched": False,
        "mapping_found": False,
        "instrument_key": None,
        "contract": None,
        "trading_symbol": None,
        "lot_size": None,
    }
    db = _scalar_then_row(
        signal_id=3,
        symbol_raw="MyCustom Commodity",
        symbol_mapped="MYCUSTOM",
        instrument_key=None,
        contract=None,
        lot_size=None,
        trade_mode="PAPER",
    )
    mock_session.return_value = db

    out = create_manual_history(
        commodity="MyCustom Commodity",
        direction="BULL",
        entry_price=10.0,
        trade_taken_at="2026-09-17 09:30:00",
        exit_price=11.0,
        exit_at="2026-09-17 10:00:00",
        trade_mode="PAPER",
    )

    assert out["status"] == "History"
    assert out["mapping_resolved"] is False
    mock_upsert.assert_not_called()
    insert_params = db.execute.call_args_list[0][0][1]
    assert insert_params["symbol_raw"] == "MyCustom Commodity"
    assert insert_params["symbol_mapped"]
    assert insert_params["instrument_key"] is None


def test_manual_rejects_blank_commodity():
    with pytest.raises(ValueError, match="commodity"):
        create_manual_history(
            commodity="  ",
            direction="BULL",
            entry_price=1.0,
            trade_taken_at="2026-09-17 10:00:00",
            exit_price=2.0,
            exit_at="2026-09-17 11:00:00",
        )


def test_manual_rejects_bad_direction():
    with pytest.raises(ValueError, match="direction"):
        create_manual_history(
            commodity="CRUDEOIL",
            direction="LONG",
            entry_price=1.0,
            trade_taken_at="2026-09-17 10:00:00",
            exit_price=2.0,
            exit_at="2026-09-17 11:00:00",
        )

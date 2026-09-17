"""Tests for CommDiv trade_log free-text repair helpers."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

from backend.services.commodities_div.actions import _ensure_resolved_instrument
from backend.services.commodities_div.trade_log_repair import (
    needs_commdiv_trade_log_repair,
    repair_unresolved_commdiv_live_trade_logs,
    resolve_instrument_for_repair,
)


def test_needs_repair_detects_coppersep_fut():
    tl = {
        "source": "commodities_div",
        "symbol": "COPPERSEPFUT",
        "contract": None,
        "qty": 1,
        "notes": '{"commodities_div_manual": true, "mapping_found": false}',
    }
    assert needs_commdiv_trade_log_repair(tl) is True


def test_needs_repair_skips_kavach_and_good_commdiv():
    assert needs_commdiv_trade_log_repair(
        {"source": "tradelog_ui", "symbol": "COPPERSEPFUT", "qty": 1, "contract": None}
    ) is False
    assert needs_commdiv_trade_log_repair(
        {
            "source": "commodities_div",
            "symbol": "SILVERMINI",
            "contract": "SILVERM FUT 30 NOV 26",
            "qty": 5,
            "notes": '{"mapping_found": true}',
        }
    ) is False


@patch("backend.services.commodities_div.trade_log_repair.attach_instrument_fields")
def test_resolve_instrument_for_repair(mock_attach):
    mock_attach.return_value = {
        "symbol_mapped": "COPPER",
        "contract": "COPPER FUT 30 SEP 26",
        "trading_symbol": "COPPER FUT 30 SEP 26",
        "lot_size": 2500,
        "instrument_key": "MCX_FO|571298",
        "underlying_matched": True,
        "mapping_found": True,
        "parse_mode": "free_text",
    }
    out = resolve_instrument_for_repair(symbol_raw="COPPER SEP FUT", symbol="COPPERSEPFUT")
    assert out["symbol_mapped"] == "COPPER"
    assert out["lot_size"] == 2500
    assert out["contract"] == "COPPER FUT 30 SEP 26"


@patch("backend.services.commodities_div.actions.attach_instrument_fields")
def test_ensure_resolved_instrument_fills_missing(mock_attach):
    mock_attach.return_value = {
        "symbol_mapped": "COPPER",
        "contract": "COPPER FUT 30 SEP 26",
        "trading_symbol": "COPPER FUT 30 SEP 26",
        "lot_size": 2500,
        "instrument_key": "MCX_FO|571298",
        "underlying_matched": True,
        "mapping_found": True,
    }
    out = _ensure_resolved_instrument(
        {
            "symbol_raw": "COPPER SEP FUT",
            "symbol_mapped": "COPPERSEPFUT",
            "contract": None,
            "lot_size": None,
            "instrument_key": None,
        }
    )
    assert out["symbol_mapped"] == "COPPER"
    assert out["lot_size"] == 2500
    assert out["contract"] == "COPPER FUT 30 SEP 26"


@patch("backend.services.commodities_div.actions.attach_instrument_fields")
def test_ensure_resolved_instrument_noop_when_complete(mock_attach):
    row = {
        "symbol_raw": "COPPERU2026",
        "symbol_mapped": "COPPER",
        "contract": "COPPER FUT 30 SEP 26",
        "lot_size": 2500,
        "instrument_key": "MCX_FO|571298",
    }
    out = _ensure_resolved_instrument(row)
    mock_attach.assert_not_called()
    assert out["lot_size"] == 2500


@patch("backend.services.commodities_div.trade_log_repair.ensure_trade_log_table")
@patch("backend.services.commodities_div.trade_log_repair.SessionLocal")
@patch("backend.services.commodities_div.trade_log_repair.attach_instrument_fields")
def test_repair_updates_trade_log_and_signal(mock_attach, mock_session, _ensure):
    mock_attach.return_value = {
        "symbol_mapped": "COPPER",
        "contract": "COPPER FUT 30 SEP 26",
        "trading_symbol": "COPPER FUT 30 SEP 26",
        "lot_size": 2500,
        "instrument_key": "MCX_FO|571298",
        "underlying_matched": True,
        "mapping_found": True,
        "parse_mode": "free_text",
        "match_mode": "exact_month",
    }
    db = MagicMock()
    select_result = MagicMock()
    select_result.mappings.return_value.all.return_value = [
        {
            "tl_id": 111,
            "session_date": "2026-09-17",
            "tl_symbol": "COPPERSEPFUT",
            "tl_contract": None,
            "tl_qty": 1,
            "direction": "LONG",
            "entry_time": "17:43:18",
            "entry_price": 1387.55,
            "exit_price": 1391.6,
            "tl_notes": '{"commodities_div_manual": true, "symbol_raw": "COPPER SEP FUT", "mapping_found": false, "trade_mode": "LIVE"}',
            "source": "commodities_div",
            "signal_id": 18,
            "symbol_raw": "COPPER SEP FUT",
            "symbol_mapped": "COPPERSEPFUT",
            "signal_contract": None,
            "signal_lot_size": None,
            "signal_instrument_key": None,
            "trade_mode": "LIVE",
        }
    ]
    # SELECT then trade_log UPDATE + signal UPDATE
    db.execute.side_effect = [select_result, MagicMock(), MagicMock()]
    mock_session.return_value = db

    changes = repair_unresolved_commdiv_live_trade_logs(dry_run=False)
    assert len(changes) == 1
    assert changes[0]["status"] == "updated"
    assert changes[0]["new_symbol"] == "COPPER"
    assert changes[0]["new_qty"] == 2500
    db.commit.assert_called_once()
    # SELECT + trade_log UPDATE + signal UPDATE
    assert db.execute.call_count == 3
    tl_params = db.execute.call_args_list[1][0][1]
    assert tl_params["symbol"] == "COPPER"
    assert tl_params["qty"] == 2500
    assert tl_params["contract"] == "COPPER FUT 30 SEP 26"

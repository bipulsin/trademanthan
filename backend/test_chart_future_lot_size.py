"""Futures lot size for the shared security chart comes from the instrument master."""
from __future__ import annotations

import json

from backend.services import chart_instrument_resolver as resolver


def _write(path, rows):
    path.write_text(json.dumps(rows), encoding="utf-8")


def _reset_index():
    resolver._lot_index["built"] = False
    resolver._lot_index["by_key"] = {}
    resolver._lot_index["by_symbol"] = {}
    resolver._lot_index["nse_mtime"] = None
    resolver._lot_index["complete_mtime"] = None


def test_lookup_future_lot_by_key_and_trading_symbol(tmp_path, monkeypatch):
    complete = tmp_path / "complete.json"
    nse = tmp_path / "nse_instruments.json"
    _write(
        complete,
        [
            {
                "instrument_key": "NSE_FO|48704",
                "trading_symbol": "NIFTY FUT 27 OCT 26",
                "instrument_type": "FUT",
                "lot_size": 65,
            },
            {
                "instrument_key": "MCX_FO|579304",
                "trading_symbol": "SILVER FUT 05 JUL 27",
                "instrument_type": "FUT",
                "lot_size": 30,
            },
            {
                "instrument_key": "NSE_EQ|NIFTY",
                "trading_symbol": "NIFTY",
                "instrument_type": "EQ",
                "lot_size": 1,
            },
            {
                "instrument_key": "MCX_FO|0",
                "trading_symbol": "BLANK FUT",
                "instrument_type": "FUT",
                "lot_size": 0,
            },
        ],
    )
    _write(
        nse,
        [
            {
                "instrument_key": "NSE_FO:48987",
                "trading_symbol": "RELIANCE FUT 27 OCT 26",
                "instrument_type": "FUT",
                "lot_size": 500,
            }
        ],
    )
    monkeypatch.setattr(resolver, "_nse_instruments_path", lambda: nse)
    monkeypatch.setattr(resolver, "_complete_master_path", lambda: complete)
    _reset_index()

    assert resolver.lookup_future_lot_size("NSE_FO|48704") == 65
    assert resolver.lookup_future_lot_size("NSE_FO:48704") == 65
    assert resolver.lookup_future_lot_size(trading_symbol="SILVER FUT 05 JUL 27") == 30
    assert resolver.lookup_future_lot_size("NSE_FO|48987") == 500
    assert resolver.lookup_future_lot_size("NSE_EQ|NIFTY") is None
    assert resolver.lookup_future_lot_size(trading_symbol="NIFTY") is None
    assert resolver.lookup_future_lot_size(trading_symbol="BLANK FUT") is None
    assert resolver.lookup_future_lot_size("MISSING") is None


def test_ambiguous_trading_symbol_is_not_guessed(tmp_path, monkeypatch):
    complete = tmp_path / "complete.json"
    nse = tmp_path / "empty.json"
    _write(
        complete,
        [
            {
                "instrument_key": "NSE_FO|1",
                "trading_symbol": "SAME FUT",
                "instrument_type": "FUT",
                "lot_size": 65,
            },
            {
                "instrument_key": "NSE_FO|2",
                "trading_symbol": "SAME FUT",
                "instrument_type": "FUT",
                "lot_size": 30,
            },
        ],
    )
    _write(nse, [])
    monkeypatch.setattr(resolver, "_nse_instruments_path", lambda: nse)
    monkeypatch.setattr(resolver, "_complete_master_path", lambda: complete)
    _reset_index()

    assert resolver.lookup_future_lot_size(trading_symbol="SAME FUT") is None
    assert resolver.lookup_future_lot_size("NSE_FO|1") == 65


def test_resolve_attaches_lot_only_for_futures(tmp_path, monkeypatch):
    complete = tmp_path / "complete.json"
    nse = tmp_path / "empty.json"
    _write(
        complete,
        [
            {
                "instrument_key": "NSE_FO|48704",
                "trading_symbol": "NIFTY FUT 27 OCT 26",
                "instrument_type": "FUT",
                "lot_size": 65,
            }
        ],
    )
    _write(nse, [])
    monkeypatch.setattr(resolver, "_nse_instruments_path", lambda: nse)
    monkeypatch.setattr(resolver, "_complete_master_path", lambda: complete)
    _reset_index()

    fut = resolver.resolve_chart_instrument(
        "NIFTY",
        "FUTURES",
        instrument_key="NSE_FO:48704",
    )
    equity = resolver.resolve_chart_instrument(
        "TCS",
        "EQUITY",
        instrument_key="NSE_EQ|INE467B01029",
    )
    assert fut["lot_size"] == 65
    assert fut["instrument_type"] == "FUT"
    assert "lot_size" not in equity

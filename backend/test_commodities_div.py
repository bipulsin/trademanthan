"""Unit tests for Commodities Div parse / normalize / flag rules (no live Upstox)."""
from __future__ import annotations

import json
from datetime import datetime
from unittest.mock import MagicMock, patch

from backend.services.commodities_div.mapping import (
    attach_instrument_fields,
    display_symbol_for,
    normalize_tv_ticker,
    parse_underlying,
)
from backend.services.commodities_div.webhook import (
    accept_webhook,
    decode_raw_payload,
    parse_flag_fields,
    _symbols_match,
)


def test_symbols_match_same_underlying_variants():
    active = {"symbol_raw": "CRUDEOIL1!", "symbol_mapped": "CRUDEOIL"}
    assert _symbols_match(active, "CRUDEOILZ2026", "CRUDEOIL")
    assert _symbols_match(active, "MCX:CRUDEOIL1!", "CRUDEOIL")
    assert not _symbols_match(active, "NATURALGAS1!", "NATURALGAS")
    assert not _symbols_match(active, "BTCUSD", "BTCUSD")
    assert not _symbols_match(active, "ETHUSDU2026", "ETHUSD")
    silver = {"symbol_raw": "SILVERMX2026", "symbol_mapped": "SILVERMINI"}
    assert _symbols_match(silver, "SILVERM", "SILVERMINI")
    assert _symbols_match(silver, "SILVERMINI1!", "SILVERMINI")
    assert not _symbols_match(silver, "COPPERU2026", "COPPER")


def test_display_symbol_for_prefers_upstox_contract():
    assert (
        display_symbol_for(
            contract="NATURALGAS 26 APR FUT",
            trading_symbol="NATURALGAS 26 APR FUT",
            symbol_mapped="NATURALGAS",
            symbol_raw="NATURALGASV2026",
        )
        == "NATURALGAS 26 APR FUT"
    )
    assert (
        display_symbol_for(
            contract="",
            trading_symbol=None,
            symbol_mapped="CRUDEOIL",
            symbol_raw="CRUDEOILV2026",
        )
        == "CRUDEOIL"
    )
    assert (
        display_symbol_for(
            contract=None,
            symbol_mapped="",
            symbol_raw="CRUDEOILV2026",
        )
        == "CRUDEOILV2026"
    )
    assert display_symbol_for() == ""


def test_attach_instrument_fields_sets_display_without_resolve():
    inst = attach_instrument_fields("NATURALGASV2026", resolve_contract=False)
    assert inst["display_symbol"] == "NATURALGAS"
    assert inst["trading_symbol"] is None
    assert inst["contract"] is None


def test_serialize_signal_exposes_display_and_trading_symbol():
    from backend.services.commodities_div.actions import serialize_signal

    row = serialize_signal(
        {
            "id": 1,
            "symbol_raw": "CRUDEOILV2026",
            "symbol_mapped": "CRUDEOIL",
            "direction": "BULL",
            "status": "Activated",
            "contract": "CRUDEOIL 19 MAY FUT",
            "instrument_key": "MCX_FO|123",
            "lot_size": 100,
            "trade_mode": "PAPER",
            "entry_price": None,
            "ltp": None,
            "exit_price": None,
        }
    )
    assert row["trading_symbol"] == "CRUDEOIL 19 MAY FUT"
    assert row["display_symbol"] == "CRUDEOIL 19 MAY FUT"
    assert row["mcx_symbol"] == "CRUDEOIL 19 MAY FUT"


def test_normalize_tv_ticker_strips_exchange_and_continuous():
    assert normalize_tv_ticker("MCX:CRUDEOIL1!") == "CRUDEOIL"
    assert normalize_tv_ticker("CRUDEOIL1!") == "CRUDEOIL"
    assert normalize_tv_ticker("NATURALGAS") == "NATURALGAS"
    assert normalize_tv_ticker("GOLDPETAL") == "GOLDPETAL"
    assert normalize_tv_ticker("CRUDEOILZ2026") == "CRUDEOIL"
    assert normalize_tv_ticker("") is None


def test_parse_underlying_allowed_forms():
    assert parse_underlying("CRUDEOIL1!") == "CRUDEOIL"
    assert parse_underlying("MCX:CRUDEOIL1!") == "CRUDEOIL"
    assert parse_underlying("CRUDEOILZ2026") == "CRUDEOIL"
    assert parse_underlying("NATURALGAS1!") == "NATURALGAS"
    assert parse_underlying("NATURALGASM2026") == "NATURALGAS"
    assert parse_underlying("COPPER") == "COPPER"
    assert parse_underlying("COPPERU2026") == "COPPER"
    assert parse_underlying("GOLDPETAL1!") == "GOLDPETAL"
    assert parse_underlying("SILVERMINI1!") == "SILVERMINI"
    assert parse_underlying("SILVERMINIZ2026") == "SILVERMINI"
    assert parse_underlying("SILVERMX2026") == "SILVERMINI"
    assert parse_underlying("SILVERM") == "SILVERMINI"
    assert parse_underlying("MCX:SILVERMX2026") == "SILVERMINI"


def test_parse_underlying_rejects_crypto_and_unknown():
    assert parse_underlying("BTCUSD") is None
    assert parse_underlying("ETHUSD") is None
    assert parse_underlying("BTCUSDT25U2026") is None
    assert parse_underlying("ETHUSDU2026") is None
    assert parse_underlying("BINANCE:ETHUSD") is None
    assert parse_underlying("GOLD1!") is None
    assert parse_underlying("NIFTY1!") is None
    assert parse_underlying("CDTEST1!") is None
    assert parse_underlying("") is None
    inst = attach_instrument_fields("ETHUSDU2026", resolve_contract=False)
    assert inst["underlying_matched"] is False


def test_attach_instrument_fields_fast_path_skips_upstox():
    with patch(
        "backend.services.commodities_div.mapping.resolve_underlying_instrument"
    ) as resolve:
        inst = attach_instrument_fields("NATURALGASV2026", resolve_contract=False)
        resolve.assert_not_called()
        assert inst["underlying_matched"] is True
        assert inst["symbol_mapped"] == "NATURALGAS"
        assert inst["instrument_key"] is None


def test_decode_text_plain_json_body():
    body = b'{"flag":"BULL-DIV","symbol":"CRUDEOIL1!","time":1757675460000}'
    parsed, raw = decode_raw_payload(body)
    assert isinstance(parsed, dict)
    assert parsed["flag"] == "BULL-DIV"
    assert raw["symbol"] == "CRUDEOIL1!"


def test_parse_flag_fields_success():
    status, fields = parse_flag_fields(
        {"flag": "BEAR-GO", "symbol": "GOLDPETAL", "time": 1757675460000}
    )
    assert status == "success"
    assert fields["direction"] == "BEAR"
    assert fields["signal_kind"] == "GO"
    assert fields["tv_time_ms"] == 1757675460000
    assert fields["tv_time_ist"] is not None


def test_parse_flag_fields_rejects_unknown_flag():
    status, fields = parse_flag_fields({"flag": "BULL-OTHER", "symbol": "X", "time": 1})
    assert status == "failed"
    assert fields is None


def test_decode_invalid_json_keeps_raw():
    parsed, raw = decode_raw_payload(b"not-json")
    assert parsed is None
    assert raw.get("_raw") == "not-json"


def test_roundtrip_payload_json_dumps():
    payload = {"flag": "BULL-EXIT", "symbol": "CRUDEOIL1!", "time": 1757675460000}
    body = json.dumps(payload).encode()
    parsed, _ = decode_raw_payload(body)
    status, fields = parse_flag_fields(parsed)
    assert status == "success"
    assert fields["signal_kind"] == "EXIT"


def test_accept_webhook_persists_without_upstox_resolve():
    body = b'{"flag":"BEAR-DIV","symbol":"NATURALGASV2026","time":1789579800000}'
    received_at = datetime(2026, 9, 16, 23, 30, 3)

    mock_db = MagicMock()
    mock_db.execute.return_value.scalar.return_value = 42

    with patch(
        "backend.services.commodities_div.webhook.ensure_commodities_div_tables"
    ), patch(
        "backend.services.commodities_div.webhook.SessionLocal", return_value=mock_db
    ), patch(
        "backend.services.commodities_div.mapping.resolve_underlying_instrument"
    ) as resolve:
        out = accept_webhook(received_at=received_at, source_ip="127.0.0.1", body=body)
        resolve.assert_not_called()

    assert out["ok"] is True
    assert out["accepted"] is True
    assert out["promote_queued"] is True
    assert out["log_id"] == 42
    assert out["flag"] == "BEAR-DIV"
    assert out["symbol_mapped"] == "NATURALGAS"
    mock_db.commit.assert_called_once()

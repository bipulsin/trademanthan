"""Unit tests for Commodities Div parse / normalize / flag rules (no live Upstox)."""
from __future__ import annotations

import json

from backend.services.commodities_div.mapping import (
    attach_instrument_fields,
    normalize_tv_ticker,
    parse_underlying,
)
from backend.services.commodities_div.webhook import (
    decode_raw_payload,
    parse_flag_fields,
)


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
    assert parse_underlying("GOLDPETAL1!") == "GOLDPETAL"
    assert parse_underlying("SILVERMINI1!") == "SILVERMINI"
    assert parse_underlying("SILVERMINIZ2026") == "SILVERMINI"


def test_parse_underlying_test_only_crypto():
    assert parse_underlying("BTCUSD") == "BTCUSD"
    assert parse_underlying("ETHUSD") == "ETHUSD"
    assert parse_underlying("BTCUSD1!") == "BTCUSD"
    assert parse_underlying("BINANCE:ETHUSD") == "ETHUSD"
    assert parse_underlying("CRYPTO:BTCUSD1!") == "BTCUSD"
    # Binance dated USDT perpetual/futures tickers from the chart
    assert parse_underlying("BTCUSDT25U2026") == "BTCUSD"
    assert parse_underlying("BINANCE:BTCUSDT25U2026") == "BTCUSD"
    assert parse_underlying("ETHUSDT") == "ETHUSD"
    inst = attach_instrument_fields("BTCUSDT25U2026")
    assert inst["underlying_matched"] is True
    assert inst["test_only"] is True
    assert inst["instrument_key"] is None
    assert inst["symbol_mapped"] == "BTCUSD"


def test_parse_underlying_rejects_unknown():
    assert parse_underlying("GOLD1!") is None
    assert parse_underlying("NIFTY1!") is None
    assert parse_underlying("CDTEST1!") is None
    assert parse_underlying("") is None


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

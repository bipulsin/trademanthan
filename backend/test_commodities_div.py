"""Unit tests for Commodities Div parse / normalize / flag rules (no live Upstox)."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from backend.services.commodities_div.mapping import (
    FUTURES_MONTH_CODES,
    attach_instrument_fields,
    display_symbol_for,
    normalize_tv_ticker,
    parse_tv_month_code,
    parse_tv_symbol,
    parse_underlying,
    resolve_mcx_instrument,
    resolve_underlying_instrument,
)
from backend.services.commodities_div.webhook import (
    accept_webhook,
    decode_raw_payload,
    parse_flag_fields,
    _symbols_match,
)


def test_futures_month_codes_standard_set():
    assert FUTURES_MONTH_CODES["U"] == 9
    assert FUTURES_MONTH_CODES["V"] == 10
    assert FUTURES_MONTH_CODES["X"] == 11
    assert FUTURES_MONTH_CODES["Z"] == 12
    assert FUTURES_MONTH_CODES["F"] == 1
    assert FUTURES_MONTH_CODES["G"] == 2
    assert FUTURES_MONTH_CODES["H"] == 3
    assert len(FUTURES_MONTH_CODES) == 12


def test_parse_tv_month_code_user_examples():
    assert parse_tv_month_code("NATURALGASV2026") == {
        "month_code": "V",
        "month": 10,
        "year": 2026,
        "contract_code": "V2026",
    }
    assert parse_tv_month_code("CRUDEOILU2026") == {
        "month_code": "U",
        "month": 9,
        "year": 2026,
        "contract_code": "U2026",
    }
    assert parse_tv_month_code("CRUDEOILX2026")["month"] == 11
    assert parse_tv_month_code("NATURALGASZ2026")["month"] == 12
    assert parse_tv_month_code("GOLDPETALF2027") == {
        "month_code": "F",
        "month": 1,
        "year": 2027,
        "contract_code": "F2027",
    }
    assert parse_tv_month_code("COPPERG2027")["month"] == 2
    assert parse_tv_month_code("SILVERMH2027")["month"] == 3
    assert parse_tv_month_code("CRUDEOIL1!") is None
    assert parse_tv_month_code("NATURALGAS") is None


def test_parse_tv_symbol_underlying_plus_month():
    p = parse_tv_symbol("NATURALGASV2026")
    assert p["underlying"] == "NATURALGAS"
    assert p["month_code"] == "V"
    assert p["month"] == 10
    assert p["year"] == 2026
    assert p["contract_code"] == "V2026"

    p2 = parse_tv_symbol("MCX:CRUDEOILU2026")
    assert p2["underlying"] == "CRUDEOIL"
    assert p2["month"] == 9
    assert p2["year"] == 2026

    p3 = parse_tv_symbol("CRUDEOIL1!")
    assert p3["underlying"] == "CRUDEOIL"
    assert p3["month"] is None
    assert p3["year"] is None


def _fake_mcx_rows():
    """Minimal Upstox-like MCX FUT rows spanning several months."""

    def row(und, key, tsym, y, m, d, lot=100):
        exp_ms = int(datetime(y, m, d, 12, 0, tzinfo=timezone.utc).timestamp() * 1000)
        return {
            "instrument_type": "FUT",
            "segment": "MCX_FO",
            "weekly": False,
            "underlying_symbol": und,
            "instrument_key": key,
            "trading_symbol": tsym,
            "lot_size": lot,
            "expiry": exp_ms,
        }

    return [
        row("CRUDEOIL", "MCX_FO|CO_SEP", "CRUDEOIL 19 SEP FUT", 2026, 9, 19),
        row("CRUDEOIL", "MCX_FO|CO_OCT", "CRUDEOIL 19 OCT FUT", 2026, 10, 19),
        row("CRUDEOIL", "MCX_FO|CO_NOV", "CRUDEOIL 19 NOV FUT", 2026, 11, 19),
        row("NATURALGAS", "MCX_FO|NG_SEP", "NATURALGAS 25 SEP FUT", 2026, 9, 25),
        row("NATURALGAS", "MCX_FO|NG_OCT", "NATURALGAS 27 OCT FUT", 2026, 10, 27),
        row("NATURALGAS", "MCX_FO|NG_NOV", "NATURALGAS 25 NOV FUT", 2026, 11, 25),
        row("NATURALGAS", "MCX_FO|NG_DEC", "NATURALGAS 28 DEC FUT", 2026, 12, 28),
        row("NATURALGAS", "MCX_FO|NG_JAN", "NATURALGAS 26 JAN FUT", 2027, 1, 26),
        row("SILVERM", "MCX_FO|SM_NOV", "SILVERM 30 NOV FUT", 2026, 11, 30, lot=1),
    ]


def test_resolve_mcx_exact_month_not_front():
    rows = _fake_mcx_rows()

    def parse_exp(v):
        return int(v) if v else None

    with patch(
        "backend.services.divtest.instruments.ensure_instrument_master",
        return_value=rows,
    ), patch(
        "backend.services.divtest.instruments._parse_expiry_ms",
        side_effect=parse_exp,
    ):
        sep = resolve_mcx_instrument("CRUDEOIL", expiry_year=2026, expiry_month=9)
        assert sep is not None
        assert sep["instrument_key"] == "MCX_FO|CO_SEP"
        assert sep["trading_symbol"] == "CRUDEOIL 19 SEP FUT"
        assert sep["match_mode"] == "exact_month"
        assert sep["expiry_fallback"] is False

        oct_ = resolve_mcx_instrument("NATURALGAS", expiry_year=2026, expiry_month=10)
        assert oct_["instrument_key"] == "MCX_FO|NG_OCT"
        assert oct_["match_mode"] == "exact_month"

        nov = resolve_mcx_instrument("NATURALGAS", expiry_year=2026, expiry_month=11)
        assert nov["instrument_key"] == "MCX_FO|NG_NOV"

        jan = resolve_mcx_instrument("NATURALGAS", expiry_year=2027, expiry_month=1)
        assert jan["instrument_key"] == "MCX_FO|NG_JAN"


def test_resolve_mcx_exact_month_fallback_warns():
    rows = _fake_mcx_rows()

    def parse_exp(v):
        return int(v) if v else None

    with patch(
        "backend.services.divtest.instruments.ensure_instrument_master",
        return_value=rows,
    ), patch(
        "backend.services.divtest.instruments._parse_expiry_ms",
        side_effect=parse_exp,
    ):
        miss = resolve_mcx_instrument(
            "CRUDEOIL", expiry_year=2028, expiry_month=3, allow_front_month_fallback=True
        )
        assert miss is not None
        assert miss["match_mode"] == "front_month_fallback"
        assert miss["expiry_fallback"] is True
        assert miss["requested_expiry_ym"] == "2028-03"
        assert miss["instrument_key"]  # some front-month, not silent None
        assert "SEP" in str(miss["trading_symbol"]) or "OCT" in str(
            miss["trading_symbol"]
        ) or "NOV" in str(miss["trading_symbol"])

        none = resolve_mcx_instrument(
            "CRUDEOIL",
            expiry_year=2028,
            expiry_month=3,
            allow_front_month_fallback=False,
        )
        assert none is None


def test_attach_instrument_fields_resolves_tv_month():
    rows = _fake_mcx_rows()

    def parse_exp(v):
        return int(v) if v else None

    with patch(
        "backend.services.divtest.instruments.ensure_instrument_master",
        return_value=rows,
    ), patch(
        "backend.services.divtest.instruments._parse_expiry_ms",
        side_effect=parse_exp,
    ):
        inst = attach_instrument_fields("NATURALGASV2026", resolve_contract=True)
        assert inst["underlying_matched"] is True
        assert inst["symbol_mapped"] == "NATURALGAS"
        assert inst["month_code"] == "V"
        assert inst["contract_month"] == 10
        assert inst["contract_year"] == 2026
        assert inst["instrument_key"] == "MCX_FO|NG_OCT"
        assert inst["contract"] == "NATURALGAS 27 OCT FUT"
        assert inst["display_symbol"] == "NATURALGAS 27 OCT FUT"
        assert inst["match_mode"] == "exact_month"
        assert inst["expiry_fallback"] is False

        crude = attach_instrument_fields("CRUDEOILU2026", resolve_contract=True)
        assert crude["instrument_key"] == "MCX_FO|CO_SEP"
        assert crude["display_symbol"] == "CRUDEOIL 19 SEP FUT"

        silver = attach_instrument_fields("SILVERMX2026", resolve_contract=True)
        assert silver["symbol_mapped"] == "SILVERMINI"
        assert silver["instrument_key"] == "MCX_FO|SM_NOV"
        assert silver["match_mode"] == "exact_month"


def test_resolve_cache_keyed_by_month_not_underlying_only():
    rows = _fake_mcx_rows()

    def parse_exp(v):
        return int(v) if v else None

    with patch(
        "backend.services.divtest.instruments.ensure_instrument_master",
        return_value=rows,
    ), patch(
        "backend.services.divtest.instruments._parse_expiry_ms",
        side_effect=parse_exp,
    ):
        a = resolve_underlying_instrument(
            "NATURALGAS", expiry_year=2026, expiry_month=10
        )
        b = resolve_underlying_instrument(
            "NATURALGAS", expiry_year=2026, expiry_month=11
        )
        assert a["instrument_key"] == "MCX_FO|NG_OCT"
        assert b["instrument_key"] == "MCX_FO|NG_NOV"
        assert a["instrument_key"] != b["instrument_key"]


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
            contract="NATURALGAS 27 OCT FUT",
            trading_symbol="NATURALGAS 27 OCT FUT",
            symbol_mapped="NATURALGAS",
            symbol_raw="NATURALGASV2026",
        )
        == "NATURALGAS 27 OCT FUT"
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
    assert inst["month_code"] == "V"
    assert inst["contract_month"] == 10


def test_serialize_signal_exposes_display_and_trading_symbol():
    from backend.services.commodities_div.actions import serialize_signal

    row = serialize_signal(
        {
            "id": 1,
            "symbol_raw": "CRUDEOILV2026",
            "symbol_mapped": "CRUDEOIL",
            "direction": "BULL",
            "status": "Activated",
            "contract": "CRUDEOIL 19 OCT FUT",
            "instrument_key": "MCX_FO|123",
            "lot_size": 100,
            "trade_mode": "PAPER",
            "entry_price": None,
            "ltp": None,
            "exit_price": None,
        }
    )
    assert row["trading_symbol"] == "CRUDEOIL 19 OCT FUT"
    assert row["display_symbol"] == "CRUDEOIL 19 OCT FUT"
    assert row["mcx_symbol"] == "CRUDEOIL 19 OCT FUT"


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

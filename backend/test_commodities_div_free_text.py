"""Unit tests for CommDiv free-text commodity parsing (month names / concat)."""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import patch

from backend.services.commodities_div.mapping import (
    attach_instrument_fields,
    infer_contract_year,
    parse_free_text_commodity,
    parse_tv_symbol,
)


AS_OF = datetime(2026, 9, 17, 12, 0, tzinfo=timezone.utc)


def test_infer_contract_year_nearest_upcoming():
    assert infer_contract_year(9, as_of=AS_OF) == 2026  # Sep current
    assert infer_contract_year(10, as_of=AS_OF) == 2026  # Oct upcoming
    assert infer_contract_year(8, as_of=AS_OF) == 2027  # Aug already past
    assert infer_contract_year(9, as_of=AS_OF, year_hint=26) == 2026
    assert infer_contract_year(9, as_of=AS_OF, year_hint=2027) == 2027


def test_parse_free_text_copper_month_name_forms():
    cases = [
        "COPPER SEP FUT",
        "COPPER SEPT FUT",
        "COPPER SEPTEMBER FUT",
        "COPPER FUT SEP",
        "SEP COPPER FUT",
        "COPPERSEPFUT",
        "copper sep fut",
    ]
    for raw in cases:
        p = parse_free_text_commodity(raw, as_of=AS_OF)
        assert p["underlying"] == "COPPER", raw
        assert p["month"] == 9, raw
        assert p["year"] == 2026, raw
        assert p["month_code"] == "U", raw
        tv = parse_tv_symbol(raw)
        assert tv["underlying"] == "COPPER", raw
        assert tv["month"] == 9, raw
        assert tv["year"] == 2026, raw


def test_parse_free_text_dated_contract():
    p = parse_free_text_commodity("COPPER 30 SEP 26", as_of=AS_OF)
    assert p["underlying"] == "COPPER"
    assert p["month"] == 9
    assert p["year"] == 2026
    assert p["day"] == 30

    p2 = parse_free_text_commodity("COPPER FUT 30 SEP 26", as_of=AS_OF)
    assert p2["underlying"] == "COPPER"
    assert p2["month"] == 9
    assert p2["year"] == 2026
    assert p2["day"] == 30


def test_parse_free_text_front_month_only():
    p = parse_free_text_commodity("COPPER", as_of=AS_OF)
    assert p["underlying"] == "COPPER"
    assert p["month"] is None
    assert p["year"] is None
    assert p["parse_mode"] == "free_text_front"


def test_parse_free_text_other_commodities():
    ng = parse_free_text_commodity("NATURALGAS OCT FUT", as_of=AS_OF)
    assert ng["underlying"] == "NATURALGAS"
    assert ng["month"] == 10
    assert ng["year"] == 2026

    sm = parse_free_text_commodity("SILVERM NOV", as_of=AS_OF)
    assert sm["underlying"] == "SILVERMINI"
    assert sm["month"] == 11

    gold = parse_free_text_commodity("GOLD DEC 26", as_of=AS_OF)
    assert gold["underlying"] == "GOLD"
    assert gold["month"] == 12
    assert gold["year"] == 2026

    crude_m = parse_free_text_commodity("CRUDEOILM OCT FUT", as_of=AS_OF)
    assert crude_m["underlying"] == "CRUDEOILM"
    assert crude_m["month"] == 10

    zinc = parse_free_text_commodity("ZINCSEPFUT", as_of=AS_OF)
    assert zinc["underlying"] == "ZINC"
    assert zinc["month"] == 9

    zinc_m = parse_free_text_commodity("ZINCMINI OCT FUT", as_of=AS_OF)
    assert zinc_m["underlying"] == "ZINCMINI"
    assert zinc_m["month"] == 10

    zinc_m2 = parse_free_text_commodity("ZINCMINISEPFUT", as_of=AS_OF)
    assert zinc_m2["underlying"] == "ZINCMINI"
    assert zinc_m2["month"] == 9

    alu_m = parse_free_text_commodity("ALUMINI SEP FUT", as_of=AS_OF)
    assert alu_m["underlying"] == "ALUMINI"
    assert alu_m["month"] == 9

    alu_m2 = parse_free_text_commodity("ALUMINIFUT", as_of=AS_OF)
    assert alu_m2["underlying"] == "ALUMINI"

    alu_full = parse_free_text_commodity("ALUMINIUM SEP FUT", as_of=AS_OF)
    assert alu_full["underlying"] == "ALUMINIUM"
    assert alu_full["month"] == 9


def test_parse_tv_symbol_still_prefers_letter_year():
    p = parse_tv_symbol("NATURALGASV2026")
    assert p["underlying"] == "NATURALGAS"
    assert p["month"] == 10
    assert p["year"] == 2026
    assert p["parse_mode"] == "tv"


def test_attach_free_text_resolves_exact_month_and_lot():
    rows = [
        {
            "instrument_type": "FUT",
            "segment": "MCX_FO",
            "weekly": False,
            "underlying_symbol": "COPPER",
            "instrument_key": "MCX_FO|CU_SEP",
            "trading_symbol": "COPPER FUT 30 SEP 26",
            "lot_size": 2500,
            "expiry": int(datetime(2026, 9, 30, 12, 0, tzinfo=timezone.utc).timestamp() * 1000),
        },
        {
            "instrument_type": "FUT",
            "segment": "MCX_FO",
            "weekly": False,
            "underlying_symbol": "COPPER",
            "instrument_key": "MCX_FO|CU_OCT",
            "trading_symbol": "COPPER FUT 30 OCT 26",
            "lot_size": 2500,
            "expiry": int(datetime(2026, 10, 30, 12, 0, tzinfo=timezone.utc).timestamp() * 1000),
        },
    ]

    def parse_exp(v):
        return int(v) if v else None

    with patch(
        "backend.services.divtest.instruments.ensure_instrument_master",
        return_value=rows,
    ), patch(
        "backend.services.divtest.instruments._parse_expiry_ms",
        side_effect=parse_exp,
    ), patch(
        "backend.services.commodities_div.mapping._resolve_cache",
        {},
    ):
        for raw in ("COPPER SEP FUT", "COPPERSEPFUT", "COPPER 30 SEP 26"):
            inst = attach_instrument_fields(raw, resolve_contract=True)
            assert inst["underlying_matched"] is True, raw
            assert inst["symbol_mapped"] == "COPPER", raw
            assert inst["instrument_key"] == "MCX_FO|CU_SEP", raw
            assert inst["contract"] == "COPPER FUT 30 SEP 26", raw
            assert inst["lot_size"] == 2500, raw
            assert inst["match_mode"] == "exact_month", raw

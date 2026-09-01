"""Trap-CE Live webhook parse tests (no DB)."""
from backend.services.trap_ce_live_webhook import (
    PARSE_FAILED,
    PARSE_PARTIAL,
    PARSE_SUCCESS,
    decode_raw_payload,
    parse_trap_ce_webhook,
)


def test_success_pairs_by_position():
    body = {
        "stocks": "SYMBOL 1, SYMBOL 2, SYMBOL 3",
        "trigger_prices": "2500.00,600.00,3400.00",
        "triggered_at": "8:47 pm",
        "scan_name": "Trap_Intraday_BS",
        "alert_name": "Alert for Trap_CE_BS",
    }
    status, rows = parse_trap_ce_webhook(body)
    assert status == PARSE_SUCCESS
    assert [r["symbol"] for r in rows] == ["SYMBOL 1", "SYMBOL 2", "SYMBOL 3"]
    assert [r["trigger_price"] for r in rows] == [2500.0, 600.0, 3400.0]


def test_unequal_length_is_partial():
    status, rows = parse_trap_ce_webhook({
        "stocks": "A, B, C",
        "trigger_prices": "1,2",
    })
    assert status == PARSE_PARTIAL
    assert len(rows) == 3
    assert rows[2]["trigger_price"] is None
    assert rows[0]["trigger_price"] == 1.0


def test_missing_stocks_is_failed():
    status, rows = parse_trap_ce_webhook({"trigger_prices": "1,2"})
    assert status == PARSE_FAILED
    assert rows == []


def test_unusable_body_is_failed():
    status, rows = parse_trap_ce_webhook(["not", "a", "dict"])
    assert status == PARSE_FAILED
    assert rows == []


def test_decode_keeps_json_object():
    raw = b'{"stocks":"AAA","trigger_prices":"1.5"}'
    parsed, payload = decode_raw_payload(raw)
    assert parsed == payload
    assert parsed["stocks"] == "AAA"


def test_decode_unusable_still_stores_raw():
    parsed, payload = decode_raw_payload(b"not-json {")
    assert parsed is None
    assert payload["_raw"] == "not-json {"

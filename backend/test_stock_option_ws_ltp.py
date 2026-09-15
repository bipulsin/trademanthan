"""Unit tests for Executed option WS LTP helpers (no live Upstox)."""
from backend.services.stock_option_ws_ltp import (
    _normalize_ik,
    _rebuild_ik_map,
    executed_option_instrument_keys,
)


def test_normalize_ik_colon_to_pipe():
    assert _normalize_ik("NSE_FO:12345") == "NSE_FO|12345"
    assert _normalize_ik("  NSE_FO|99  ") == "NSE_FO|99"


def test_rebuild_ik_map_and_keys():
    rows = [
        {
            "id": 1,
            "sell_instrument_key": "NSE_FO:111",
            "buy_instrument_key": "NSE_FO:222",
        },
        {
            "id": 2,
            "sell_instrument_key": "NSE_FO|111",
            "buy_instrument_key": None,
        },
    ]
    mapping = _rebuild_ik_map(rows)
    assert mapping["NSE_FO|111"] == [(1, "sell"), (2, "sell")]
    assert mapping["NSE_FO|222"] == [(1, "buy")]
    keys = executed_option_instrument_keys(rows)
    assert keys == ["NSE_FO:111", "NSE_FO:222"]

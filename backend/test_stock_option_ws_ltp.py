"""Unit tests for Executed option WS LTP helpers (no live Upstox)."""
from datetime import datetime

import pytz

from backend.services.stock_option_scheduler import _within_ws_ltp_session
from backend.services.stock_option_signals import SIDE_BEAR, instrument_key_at_strike
from backend.services.stock_option_ws_ltp import (
    _normalize_ik,
    _rebuild_ik_map,
    executed_option_instrument_keys,
)

IST = pytz.timezone("Asia/Kolkata")


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


def test_instrument_key_at_strike_exact():
    chain = {
        "data": [
            {
                "strike_price": 4500,
                "call_options": {"instrument_key": "NSE_FO|AAA"},
            },
            {
                "strike_price": 4600,
                "call_options": {"instrument_key": "NSE_FO|BBB"},
            },
        ]
    }
    assert instrument_key_at_strike(chain, SIDE_BEAR, 4500) == "NSE_FO|AAA"
    assert instrument_key_at_strike(chain, SIDE_BEAR, 4600) == "NSE_FO|BBB"
    assert instrument_key_at_strike(chain, SIDE_BEAR, 4700) is None


def test_ws_ltp_session_starts_0930():
    before = IST.localize(datetime(2026, 9, 15, 9, 29, 59))
    at = IST.localize(datetime(2026, 9, 15, 9, 30, 0))
    after_close = IST.localize(datetime(2026, 9, 15, 15, 36, 0))
    assert not _within_ws_ltp_session(before)
    assert _within_ws_ltp_session(at)
    assert not _within_ws_ltp_session(after_close)

"""Unit checks for curr-month-only candle warm + WS LTP helpers."""
from backend.services.market_data.engine import (
    DEFAULT_CANDLE_LEGS,
    _collect_instrument_keys_for_legs,
    _leg_instrument_key,
)


def test_default_candle_legs_currmth_only():
    assert DEFAULT_CANDLE_LEGS == ("currmth",)


def test_leg_instrument_key():
    row = {
        "stock_instrument_key": "NSE_EQ|1",
        "currmth_future_instrument_key": "NSE_FO|2",
        "nextmth_future_instrement_key": "NSE_FO|3",
    }
    assert _leg_instrument_key(row, "stock") == "NSE_EQ|1"
    assert _leg_instrument_key(row, "currmth") == "NSE_FO|2"
    assert _leg_instrument_key(row, "nextmth") == "NSE_FO|3"


def test_collect_keys_currmth_only():
    rows = [
        {
            "stock": "AAA",
            "stock_instrument_key": "NSE_EQ|A",
            "currmth_future_instrument_key": "NSE_FO|A1",
            "nextmth_future_instrement_key": "NSE_FO|A2",
        },
        {
            "stock": "BBB",
            "stock_instrument_key": "NSE_EQ|B",
            "currmth_future_instrument_key": "NSE_FO|B1",
            "nextmth_future_instrement_key": "NSE_FO|B2",
        },
    ]
    assert _collect_instrument_keys_for_legs(rows, ("currmth",)) == [
        "NSE_FO|A1",
        "NSE_FO|B1",
    ]
    assert _collect_instrument_keys_for_legs(rows, ("stock", "nextmth")) == [
        "NSE_EQ|A",
        "NSE_FO|A2",
        "NSE_EQ|B",
        "NSE_FO|B2",
    ]

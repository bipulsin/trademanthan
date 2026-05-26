"""Centralized market data reads."""
from backend.services.market_data.reads import (
    get_ltp_for_instrument_key,
    is_market_data_fresh,
    ltp_map_with_fallback,
)
from datetime import datetime
import pytz

IST = pytz.timezone("Asia/Kolkata")


def test_is_market_data_fresh_recent():
    now = datetime.now(IST)
    assert is_market_data_fresh(now, max_age_sec=600)


def test_ltp_map_fallback_empty_keys():
    assert ltp_map_with_fallback([], allow_broker_fallback=False) == {}


def test_get_ltp_missing_key():
    assert get_ltp_for_instrument_key("") is None

"""Centralized market data reads."""
from backend.services.market_data.reads import (
    get_ltp_for_instrument_key,
    is_market_data_fresh,
    ltp_map_with_fallback,
)
from backend.services.market_data.repository import normalize_market_data_update
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


def test_normalize_market_data_update_fills_optional_keys():
    row = normalize_market_data_update(
        {"stock": "RELIANCE", "stock_ltp": 2500.0, "market_data_source": "upstox_ws"}
    )
    assert row["stock"] == "RELIANCE"
    assert row["stock_ltp"] == 2500.0
    assert row["stock_vwap"] is None
    assert row["currmth_future_ema5"] is None
    assert "stock_vwap" in row
    assert "currmth_candle_close_5m" in row

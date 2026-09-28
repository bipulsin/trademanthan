"""Chart candle fetch: key normalization and priority over background candle jobs."""
from __future__ import annotations

from backend.services.chart_candles import (
    fetch_chart_candles,
    normalize_chart_instrument_key,
)
from backend.services.upstox_rate_limiter import chart_priority_active


def test_normalize_chart_instrument_key_colon_and_spaces():
    assert normalize_chart_instrument_key("NSE_EQ:INE982J01020") == "NSE_EQ|INE982J01020"
    assert normalize_chart_instrument_key(" nse_eq|ine982j01020 ") == "NSE_EQ|INE982J01020"
    assert normalize_chart_instrument_key("NSE_EQ|INE982J01020") == "NSE_EQ|INE982J01020"


def test_fetch_chart_candles_uses_pipe_key_under_priority(monkeypatch):
    seen = {}

    class FakeUpstox:
        def __init__(self, *args, **kwargs):
            del args, kwargs

        def reload_token_from_storage(self):
            return None

        def get_historical_candles_by_instrument_key(self, instrument_key, interval="hours/1", days_back=2, **kwargs):
            del kwargs
            seen["key"] = instrument_key
            seen["interval"] = interval
            seen["days_back"] = days_back
            seen["priority"] = chart_priority_active()
            return [
                {
                    "timestamp": "2026-09-28T11:15:00+05:30",
                    "open": 1640.0,
                    "high": 1652.0,
                    "low": 1638.0,
                    "close": 1645.3,
                    "volume": 1200,
                }
            ]

    monkeypatch.setattr("backend.services.chart_candles.UpstoxService", FakeUpstox)
    out = fetch_chart_candles("NSE_EQ:INE982J01020", "2h")
    assert seen["key"] == "NSE_EQ|INE982J01020"
    assert seen["interval"] == "hours/2"
    assert seen["priority"] is True
    assert out["count"] == 1
    assert out["bars"][0]["close"] == 1645.3
    assert out["instrument_key"] == "NSE_EQ|INE982J01020"
    assert chart_priority_active() is False

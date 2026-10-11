"""NSE MCP gateway. MCP payloads here are fixtures, not a live tools/list."""
from __future__ import annotations

import threading
import time
from datetime import datetime

import pytz

from backend.config import settings
from backend.services.market_data import gateway
from backend.services.market_data.gateway import (
    candle_series_continuous,
    fetch_candles,
    fetch_quote,
    join_candle_series,
    reset_gateway_state,
    resolve_capability,
)
from backend.services.market_data.nse_mcp_client import McpCallError

IST = pytz.timezone("Asia/Kolkata")
NOW = IST.localize(datetime(2026, 10, 9, 10, 1, 0))


class BoomClient:
    def call_tool(self, *args, **kwargs):
        raise AssertionError("MCP must not be called")


class ScriptedClient:
    def __init__(self, payload=None, error: Exception | None = None, delay: float = 0.0):
        self.payload = payload
        self.error = error
        self.delay = delay
        self.calls = []

    def call_tool(self, server, tool, arguments, timeout):
        self.calls.append(
            {"server": server, "tool": tool, "arguments": arguments, "timeout": timeout}
        )
        if self.delay:
            time.sleep(self.delay)
        if self.error:
            raise self.error
        return self.payload


class FakeUpstox:
    def __init__(self, quote=None, candles=None, error: Exception | None = None):
        self.quote = quote
        self.candles = candles
        self.error = error
        self.calls = []

    def get_market_quote_by_key(self, instrument_key):
        self.calls.append(("quote", instrument_key))
        if self.error:
            raise self.error
        return self.quote

    def get_historical_candles_by_instrument_key(self, instrument_key, **kwargs):
        self.calls.append(("candles", instrument_key, kwargs))
        if self.error:
            raise self.error
        return self.candles

    def place_order(self, *args, **kwargs):
        self.calls.append(("place_order", args, kwargs))
        raise AssertionError("gateway placed an order")

    def cancel_order(self, *args, **kwargs):
        self.calls.append(("cancel_order", args, kwargs))
        raise AssertionError("gateway cancelled an order")

    def modify_order(self, *args, **kwargs):
        self.calls.append(("modify_order", args, kwargs))
        raise AssertionError("gateway modified an order")


def _fresh_equity():
    return {
        "updatedAt": "2026-10-09T04:31:00Z",
        "stock": {
            "symbol": "RELIANCE",
            "lastTradedPrice": 1170.3,
            "openPrice": 1179.0,
            "highPrice": 1180.0,
            "lowPrice": 1160.2,
            "preClosePrice": 1178.0,
            "latestTimestamp": "2026-10-09 10:00:27",
        },
    }


def _upstox_quote():
    return {
        "last_price": 2500.5,
        "close_price": 2490.0,
        "ohlc": {"open": 2488.0, "high": 2510.0, "low": 2480.0, "close": 2490.0},
    }


def setup_function():
    reset_gateway_state()


def teardown_function():
    reset_gateway_state()


def _enable(monkeypatch):
    monkeypatch.setattr(settings, "NSE_MCP_ENABLED", True)
    monkeypatch.setattr(settings, "NSE_MCP_PRODUCTION_APPROVED", True)
    monkeypatch.setattr(settings, "NSE_MCP_TRADING_TIMEOUT_SEC", 1.5)
    monkeypatch.setattr(settings, "NSE_MCP_QUOTE_MAX_AGE_SEC", 180.0)


def test_flags_default_off():
    text = open("backend/config.py", encoding="utf-8").read()
    assert 'os.getenv("NSE_MCP_ENABLED", "false")' in text
    assert 'os.getenv("NSE_MCP_PRODUCTION_APPROVED", "false")' in text
    assert settings.NSE_MCP_ENABLED is False
    assert settings.NSE_MCP_PRODUCTION_APPROVED is False


def test_flags_off_skips_mcp_and_uses_upstox(monkeypatch):
    monkeypatch.setattr(settings, "NSE_MCP_ENABLED", False)
    monkeypatch.setattr(settings, "NSE_MCP_PRODUCTION_APPROVED", False)
    ux = FakeUpstox(quote=_upstox_quote())
    started = time.perf_counter()
    result = fetch_quote("NSE_INDEX|Nifty 50", upstox=ux, client=BoomClient(), now=NOW)
    elapsed = time.perf_counter() - started
    assert result.status == "ok"
    assert result.source == "upstox"
    assert result.quote["last_price"] == 2500.5
    assert result.fallback_reason == "nse_mcp_disabled"
    assert elapsed < 0.2
    assert [c[0] for c in ux.calls] == ["quote"]


def test_enabled_without_approval_skips_mcp(monkeypatch):
    monkeypatch.setattr(settings, "NSE_MCP_ENABLED", True)
    monkeypatch.setattr(settings, "NSE_MCP_PRODUCTION_APPROVED", False)
    ux = FakeUpstox(quote=_upstox_quote())
    result = fetch_quote("NSE_EQ|RELIANCE", upstox=ux, client=BoomClient(), now=NOW)
    assert result.source == "upstox"
    assert result.fallback_reason == "nse_mcp_not_approved"


def test_mcp_fresh_equity_quote(monkeypatch):
    _enable(monkeypatch)
    client = ScriptedClient(_fresh_equity())
    ux = FakeUpstox(quote=_upstox_quote())
    result = fetch_quote("NSE_EQ|RELIANCE", upstox=ux, client=client, now=NOW)
    assert result.status == "ok"
    assert result.source == "nse_mcp"
    assert result.freshness == "fresh"
    assert result.quote["last_price"] == 1170.3
    assert result.quote["close_price"] == 1178.0
    assert result.source_timestamp is not None
    assert result.ingest_time
    assert result.fallback_reason is None
    assert ux.calls == []
    assert client.calls[0]["tool"] == "cm_get_stock_quote"
    assert client.calls[0]["arguments"] == {"symbol": "RELIANCE"}
    assert client.calls[0]["timeout"] == 1.5


def test_stale_mcp_falls_back_to_upstox(monkeypatch):
    _enable(monkeypatch)
    payload = _fresh_equity()
    payload["stock"]["latestTimestamp"] = "2026-10-07 16:00:27"
    client = ScriptedClient(payload)
    ux = FakeUpstox(quote=_upstox_quote())
    result = fetch_quote("NSE_EQ|RELIANCE", upstox=ux, client=client, now=NOW)
    assert result.source == "upstox"
    assert result.fallback_reason == "stale_mcp"
    assert result.quote["last_price"] == 2500.5
    assert len(client.calls) == 1
    assert ux.calls


def test_unsupported_future_does_not_wait(monkeypatch):
    _enable(monkeypatch)
    ux = FakeUpstox(quote=_upstox_quote())
    started = time.perf_counter()
    result = fetch_quote("NSE_FO|RELIANCE25OCTFUT", upstox=ux, client=BoomClient(), now=NOW)
    elapsed = time.perf_counter() - started
    assert result.source == "upstox"
    assert result.fallback_reason == "futures_or_options"
    assert elapsed < 0.2, f"unsupported path took {elapsed:.4f}s"
    cap = resolve_capability("NSE_FO|RELIANCE25OCTFUT", data_type="quote")
    assert cap.supported is False


def test_intraday_and_oi_skip_mcp(monkeypatch):
    _enable(monkeypatch)
    assert resolve_capability("NSE_EQ|RELIANCE", data_type="candles", interval="minutes/5").reason == (
        "intraday_candles"
    )
    assert resolve_capability("NSE_EQ|RELIANCE", data_type="oi").reason == "open_interest"
    assert resolve_capability("NSE_EQ|INE002A01018", data_type="quote").reason == "isin_without_symbol"
    ux = FakeUpstox(candles=[{"timestamp": "2026-10-09T10:00:00+05:30", "open": 1, "high": 2, "low": 1, "close": 2, "volume": 3}])
    started = time.perf_counter()
    result = fetch_candles(
        "NSE_FO|123",
        interval="minutes/15",
        days_back=3,
        upstox=ux,
        client=BoomClient(),
        now=NOW,
    )
    elapsed = time.perf_counter() - started
    assert result.source == "upstox"
    assert result.fallback_reason == "futures_or_options"
    assert elapsed < 0.2, f"futures candle path took {elapsed:.4f}s"
    assert ux.calls[0][0] == "candles"
    assert ux.calls[0][2]["interval"] == "minutes/15"


def test_timeout_then_upstox(monkeypatch):
    _enable(monkeypatch)
    client = ScriptedClient(error=McpCallError("timeout"))
    ux = FakeUpstox(quote=_upstox_quote())
    result = fetch_quote("NSE_INDEX|Nifty 50", upstox=ux, client=client, now=NOW)
    assert result.source == "upstox"
    assert result.fallback_reason == "timeout"
    assert client.calls[0]["tool"] == "cm_get_index_quote"
    assert client.calls[0]["arguments"] == {"indexName": "NIFTY 50"}
    assert len(client.calls) == 1


def test_malformed_mcp_falls_back(monkeypatch):
    _enable(monkeypatch)
    client = ScriptedClient({"stock": {"lastTradedPrice": 0, "latestTimestamp": "2026-10-09 10:00:00"}})
    ux = FakeUpstox(quote=_upstox_quote())
    result = fetch_quote("NSE_EQ|TCS", upstox=ux, client=client, now=NOW)
    assert result.source == "upstox"
    assert result.fallback_reason == "malformed"
    assert result.quote["last_price"] != 0


def test_both_fail_is_unavailable(monkeypatch):
    _enable(monkeypatch)
    client = ScriptedClient(error=McpCallError("transport"))
    ux = FakeUpstox(quote=None)
    result = fetch_quote("NSE_EQ|INFY", upstox=ux, client=client, now=NOW)
    assert result.status == "unavailable"
    assert result.source == "unavailable"
    assert result.quote is None
    assert result.candles is None
    assert result.calculation_available is False
    assert "upstox_failed" in (result.fallback_reason or "")
    assert result.as_dict().get("last_price") is None


def test_daily_history_success_and_incomplete_not_mixed(monkeypatch):
    _enable(monkeypatch)
    bars = []
    for day, close in (("2026-10-06", 100.0), ("2026-10-07", 101.0), ("2026-10-08", 102.0), ("2026-10-09", 103.0)):
        bars.append(
            {
                "date": day,
                "open": close - 1,
                "high": close + 1,
                "low": close - 2,
                "close": close,
                "volume": 10,
            }
        )
    client = ScriptedClient({"data": bars, "from_date": "2026-10-06", "to_date": "2026-10-09"})
    ux = FakeUpstox(candles=[{"timestamp": "x", "close": 1}])
    result = fetch_candles(
        "NSE_INDEX|Nifty 50",
        interval="days/1",
        days_back=5,
        upstox=ux,
        client=client,
        now=NOW,
        purpose="historical",
    )
    assert result.source == "nse_mcp"
    assert result.candles[-1]["close"] == 103.0
    assert ux.calls == []
    assert client.calls[0]["tool"] == "get_index_history"

    short = ScriptedClient(
        {"data": [bars[0]], "from_date": "2026-10-06", "to_date": "2026-10-06"}
    )
    ux2 = FakeUpstox(
        candles=[
            {"timestamp": "2026-10-08T00:00:00+05:30", "open": 9, "high": 9, "low": 9, "close": 9, "volume": 1},
            {"timestamp": "2026-10-09T00:00:00+05:30", "open": 8, "high": 8, "low": 8, "close": 8, "volume": 1},
        ]
    )
    mixed = fetch_candles(
        "NSE_INDEX|Nifty Bank",
        interval="days/1",
        days_back=12,
        upstox=ux2,
        client=short,
        now=NOW,
        purpose="historical",
    )
    assert mixed.source == "upstox"
    assert mixed.fallback_reason == "incomplete_series"
    assert [c["close"] for c in mixed.candles] == [9, 8]
    assert 100.0 not in [c["close"] for c in mixed.candles]


def test_discontinuous_mix_marks_calculation_unavailable():
    older = [{"timestamp": "2026-10-08T00:00:00+05:30", "close": 10}]
    newer = [{"timestamp": "2026-10-08T00:00:00+05:30", "close": 12}]
    assert candle_series_continuous(older, newer) is False
    refused = join_candle_series(older, newer, instrument_key="NSE_EQ|RELIANCE")
    assert refused.status == "unavailable"
    assert refused.calculation_available is False
    assert refused.candles is None
    assert refused.fallback_reason == "mixed_series_discontinuous"


def test_cache_keeps_mcp_provenance(monkeypatch):
    _enable(monkeypatch)
    client = ScriptedClient(_fresh_equity())
    ux = FakeUpstox(quote=_upstox_quote())
    first = fetch_quote("NSE_EQ|RELIANCE", upstox=ux, client=client, now=NOW)
    second = fetch_quote("NSE_EQ|RELIANCE", upstox=ux, client=client, now=NOW)
    assert first.source == "nse_mcp"
    assert second.source == "nse_mcp"
    assert second.source_timestamp == first.source_timestamp
    assert len(client.calls) == 1


def test_circuit_opens_without_another_mcp_attempt(monkeypatch):
    _enable(monkeypatch)
    client = ScriptedClient(error=McpCallError("timeout"))
    ux = FakeUpstox(quote=_upstox_quote())
    for _ in range(3):
        fetch_quote("NSE_EQ|RELIANCE", upstox=ux, client=client, now=NOW)
    assert len(client.calls) == 3
    opened = fetch_quote("NSE_EQ|TCS", upstox=ux, client=BoomClient(), now=NOW)
    assert opened.fallback_reason == "circuit_open"
    assert opened.source == "upstox"


def test_in_flight_quote_is_coalesced(monkeypatch):
    _enable(monkeypatch)
    client = ScriptedClient(_fresh_equity(), delay=0.2)
    ux = FakeUpstox(quote=_upstox_quote())
    results = []

    def _run():
        results.append(fetch_quote("NSE_EQ|RELIANCE", upstox=ux, client=client, now=NOW))

    threads = [threading.Thread(target=_run) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(2)
    assert len(client.calls) == 1
    assert all(item.source == "nse_mcp" for item in results)


def test_no_order_side_effects(monkeypatch):
    _enable(monkeypatch)
    ux = FakeUpstox(quote=None, candles=None)
    fetch_quote("NSE_FO|ABC", upstox=ux, client=BoomClient(), now=NOW)
    fetch_candles(
        "NSE_FO|ABC",
        interval="minutes/5",
        days_back=2,
        upstox=ux,
        client=BoomClient(),
        now=NOW,
    )
    names = [c[0] for c in ux.calls]
    assert "place_order" not in names
    assert "cancel_order" not in names
    assert "modify_order" not in names
    source = open("backend/services/market_data/gateway.py", encoding="utf-8").read()
    assert "place_order" not in source
    assert "cancel_order" not in source

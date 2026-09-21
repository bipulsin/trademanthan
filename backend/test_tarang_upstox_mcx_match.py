"""Upstox MCX adapter: join quotes/greeks by instrument_token when keyed as MCX_FO:SYMBOL."""
from __future__ import annotations

from typing import Any, Dict

from backend.services.tarang.adapters.upstox_mcx import UpstoxMcxAdapter


def _mcx_symbol_keyed_payload() -> Dict[str, Any]:
    """Simulate Upstox response: top-level key is trading symbol, token on instrument_token."""
    return {
        "MCX_FO:CRUDEOILM26OCT7850PE": {
            "instrument_token": "MCX_FO|580909",
            "last_price": 42.5,
            "oi": 1200,
            "volume": 80,
            "depth": {
                "buy": [{"price": 41.0, "quantity": 5}],
                "sell": [{"price": 44.0, "quantity": 3}],
            },
            "iv": 0.38,
            "delta": -0.42,
            "gamma": 0.01,
            "theta": -0.05,
            "vega": 0.12,
        }
    }


def test_fetch_quotes_matches_by_instrument_token(monkeypatch):
    adapter = UpstoxMcxAdapter.__new__(UpstoxMcxAdapter)
    payload = _mcx_symbol_keyed_payload()

    def fake_get(url: str) -> Dict[str, Any]:
        assert "market-quote/quotes" in url
        return {"http_status": 200, "body": {"data": payload}}

    monkeypatch.setattr(adapter, "_get", fake_get)
    out = adapter._fetch_quotes(["MCX_FO|580909"])
    assert "MCX_FO|580909" in out
    row = out["MCX_FO|580909"]
    assert row["bid"] == 41.0
    assert row["ask"] == 44.0
    assert row["mid"] == 42.5
    assert row["two_sided"] is True
    assert row["last"] == 42.5


def test_fetch_greeks_matches_by_instrument_token(monkeypatch):
    adapter = UpstoxMcxAdapter.__new__(UpstoxMcxAdapter)
    payload = _mcx_symbol_keyed_payload()

    def fake_get(url: str) -> Dict[str, Any]:
        assert "option-greek" in url
        return {"http_status": 200, "body": {"data": payload}}

    monkeypatch.setattr(adapter, "_get", fake_get)
    out = adapter._fetch_greeks(["MCX_FO|580909"])
    assert "MCX_FO|580909" in out
    gd = out["MCX_FO|580909"]
    assert gd["iv"] == 0.38
    assert gd["delta"] == -0.42


def test_fetch_quotes_still_matches_exact_top_level_key(monkeypatch):
    """Keep behavior for payloads already keyed as MCX_FO|token."""
    adapter = UpstoxMcxAdapter.__new__(UpstoxMcxAdapter)
    payload = {
        "MCX_FO|580909": {
            "instrument_token": "MCX_FO|580909",
            "last_price": 10.0,
            "depth": {
                "buy": [{"price": 9.0, "quantity": 1}],
                "sell": [{"price": 11.0, "quantity": 1}],
            },
        }
    }

    monkeypatch.setattr(
        adapter,
        "_get",
        lambda url: {"http_status": 200, "body": {"data": payload}},
    )
    out = adapter._fetch_quotes(["MCX_FO|580909"])
    assert out["MCX_FO|580909"]["bid"] == 9.0
    assert out["MCX_FO|580909"]["ask"] == 11.0

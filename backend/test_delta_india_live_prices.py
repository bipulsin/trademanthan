"""Desktop crypto cards mapped from a mocked Delta Exchange India ticker list."""
from __future__ import annotations

import backend.services.delta_india_live_prices as prices


def _ticker(symbol, close, change, status="operational"):
    underlying = symbol.replace("USD", "")
    return {
        "symbol": symbol,
        "contract_type": "perpetual_futures",
        "underlying_asset_symbol": underlying,
        "product_trading_status": status,
        "close": close,
        "ltp_change_24h": change,
    }


def test_mocked_delta_tickers_fill_live_cards_and_leave_gaps_unavailable(monkeypatch):
    def fake_fetch(**kwargs):
        assert kwargs["contract_types"] == "perpetual_futures"
        assert "BTC" in kwargs["underlying_asset_symbols"]
        assert "POL" in kwargs["underlying_asset_symbols"]
        return {
            "ok": True,
            "result": [
                _ticker("BTCUSD", 83000.5, 0.1526),
                _ticker("ETHUSD", "2500.2", 0.1743),
                _ticker("POLUSD", 0.10152, 0.9446),
                # Expired MATIC contract must not price the Polygon card.
                _ticker("MATICUSD", 0.55, 1.0),
                {"symbol": "SOLUSD", "contract_type": "perpetual_futures", "close": None},
            ],
        }

    monkeypatch.setattr(prices, "fetch_public_tickers", fake_fetch)
    payload = prices.build_desktop_crypto_prices()
    assert payload["ok"] is True
    assert payload["source"] == "delta_india"
    assert payload["refresh_seconds"] == 300
    assert payload["updated_at"]
    by_card = {row["card"]: row for row in payload["prices"]}

    assert by_card["BTCUSDT"]["available"] is True
    assert by_card["BTCUSDT"]["product_symbol"] == "BTCUSD"
    assert by_card["BTCUSDT"]["price"] == 83000.5
    assert by_card["BTCUSDT"]["change_percent"] == 0.1526

    assert by_card["ETHUSDT"]["available"] is True
    assert by_card["ETHUSDT"]["product_symbol"] == "ETHUSD"
    assert by_card["ETHUSDT"]["price"] == 2500.2

    assert by_card["MATICUSDT"]["product_symbol"] == "POLUSD"
    assert by_card["MATICUSDT"]["available"] is True
    assert by_card["MATICUSDT"]["price"] == 0.10152

    assert by_card["BNBUSDT"]["available"] is False
    assert by_card["BNBUSDT"]["price"] is None
    assert by_card["SOLUSDT"]["available"] is False
    assert by_card["SOLUSDT"]["price"] is None
    assert all(row["price"] != 45000 for row in payload["prices"])


def test_delta_down_marks_every_card_unavailable(monkeypatch):
    monkeypatch.setattr(
        prices,
        "fetch_public_tickers",
        lambda **kwargs: {"ok": False, "error": "request_failed", "result": []},
    )
    payload = prices.build_desktop_crypto_prices()
    assert payload["ok"] is False
    assert payload["source"] == "delta_india"
    assert payload["updated_at"] is None
    assert payload["error"] == "request_failed"
    assert len(payload["prices"]) == 8
    assert all(row["available"] is False and row["price"] is None for row in payload["prices"])

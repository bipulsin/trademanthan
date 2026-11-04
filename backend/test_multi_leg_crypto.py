"""Delta India crypto option keys for Multi-Leg Bitcoin / Ethereum."""
from datetime import date

from backend.services.multi_leg_crypto import (
    constructed_crypto_option_key,
    is_crypto_instrument,
    is_delta_option_key,
    parse_crypto_option_key,
    resolve_crypto_option_contract,
    split_upstox_and_delta_keys,
)


def test_crypto_instrument_and_symbol_roundtrip():
    assert is_crypto_instrument("Bitcoin")
    assert is_crypto_instrument("ETHEREUM")
    assert not is_crypto_instrument("NIFTY")
    key = constructed_crypto_option_key("BITCOIN", date(2026, 11, 27), 99000, "CE")
    assert key == "C-BTC-99000-271126"
    assert is_delta_option_key(key)
    assert not is_delta_option_key("NSE_FO|NIFTY26NOV25000CE")
    parsed = parse_crypto_option_key(key)
    assert parsed["instrument"] == "BITCOIN"
    assert parsed["option_type"] == "CE"
    assert parsed["strike"] == 99000.0
    assert parsed["expiry"] == date(2026, 11, 27)


def test_resolve_crypto_option_contract_defaults_lot(monkeypatch):
    monkeypatch.setattr(
        "backend.services.multi_leg_crypto._ticker_rows",
        lambda _und: [{"symbol": "P-ETH-3000-251226", "spot_price": "2800"}],
    )
    resolved = resolve_crypto_option_contract("ETHEREUM", 3000, "PE", date(2026, 12, 25))
    assert resolved["instrument_key"] == "P-ETH-3000-251226"
    assert resolved["lot_size"] == 1
    assert resolved["resolved_from"] == "delta_india"


def test_split_upstox_and_delta_keys():
    upstox, delta = split_upstox_and_delta_keys(
        ["NSE_FO|NIFTY26NOV25000CE", "C-BTC-99000-271126", "", "BSE_FO|SENSEX26NOV80000PE"]
    )
    assert upstox == ["NSE_FO|NIFTY26NOV25000CE", "BSE_FO|SENSEX26NOV80000PE"]
    assert delta == ["C-BTC-99000-271126"]

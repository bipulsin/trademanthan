"""Provider-key merge for shared Upstox market feed (no live WS)."""
from backend.services import upstox_market_feed as feed


def test_combined_keys_merges_providers(monkeypatch):
    monkeypatch.setattr(feed, "_BASE_KEYS_SIG", ("A", "B"))
    monkeypatch.setattr(feed, "_PROVIDER_KEYS", {"stock_option_executed": ("X", "Y")})
    assert feed._combined_subscribe_keys(["A", "B"]) == ["A", "B", "X", "Y"]


def test_provider_key_set_empty(monkeypatch):
    monkeypatch.setattr(feed, "_PROVIDER_KEYS", {})
    assert feed._provider_key_set() == set()

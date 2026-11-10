"""Dhan token manager and client. Mocks only — no PIN, TOTP, or network."""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

import pytest
import pytz

from backend.dhan.auth import DhanAuthError, get_valid_token, parse_token_response
from backend.dhan import auth as auth_mod
from backend.dhan.client import DhanClient

IST = pytz.timezone("Asia/Kolkata")


@contextmanager
def _nolock():
    yield


def test_parse_token_camel_case_expiry():
    token, expiry = parse_token_response(
        {
            "dhanClientId": "1000000401",
            "accessToken": "camel-token",
            "expiryTime": "2026-11-10T18:00:00.000",
        }
    )
    assert token == "camel-token"
    assert expiry.astimezone(IST).hour == 18
    assert expiry.astimezone(IST).date().isoformat() == "2026-11-10"


def test_parse_token_nested_object_snake_case():
    class Bag:
        access_token = "snake-token"
        expiry = "2026-11-12T08:30:00"

    token, expiry = parse_token_response({"status": "success", "data": Bag()})
    assert token == "snake-token"
    assert expiry.astimezone(IST).hour == 8
    assert expiry.astimezone(IST).minute == 30


def test_get_valid_token_returns_stored_when_more_than_two_hours_remain(monkeypatch):
    expiry = datetime.now(timezone.utc) + timedelta(hours=10)
    monkeypatch.setattr(auth_mod, "_refresh_lock", _nolock)
    monkeypatch.setattr(auth_mod, "_read_stored", lambda: ("stored-token", expiry))

    def _renew(*_args, **_kwargs):
        raise AssertionError("renew should not run")

    def _generate():
        raise AssertionError("generate should not run")

    monkeypatch.setattr(auth_mod, "renew_token", _renew)
    monkeypatch.setattr(auth_mod, "generate_token", _generate)
    assert get_valid_token() == "stored-token"


def test_renew_failure_falls_through_to_generate(monkeypatch):
    expiry = datetime.now(timezone.utc) + timedelta(minutes=30)
    monkeypatch.setattr(auth_mod, "_refresh_lock", _nolock)
    monkeypatch.setattr(auth_mod, "_read_stored", lambda: ("old-token", expiry))

    def _renew(*_args, **_kwargs):
        raise DhanAuthError("renew failed")

    def _generate():
        return "fresh-token", datetime.now(timezone.utc) + timedelta(hours=24)

    monkeypatch.setattr(auth_mod, "renew_token", _renew)
    monkeypatch.setattr(auth_mod, "generate_token", _generate)
    assert get_valid_token() == "fresh-token"


def test_trade_history_stops_on_empty_page(monkeypatch):
    seen = []

    def fake_sdk_call(self, name, *args, **kwargs):
        assert name == "get_trade_history"
        page = kwargs.get("page_number", 0)
        seen.append(page)
        if page == 0:
            return {"status": "success", "data": [{"exchangeTradeId": "1", "transactionType": "BUY"}]}
        return {"status": "success", "data": []}

    monkeypatch.setattr(DhanClient, "_sdk_call", fake_sdk_call)
    rows = DhanClient(access_token="test-token").get_trade_history("2026-11-09", "2026-11-09")
    assert seen == [0, 1]
    assert rows == [{"exchangeTradeId": "1", "transactionType": "BUY"}]


def test_auth_failure_retries_once(monkeypatch):
    calls = {"n": 0}
    refreshes = {"n": 0}

    class FakeSdk:
        def get_trade_book(self, order_id=None):
            calls["n"] += 1
            if calls["n"] == 1:
                return {
                    "status": "failure",
                    "remarks": {"error_code": "DH-901", "error_message": "Invalid Token"},
                    "data": "",
                }
            return {"status": "success", "data": [{"exchangeTradeId": "9"}]}

    monkeypatch.setattr(DhanClient, "_build_sdk", lambda self: FakeSdk())

    def rotate(self):
        refreshes["n"] += 1
        self._token = "rotated-token"
        return self._token

    monkeypatch.setattr(DhanClient, "_rotate_token", rotate)
    rows = DhanClient(access_token="old-token").get_trade_book()
    assert calls["n"] == 2
    assert refreshes["n"] == 1
    assert rows == [{"exchangeTradeId": "9"}]


def test_auth_failure_does_not_retry_forever(monkeypatch):
    calls = {"n": 0}

    class FakeSdk:
        def get_trade_book(self, order_id=None):
            calls["n"] += 1
            return {
                "status": "failure",
                "remarks": {"error_message": "Invalid Token"},
                "data": "",
            }

    monkeypatch.setattr(DhanClient, "_build_sdk", lambda self: FakeSdk())
    monkeypatch.setattr(DhanClient, "_rotate_token", lambda self: "rotated-token")
    with pytest.raises(DhanAuthError):
        DhanClient(access_token="old-token").get_trade_book()
    assert calls["n"] == 2

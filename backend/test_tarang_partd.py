"""Part D live-data: daily vs hourly RV, MCX reminder, archive alert kinds."""
from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

from backend.services.tarang.alerts_telegram import OPS_KINDS, _destinations
from backend.services.tarang.mcx_download import AUTOMATION_ALLOWED
from backend.services.tarang.mcx_expiry_reminder import download_span, reminder_lines
from backend.services.tarang.rv import realized_vol_20d, realized_vol_hourly_20d, SYMBOL_FOR_PROFILE
from backend.services.tarang.spread_scan import _chain_from_payload, conservative_credit


def test_gate_rv_is_daily_not_hourly():
    daily = [100.0 + i * 0.1 for i in range(21)]
    hourly = [100.0 + (i % 24) * 0.01 for i in range(20 * 24 + 1)]
    d = realized_vol_20d(daily)
    h = realized_vol_hourly_20d(hourly)
    assert d is not None
    assert h is not None
    assert SYMBOL_FOR_PROFILE["CL"] == "CRUDEOILM"
    assert SYMBOL_FOR_PROFILE["NG"] == "NATGASMINI"
    assert SYMBOL_FOR_PROFILE["BTC"] == "BTCUSD"


def test_mcx_scrape_still_off():
    assert AUTOMATION_ALLOWED is False


def test_download_span_90d():
    lo, hi = download_span(date(2026, 9, 17))
    assert hi == date(2026, 9, 17)
    assert lo == date(2026, 6, 19)


def test_conservative_credit_sell_bid_buy_ask():
    legs = [
        {"side": "SELL", "bid": 10.0, "ask": 11.0},
        {"side": "BUY", "bid": 4.0, "ask": 5.0},
    ]
    assert conservative_credit(legs) == 5.0


def test_archive_and_expiry_kinds_are_ops_private():
    assert "archive_fetch_failed" in OPS_KINDS
    assert "mcx_expiry_download_reminder" in OPS_KINDS
    db = MagicMock()
    with patch("backend.services.tarang.alerts_telegram.private_chat_id", return_value="111"), patch(
        "backend.services.tarang.alerts_telegram.public_signals_enabled", return_value=True
    ), patch("backend.services.tarang.alerts_telegram.public_chat_id", return_value="@Tradewithcto"):
        dests = _destinations(db, "mcx_expiry_download_reminder")
    assert dests == ["111"]


def test_chain_from_payload_appends_dict_quotes():
    chain = _chain_from_payload(
        "BTC",
        "delta_india",
        "BTCUSD",
        "2026-09-25",
        {
            "quotes": [
                {
                    "instrument_key": "C-BTC-100000-250925",
                    "symbol": "C-BTC-100000-250925",
                    "strike": 100000,
                    "right": "C",
                    "bid": 10,
                    "ask": 12,
                    "mid": 11,
                    "delta": -0.12,
                    "contract_value": 0.001,
                }
            ]
        },
        115000,
        1,
        50,
    )
    assert len(chain.quotes) == 1
    assert chain.quotes[0].strike == 100000
    assert chain.quotes[0].delta == -0.12


def test_reminder_lines_empty_when_no_expiry_match():
    with patch("backend.services.tarang.mcx_expiry_reminder.option_expiries_on", return_value=[]):
        assert reminder_lines(date(2026, 9, 20)) == []

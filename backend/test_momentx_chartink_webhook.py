"""Tests for MomentX ChartInk webhook parse + board activation (no Upstox)."""
from __future__ import annotations

import json
from datetime import datetime
from unittest.mock import patch

import pytz

from backend.services.momentx import state as mx_state
from backend.services.momentx.chartink_webhook import (
    ENTRY_GATE_CHARTINK,
    is_chartink_entry_gate,
    parse_chartink_momentx_payload,
    parse_side_from_alert,
    process_momentx_chartink_payload,
)
from backend.services.momentx.day_board import merge_day_cards

IST = pytz.timezone("Asia/Kolkata")


def test_parse_side_from_alert_name():
    assert parse_side_from_alert("Alert for Intraday Momentum Bullish_HA") == "bullish"
    assert parse_side_from_alert("Alert for Intraday Momentum Bearish_HA") == "bearish"
    assert parse_side_from_alert("something", "Bearish scan") == "bearish"
    assert parse_side_from_alert("") == "bullish"


def test_parse_chartink_payload_json():
    body = {
        "stocks": "RELIANCE, INFY, TCS",
        "trigger_prices": "2500.00,600.00,3400.00",
        "triggered_at": "5:12 pm",
        "scan_name": "Intraday Momentum Screener_HA",
        "scan_url": "intraday-momentum-screener-ha",
        "alert_name": "Alert for Intraday Momentum Bullish_HA",
        "webhook_url": "https://tradewithcto.com/webhook/momentx",
    }
    parsed = parse_chartink_momentx_payload(json.dumps(body).encode("utf-8"))
    assert parsed["stocks"] == ["RELIANCE", "INFY", "TCS"]
    assert parsed["side"] == "bullish"
    assert parsed["price_by_stock"]["INFY"] == 600.0
    assert parsed["alert_name"].startswith("Alert for")


def test_parse_bearish_alert():
    body = {
        "stocks": "SBIN",
        "trigger_prices": "800",
        "alert_name": "Alert for Intraday Momentum Bearish_HA",
    }
    parsed = parse_chartink_momentx_payload(json.dumps(body).encode("utf-8"))
    assert parsed["side"] == "bearish"
    assert parsed["stocks"] == ["SBIN"]


def test_chartink_gate_survives_stage1_miss_on_merge():
    now = IST.localize(datetime(2026, 11, 1, 11, 0))
    prev = [
        {
            "stock": "INFY",
            "entry_gate": ENTRY_GATE_CHARTINK,
            "filters_ok": True,
            "side": "bullish",
            "status": "active",
            "stage1": {"source": ENTRY_GATE_CHARTINK, "core_qualified": True},
            "first_seen_at": now.isoformat(),
        }
    ]
    merged, _ = merge_day_cards(
        prev_cards=prev,
        qualified_cards=[],  # Stage-1 did not re-qualify INFY
        stage1_by_stock={},
        now=now,
    )
    by = {c["stock"]: c for c in merged}
    assert by["INFY"]["filters_ok"] is True
    assert by["INFY"]["status"] == "active"
    assert is_chartink_entry_gate(by["INFY"])


def test_process_creates_and_activates_card():
    mx_state.clear_day_board()
    mx_state.ensure_board_date("2026-11-01")
    uni = [
        {
            "stock": "INFY",
            "equity_instrument_key": "NSE_EQ|INFY",
            "future_symbol": "INFY FUT 23 NOV 26",
            "future_instrument_key": "NSE_FO|1",
            "lot_size": 400,
        }
    ]
    parsed = {
        "stocks": ["INFY"],
        "side": "bullish",
        "price_by_stock": {"INFY": 1160.0},
        "alert_name": "Alert for Intraday Momentum Bullish_HA",
        "scan_name": "scan",
        "triggered_at": "5:12 pm",
    }
    with patch(
        "backend.services.momentx.chartink_webhook.load_momentx_universe",
        return_value=uni,
    ), patch(
        "backend.services.momentx.chartink_webhook.past_lock_all",
        return_value=False,
    ), patch(
        "backend.services.momentx.tracker.refresh_emas",
        return_value={"ok": True, "updated": 1},
    ), patch(
        "backend.services.momentx.persistence.record_filtered_symbols",
        return_value=1,
    ), patch(
        "backend.services.momentx.persistence.record_activation",
        return_value=None,
    ):
        out = process_momentx_chartink_payload(parsed)
    assert out["ok"] is True
    assert out["created"] == ["INFY"]
    assert "INFY" in out["activated"]
    cards = mx_state.ready_cards()
    assert len(cards) == 1
    assert cards[0]["side"] == "bullish"
    assert cards[0]["status"] == "active"
    assert cards[0]["entry_gate"] == ENTRY_GATE_CHARTINK
    assert cards[0]["filters_ok"] is True

    # Second webhook updates existing card (bearish flip) without duplicating.
    parsed2 = dict(parsed)
    parsed2["side"] = "bearish"
    parsed2["alert_name"] = "Alert for Intraday Momentum Bearish_HA"
    with patch(
        "backend.services.momentx.chartink_webhook.load_momentx_universe",
        return_value=uni,
    ), patch(
        "backend.services.momentx.chartink_webhook.past_lock_all",
        return_value=False,
    ), patch(
        "backend.services.momentx.tracker.refresh_emas",
        return_value={"ok": True, "updated": 1},
    ), patch(
        "backend.services.momentx.persistence.record_activation",
        return_value=None,
    ):
        out2 = process_momentx_chartink_payload(parsed2)
    assert out2["created"] == []
    assert out2["updated"] == ["INFY"]
    cards2 = mx_state.ready_cards()
    assert len(cards2) == 1
    assert cards2[0]["side"] == "bearish"

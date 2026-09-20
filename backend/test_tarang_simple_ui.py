"""Tests for Tarang simple UI rules: one FT per symbol, unconfirmed Live, void window, IST."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from backend.services.tarang.calendar import format_ist, parse_to_ist_str
from backend.services.tarang.heartbeat import SILENCE_SEC, check_watchdog
from backend.services.tarang.labels import format_inr, gate_plain, friendly_symbol
from backend.services.tarang.report import compute_metrics


def test_format_inr_indian_grouping():
    assert format_inr(2938) == "₹2,938"
    assert format_inr(123456) == "₹1,23,456"


def test_format_ist_helper():
    dt = datetime(2026, 9, 20, 6, 52, tzinfo=timezone.utc)
    s = format_ist(dt)
    assert "IST" in s
    assert "20 Sep 2026" in s
    assert parse_to_ist_str(dt.isoformat())


def test_gate_plain_map():
    assert "realized" in gate_plain("iv_vs_rv").lower()
    assert friendly_symbol("CL")["code"] == "CL"


def test_live_metrics_exclude_unconfirmed():
    confirmed = {
        "record_type": "LIVE",
        "fills_confirmed": True,
        "net_pnl": 100,
        "gross_pnl": 100,
        "fees_total": 0,
        "origin": "USER",
    }
    unconfirmed = {
        "record_type": "LIVE",
        "fills_confirmed": False,
        "net_pnl": 999,
        "gross_pnl": 999,
        "fees_total": 0,
        "origin": "USER",
    }
    live = [t for t in [confirmed, unconfirmed] if t.get("fills_confirmed")]
    m = compute_metrics(live)
    assert m["count"] == 1
    assert m["net_total"] == 100
    assert m["stats_ready"] is False
    assert "Too few" in (m.get("stats_note") or "")


def test_void_window():
    from backend.services.tarang.lifecycle import void_user_trade

    with patch("backend.services.tarang.lifecycle.get_trade") as gt:
        gt.return_value = {
            "id": 1,
            "entry_at": (datetime.now(timezone.utc) - timedelta(minutes=6)).isoformat(),
            "voided": False,
        }
        out = void_user_trade(1, "mistake")
        assert out["ok"] is False
        assert out["error"] == "void_window_elapsed"
        gt.return_value = {
            "id": 1,
            "entry_at": datetime.now(timezone.utc).isoformat(),
            "voided": False,
        }
        with patch("backend.services.tarang.lifecycle.SessionLocal") as sl, patch(
            "backend.services.tarang.lifecycle.append_event"
        ), patch("backend.services.tarang.lifecycle.ensure_tarang_tables"):
            db = MagicMock()
            sl.return_value = db
            out2 = void_user_trade(1, "mistake")
        assert out2["ok"] is True


def test_one_open_ft_per_symbol_skip():
    from backend.services.tarang.forward_record import record_forward_tests_from_screen

    results = [
        {
            "status": "QUALIFIED",
            "candidate_id": 9,
            "profile_id": "BTC",
            "underlying": "BTC",
            "expiry": "2026-10-01",
            "structure": "put_credit_spread",
            "legs": [{"strike": 100}],
        }
    ]
    db = MagicMock()
    db.execute.return_value.mappings.return_value.first.return_value = {"id": 44}

    with patch("backend.services.tarang.forward_record.ensure_tarang_tables"), patch(
        "backend.services.tarang.forward_record.SessionLocal", return_value=db
    ), patch("backend.services.tarang.forward_record.forward_test_cfg", return_value={"cooldown_hours": 6}), patch(
        "backend.services.tarang.forward_record.get_events", return_value={"events": []}
    ), patch("backend.services.tarang.lifecycle.take_trade_paper") as take:
        out = record_forward_tests_from_screen(results)
    take.assert_not_called()
    assert out["skipped"]
    assert out["skipped"][0]["reason"] == "open_forward_test_symbol"


def test_watchdog_no_repeat_while_open():
    assert SILENCE_SEC == 180
    db = MagicMock()
    old = datetime.now(timezone.utc) - timedelta(seconds=240)
    db.execute.return_value.mappings.return_value.all.return_value = [{"key": "scheduler", "last_beat_at": old}]
    db.execute.return_value.mappings.return_value.first.return_value = {"last_message": "open"}

    with patch("backend.services.tarang.heartbeat.ensure_tarang_tables"), patch(
        "backend.services.tarang.heartbeat.SessionLocal", return_value=db
    ), patch("backend.services.tarang.alerts_telegram.notify_critical") as n:
        out = check_watchdog()
    n.assert_not_called()
    assert out["fired"] == []

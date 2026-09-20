"""Tarang follow-up A–G: token window, telegram routing, immutability, bhavcopy, lock, backup."""
from __future__ import annotations

import os
from datetime import datetime
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

from backend.services.tarang.alerts_telegram import _destinations, handle_telegram_update
from backend.services.tarang.backup import run_backup
from backend.services.tarang.forward_record import build_immutable_snapshot
from backend.services.tarang.mcx_download import BHAVCOPY_URL, run_mcx_bhavcopy_download
from backend.services.tarang.token_check import should_alert_token

IST = ZoneInfo("Asia/Kolkata")


def test_token_alert_not_at_0330():
    assert should_alert_token(datetime(2026, 9, 21, 3, 30, tzinfo=IST)) is False
    assert should_alert_token(datetime(2026, 9, 21, 8, 45, tzinfo=IST)) is True
    assert should_alert_token(datetime(2026, 9, 21, 9, 0, tzinfo=IST)) is True
    assert should_alert_token(datetime(2026, 9, 21, 9, 5, tzinfo=IST)) is True
    assert should_alert_token(datetime(2026, 9, 21, 12, 0, tzinfo=IST)) is False


def test_telegram_ops_private_only_signals_gated():
    db = MagicMock()
    with patch("backend.services.tarang.alerts_telegram.private_chat_id", return_value="111"), patch(
        "backend.services.tarang.alerts_telegram.public_signals_enabled", return_value=False
    ):
        ops = _destinations(db, "upstox_token_expired")
        sig = _destinations(db, "forward_test_signal")
    assert ops == ["111"]
    assert sig == ["111"]
    with patch("backend.services.tarang.alerts_telegram.private_chat_id", return_value="111"), patch(
        "backend.services.tarang.alerts_telegram.public_signals_enabled", return_value=True
    ), patch("backend.services.tarang.alerts_telegram.public_chat_id", return_value="@Tradewithcto"):
        sig2 = _destinations(db, "forward_test_signal")
        ops2 = _destinations(db, "kill_switch")
    assert "@Tradewithcto" in sig2
    assert ops2 == ["111"]


def test_telegram_start_saves_chat(monkeypatch):
    db = MagicMock()
    with patch("backend.database.SessionLocal", return_value=db), patch(
        "backend.services.tarang.alerts_telegram.ensure_tarang_tables"
    ), patch("backend.services.tarang.alerts_telegram.save_private_chat_id") as save, patch(
        "backend.services.tarang.alerts_telegram._send", return_value=True
    ):
        out = handle_telegram_update({"message": {"text": "/start tarang", "chat": {"id": 999, "username": "bipulsahay"}}})
    assert out.get("linked") == "999"
    save.assert_called()


def test_snapshot_stores_commit_and_ruleset():
    with patch("backend.services.tarang.forward_record.code_commit", return_value="abc123"), patch(
        "backend.services.tarang.forward_record.rule_set_version", return_value=7
    ):
        s = build_immutable_snapshot({"legs": []})
    assert s["code_commit"] == "abc123"
    assert s["rule_set_version"] == 7


def test_bhavcopy_url_not_upstox():
    assert "upstox" not in BHAVCOPY_URL.lower()
    with patch("backend.services.tarang.mcx_download.probe_endpoints", return_value={"ok": False, "blocked": True}):
        out = run_mcx_bhavcopy_download(force_probe=True)
    assert out["upstox_used"] is False
    assert out["blocked"] is True


def test_backup_dry_run():
    out = run_backup(dry_run=True)
    assert out["dry_run"] is True
    assert "pg_dump" in out["command"]
    assert "tarang_trades" in out["command"]


def test_exclude_requires_reason():
    from backend.services.tarang.lifecycle import exclude_forward_test

    assert exclude_forward_test(1, "")["ok"] is False


def test_live_still_locked():
    os.environ.pop("TARANG_LIVE_ENABLED", None)
    from backend.services.tarang.live_control import live_send_allowed, tarang_live_enabled

    assert tarang_live_enabled() is False
    assert live_send_allowed() is False


def test_private_chat_id_empty_dict_not_linked(monkeypatch):
    from backend.services.tarang.alerts_telegram import private_chat_id

    monkeypatch.delenv("TARANG_TELEGRAM_CHAT_ID", raising=False)
    monkeypatch.delenv("TELEGRAM_TARANG_CHAT_ID", raising=False)
    db = MagicMock()
    with patch("backend.services.tarang.alerts_telegram._setting", return_value={}):
        assert private_chat_id(db) is None
    with patch("backend.services.tarang.alerts_telegram._setting", return_value=None):
        assert private_chat_id(db) is None
    with patch("backend.services.tarang.alerts_telegram._setting", return_value={"id": "555"}):
        assert private_chat_id(db) == "555"


def test_iv_vs_rv_missing_is_not_evaluable():
    from backend.services.tarang.gates import EVAL_NOT_EVALUABLE, aggregate_status, gate_iv_percentile, gate_iv_vs_rv

    g = gate_iv_vs_rv(0.28, None, 0.10)
    assert g.evaluability == EVAL_NOT_EVALUABLE
    assert g.passed is True
    assert "missing" in g.detail.lower()
    g2 = gate_iv_percentile(None, snapshot_count=80, min_percentile=50, min_snapshots=60)
    assert g2.evaluability == EVAL_NOT_EVALUABLE
    warm = gate_iv_percentile(40.0, snapshot_count=10, min_percentile=50, min_snapshots=60)
    assert warm.status_hint == "WARMING_UP"
    assert warm.passed
    assert aggregate_status([g, warm]) == "NOT_EVALUABLE"
    fail = gate_iv_vs_rv(0.31, 0.30, 0.10)
    assert fail.evaluability == "failed"
    assert aggregate_status([fail, g]) == "WATCHING"


def test_job_lock_key_stable():
    from backend.services.tarang.job_lock import _lock_key

    assert _lock_key("delta_snapshots") == _lock_key("delta_snapshots")
    assert _lock_key("a") != _lock_key("b")

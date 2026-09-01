"""Tests for Breakfast monitor preflight and post-session report."""
from __future__ import annotations

from datetime import datetime
from unittest.mock import patch

import pytz

from backend.services.breakfast_monitor import (
    assess_production_stability,
    build_post_session_report,
    collect_breakfast_preflight_status,
    format_preflight_telegram,
    _tick_source_breakdown,
)

IST = pytz.timezone("Asia/Kolkata")


def test_tick_source_breakdown_filters_minutes():
    rows = [
        {"minute": 16, "source": "ws", "reason": None},
        {"minute": 17, "source": "rest_fallback", "reason": "stale_ws"},
        {"minute": 15, "source": "ws", "reason": None},
    ]
    by_src, reasons = _tick_source_breakdown(rows)
    assert by_src == {"ws": 1, "rest_fallback": 1}
    assert reasons == {"stale_ws": 1}


@patch("backend.services.breakfast_monitor._backend_health_ok", return_value=True)
@patch("backend.services.breakfast_monitor.breakfast_prev_close_scheduler_status")
@patch("backend.services.breakfast_monitor.breakfast_live_scheduler_status")
@patch("backend.services.breakfast_monitor.get_last_warmup_result", return_value=None)
@patch("backend.services.breakfast_monitor._estimate_warmup_instruments", return_value=159)
def test_preflight_ok(mock_est, _warmup, mock_live, mock_prev, _health):
    mock_live.return_value = {
        "running": True,
        "job_ids": [
            "breakfast_ws_warmup_910",
            "breakfast_live_tick_16",
            "breakfast_live_tick_17",
            "breakfast_live_tick_18",
            "breakfast_live_tick_19",
            "breakfast_live_freeze_92005",
        ],
    }
    mock_prev.return_value = {
        "running": True,
        "job_ids": [
            "breakfast_prev_close_1600",
            "breakfast_prev_close_1630",
            "breakfast_prev_close_0905",
        ],
    }
    now = IST.localize(datetime(2026, 9, 1, 9, 0, 0))
    status = collect_breakfast_preflight_status(now=now)
    assert status["ok"]
    text = format_preflight_telegram(status)
    assert "159" in text
    assert "scheduled 09:10" in text
    assert "preflight ✓" in text


@patch("backend.services.breakfast_monitor.fetch_session_lock")
@patch("backend.services.breakfast_monitor.get_live_tick_snapshot")
@patch("backend.services.breakfast_monitor.get_breakfast_session_monitor_stats")
def test_post_session_report(mock_stats, mock_snap, mock_lock):
    mock_stats.return_value = {
        "tick_sources": [
            {"minute": 16, "source": "ws"},
            {"minute": 16, "source": "ws"},
            {"minute": 17, "source": "rest_fallback", "reason": "stale_ws"},
        ],
        "repicks": [{"minute": 17, "from": ["a"], "to": ["b"], "stocks": 3}],
    }
    mock_lock.return_value = {
        "lock_status": "locked",
        "signal_count": 6,
        "failure_reason": None,
        "payload_json": {"cross_check_status": "ws_rest:18/20_matched"},
    }
    mock_snap.return_value = None
    text = build_post_session_report("2026-09-01")
    assert "WS 6" in text  # 66% or 67%
    assert "locked" in text
    assert "Sector re-picks: 1" in text
    assert "ws_rest:18/20_matched" in text


def test_stability_assessment_locked_high_ws():
    verdict, _ = assess_production_stability(
        lock_row={"lock_status": "locked"},
        by_source={"ws": 90, "rest_fallback": 10},
        fallback_reasons={},
    )
    assert verdict == "OK"

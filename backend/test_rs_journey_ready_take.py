"""Unit tests for RS journey READY NOW / Take Trade annotation helpers."""
from datetime import datetime

import pytz

from backend.services.rs_journey_lookup import (
    _annotate_ready_take,
    _is_ready_family,
    _ready_take_summary,
)

IST = pytz.timezone("Asia/Kolkata")


def test_is_ready_family():
    assert _is_ready_family("READY")
    assert _is_ready_family("READY(RECHECK)")
    assert not _is_ready_family("WAIT")
    assert not _is_ready_family(None)


def test_ready_take_summary_episodes():
    events = [
        {
            "logged_at": IST.localize(datetime(2026, 8, 20, 10, 5, 0)),
            "logged_at_ist": "10:05:00",
            "ready_now": True,
            "take_trade_enabled": False,
        },
        {
            "logged_at": IST.localize(datetime(2026, 8, 20, 10, 10, 0)),
            "logged_at_ist": "10:10:00",
            "ready_now": True,
            "take_trade_enabled": True,
        },
        {
            "logged_at": IST.localize(datetime(2026, 8, 20, 11, 0, 0)),
            "logged_at_ist": "11:00:00",
            "ready_now": False,
            "take_trade_enabled": False,
        },
        {
            "logged_at": IST.localize(datetime(2026, 8, 20, 12, 0, 0)),
            "logged_at_ist": "12:00:00",
            "ready_now": True,
            "take_trade_enabled": True,
        },
    ]
    s = _ready_take_summary(events)
    assert s["first_ready_now_at_ist"] == "10:05:00"
    assert s["first_take_trade_at_ist"] == "10:10:00"
    assert s["ready_now_episodes"] == 2
    assert s["take_trade_episodes"] == 2
    assert s["ready_now_times_ist"] == ["10:05:00", "12:00:00"]


def test_annotate_ready_take_within_window():
    cps = [
        {
            "scan_time": IST.localize(datetime(2026, 8, 20, 10, 7, 0)).isoformat(),
            "scan_time_ist": "10:07:00",
        },
        {
            "scan_time": IST.localize(datetime(2026, 8, 20, 13, 0, 0)).isoformat(),
            "scan_time_ist": "13:00:00",
        },
    ]
    events = [
        {
            "logged_at": IST.localize(datetime(2026, 8, 20, 10, 5, 0)),
            "logged_at_ist": "10:05:00",
            "ready_now": True,
            "take_trade_enabled": True,
            "source": "consistency",
            "episode_start": None,
            "episode_end": None,
        }
    ]
    _annotate_ready_take(cps, events)
    assert cps[0]["ready_now"] is True
    assert cps[0]["take_trade_enabled"] is True
    assert cps[0]["ready_now_at_ist"] == "10:05:00"
    assert cps[1]["ready_now"] is False
    assert cps[1]["take_trade_enabled"] is False


def test_annotate_sq_promotion_episode_covers_later_scans():
    start = IST.localize(datetime(2026, 8, 20, 11, 5, 35))
    end = IST.localize(datetime(2026, 8, 20, 15, 30, 0))
    events = [
        {
            "logged_at": start,
            "logged_at_ist": "11:05:35",
            "ready_now": True,
            "take_trade_enabled": True,
            "source": "sq_promotion",
            "episode_start": start,
            "episode_end": end,
        }
    ]
    cps = [
        {"scan_time": IST.localize(datetime(2026, 8, 20, 10, 0, 0)).isoformat()},
        {"scan_time": IST.localize(datetime(2026, 8, 20, 11, 10, 0)).isoformat()},
        {"scan_time": IST.localize(datetime(2026, 8, 20, 14, 0, 0)).isoformat()},
    ]
    _annotate_ready_take(cps, events)
    assert cps[0]["ready_now"] is False
    assert cps[1]["ready_now"] is True
    assert cps[1]["take_trade_enabled"] is True
    assert cps[1]["ready_now_source"] == "sq_promotion"
    assert cps[2]["ready_now"] is True


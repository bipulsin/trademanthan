"""Tests for RS conviction board scheduling helpers."""
from backend.services.rs_conviction_board import (
    is_board_cycle_for_scheduled_minute,
    is_board_cycle_minute,
)


def test_board_cycle_minutes_match_rs_scan_schedule():
    cfg = {"board_cutoff_min": 15 * 60 + 15}
    assert is_board_cycle_for_scheduled_minute(9, 25, cfg)
    assert not is_board_cycle_for_scheduled_minute(9, 30, cfg)
    assert is_board_cycle_for_scheduled_minute(9, 35, cfg)
    assert is_board_cycle_for_scheduled_minute(10, 5, cfg)
    assert not is_board_cycle_for_scheduled_minute(9, 20, cfg)
    assert not is_board_cycle_for_scheduled_minute(15, 20, cfg)


def test_board_cycle_uses_scheduled_not_delayed_clock():
    """Job fired for :35 should qualify even if is_board_cycle_minute(now) would use :36."""
    from datetime import datetime

    import pytz

    ist = pytz.timezone("Asia/Kolkata")
    delayed = ist.localize(datetime(2026, 7, 3, 9, 36, 45))
    assert not is_board_cycle_minute(delayed)
    assert is_board_cycle_for_scheduled_minute(9, 35)

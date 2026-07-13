"""Tests for same-day re-entry block reason matching (both trade sources)."""
from backend.services.daily_checklist_chop_gates import exit_reason_blocks_reentry


def test_panel_ema10_blocks():
    assert exit_reason_blocks_reentry("EMA10 reverse close (rule)") is True


def test_panel_ema5_blocks():
    assert exit_reason_blocks_reentry("EMA5 reverse close (profit protection)") is True


def test_panel_risk_and_discretionary_block():
    assert exit_reason_blocks_reentry("Risk cap exceeded") is True
    assert exit_reason_blocks_reentry("Discretionary early exit") is True


def test_square_off_1515_does_not_block():
    assert exit_reason_blocks_reentry("15:15 square-off") is False


def test_daily_futures_sl_blocks():
    assert exit_reason_blocks_reentry("SL_HIT") is True
    assert exit_reason_blocks_reentry("TRAIL_STOP") is True


def test_state_machine_raw_reasons_block():
    assert exit_reason_blocks_reentry("EMA10 reverse close") is True
    assert exit_reason_blocks_reentry("EMA5 reverse close after profit protection") is True
    assert exit_reason_blocks_reentry("Risk cap exceeded before 1:2") is True

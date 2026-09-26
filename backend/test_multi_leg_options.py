"""Multi-leg journal rules: min legs, PnL sign, close gate, expiry roll, partial exit."""
from datetime import date

import pytest

from backend.services.multi_leg_options import (
    MultiLegValidationError,
    assert_min_legs,
    assert_ready_to_close,
    close_block_reason,
    direction_sign,
    leg_pnl,
    next_monthly_option_expiry,
    status_after_save,
    trade_pnl,
)


def test_min_legs_straddle_iron_fly_condor():
    assert_min_legs("STRADDLE", 2)
    assert_min_legs("IRON_FLY", 4)
    assert_min_legs("IRON_CONDOR", 4)
    assert_min_legs("IRON_CONDOR", 6)
    with pytest.raises(MultiLegValidationError, match="at least 2"):
        assert_min_legs("STRADDLE", 1)
    with pytest.raises(MultiLegValidationError, match="at least 4"):
        assert_min_legs("IRON_FLY", 3)
    with pytest.raises(MultiLegValidationError, match="at least 4"):
        assert_min_legs("iron_condor", 2)
    with pytest.raises(MultiLegValidationError, match="trade_type"):
        assert_min_legs("SPREAD", 4)


def test_pnl_sign_long_profits_when_price_rises_short_when_price_falls():
    # direction_sign: BUY +1, SELL -1.
    assert direction_sign("BUY") == 1
    assert direction_sign("SELL") == -1
    # Long: (ltp - entry) × lot.
    assert leg_pnl("BUY", entry_price=10, mark=12, lot_size=50) == 100
    # Short: (entry - ltp) × lot, i.e. (mark - entry) × -1 × lot.
    assert leg_pnl("SELL", entry_price=10, mark=8, lot_size=50) == 100
    assert leg_pnl("SELL", entry_price=10, mark=12, lot_size=50) == -100
    assert trade_pnl([100, -40]) == 60
    assert leg_pnl("BUY", entry_price=10, mark=None, lot_size=50) is None


def test_close_rejected_when_a_leg_lacks_exit():
    partial = [
        {"exit_price": 12.5, "exit_time": "2026-09-26T15:30:00"},
        {"exit_price": None, "exit_time": None},
    ]
    assert close_block_reason(partial)
    with pytest.raises(MultiLegValidationError, match="exit_price and exit_time"):
        assert_ready_to_close(partial)
    one_field = [
        {"exit_price": 12.5, "exit_time": None},
        {"exit_price": 1, "exit_time": "2026-09-26T15:30:00"},
    ]
    with pytest.raises(MultiLegValidationError):
        assert_ready_to_close(one_field)
    complete = [
        {"exit_price": 12.5, "exit_time": "2026-09-26T15:30:00"},
        {"exit_price": 4, "exit_time": "2026-09-26T15:31:00"},
    ]
    assert close_block_reason(complete) is None
    assert_ready_to_close(complete)


def test_next_month_expiry_when_entry_is_after_this_months_expiry():
    # 29 Sep 2026 is the last Tuesday. Entry on the 26th stays in September.
    assert next_monthly_option_expiry(date(2026, 9, 26), "NIFTY") == date(2026, 9, 29)
    assert next_monthly_option_expiry(date(2026, 9, 26), "BANKNIFTY") == date(2026, 9, 29)
    # SENSEX follows the same last-Tuesday helper (no separate BSE calendar in repo).
    assert next_monthly_option_expiry(date(2026, 9, 26), "SENSEX") == date(2026, 9, 29)
    # 30 Sep is after that Tuesday, so October's last Tuesday (27 Oct 2026).
    assert next_monthly_option_expiry(date(2026, 9, 30), "NIFTY") == date(2026, 10, 27)
    # 31 Mar 2026 is a Tuesday and an NSE holiday, so March expiry is 30 Mar.
    assert next_monthly_option_expiry(date(2026, 3, 1), "NIFTY") == date(2026, 3, 30)
    # MCX fallback is last Thursday (24 Sep 2026). 26 Sep has already passed it.
    assert next_monthly_option_expiry(date(2026, 9, 24), "CRUDEOIL") == date(2026, 9, 24)
    assert next_monthly_option_expiry(date(2026, 9, 26), "CRUDEOIL") == date(2026, 10, 29)


def test_partial_exit_keeps_trade_active():
    partial = [
        {"exit_price": 12.5, "exit_time": "2026-09-26T15:30:00"},
        {"exit_price": None, "exit_time": None},
        {"exit_price": 3, "exit_time": "2026-09-26T15:30:00"},
        {"exit_price": None, "exit_time": None},
    ]
    assert status_after_save(closing=False, legs=partial) == "ACTIVE"
    with pytest.raises(MultiLegValidationError):
        status_after_save(closing=True, legs=partial)
    complete = [
        {"exit_price": 12.5, "exit_time": "2026-09-26T15:30:00"},
        {"exit_price": 1.2, "exit_time": "2026-09-26T15:30:00"},
    ]
    # Saving still leaves the trade ACTIVE until the close endpoint.
    assert status_after_save(closing=False, legs=complete) == "ACTIVE"
    assert status_after_save(closing=True, legs=complete) == "CLOSED"

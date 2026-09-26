"""Multi-leg journal rules: min legs, PnL sign, close gate, expiry roll, partial exit."""
from datetime import date

import pytest

from backend.services.multi_leg_options import (
    MultiLegValidationError,
    _parse_dt,
    assert_min_legs,
    assert_ready_to_close,
    assign_missing_trade_numbers,
    close_block_reason,
    direction_sign,
    leg_pnl,
    next_monthly_option_expiry,
    next_trade_number,
    parse_clock_ampm,
    status_after_save,
    suggested_expiry,
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


def test_suggested_expiry_skips_two_months_under_40_dte():
    # 26 Sep 2026. Last Tuesday 29 Sep is 3 DTE and 27 Oct is 31 DTE, both under 40.
    # November's last Tuesday is 24 Nov, an NSE holiday, so the helper uses 23 Nov (58 DTE).
    as_of = date(2026, 9, 26)
    assert (date(2026, 9, 29) - as_of).days < 40
    assert (date(2026, 10, 27) - as_of).days < 40
    assert suggested_expiry(as_of, "NIFTY", as_of=as_of) == date(2026, 11, 23)
    assert suggested_expiry(as_of, "BANKNIFTY", as_of=as_of) == date(2026, 11, 23)
    assert suggested_expiry(as_of, "SENSEX", as_of=as_of) == date(2026, 11, 23)
    assert suggested_expiry(as_of, "RELIANCE", as_of=as_of) == date(2026, 11, 23)


def test_mcx_suggested_expiry_takes_soonest_listed_with_40_dte(monkeypatch):
    as_of = date(2026, 9, 26)
    monkeypatch.setattr(
        "backend.services.multi_leg_options.listed_option_expiries",
        lambda instrument, on_or_after=None: [
            date(2026, 10, 15),
            date(2026, 11, 17),
            date(2026, 12, 15),
        ],
    )
    assert suggested_expiry(as_of, "CRUDEOIL", as_of=as_of) == date(2026, 11, 17)


def test_mcx_suggested_expiry_steps_monthly_when_master_is_short(monkeypatch):
    as_of = date(2026, 9, 26)
    monkeypatch.setattr(
        "backend.services.multi_leg_options.listed_option_expiries",
        lambda instrument, on_or_after=None: [date(2026, 10, 15)],
    )
    assert suggested_expiry(as_of, "CRUDEOIL", as_of=as_of) == date(2026, 11, 26)


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


def test_trade_numbers_are_stable_integers():
    rows = [
        {"id": "a", "created_at": "2026-01-03T00:00:00", "trade_no": None},
        {"id": "b", "created_at": "2026-01-01T00:00:00", "trade_no": None},
        {"id": "c", "created_at": "2026-01-02T00:00:00", "trade_no": None},
    ]
    assert assign_missing_trade_numbers(rows) == {"b": 1, "c": 2, "a": 3}
    # Deleting the middle trade must not renumber the ones that remain.
    kept = assign_missing_trade_numbers([
        {"id": "b", "created_at": "2026-01-01T00:00:00", "trade_no": 1},
        {"id": "a", "created_at": "2026-01-03T00:00:00", "trade_no": 3},
    ])
    assert kept == {"b": 1, "a": 3}
    assert next_trade_number(3) == 4
    assert next_trade_number(None) == 1
    again = assign_missing_trade_numbers([
        {"id": "b", "created_at": "2026-01-01T00:00:00", "trade_no": 1},
        {"id": "a", "created_at": "2026-01-03T00:00:00", "trade_no": 3},
        {"id": "d", "created_at": "2026-01-04T00:00:00", "trade_no": None},
    ])
    assert again["b"] == 1
    assert again["a"] == 3
    assert again["d"] == 4


def test_parse_clock_03_10_pm():
    assert parse_clock_ampm("03:10 PM") == (15, 10)
    assert parse_clock_ampm("03:10 AM") == (3, 10)
    assert parse_clock_ampm("12:00 AM") == (0, 0)
    assert parse_clock_ampm("12:00 PM") == (12, 0)
    saved = _parse_dt("2026-09-17T03:10 PM")
    assert saved is not None
    assert saved.hour == 15
    assert saved.minute == 10
    assert saved.date().isoformat() == "2026-09-17"
    plain = _parse_dt("2026-09-17T15:10")
    assert plain is not None
    assert plain.hour == 15 and plain.minute == 10

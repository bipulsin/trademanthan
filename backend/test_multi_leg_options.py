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


class _Fetch:
    def __init__(self, row):
        self._row = row

    def fetchone(self):
        return self._row


class _RecordingDB:
    def __init__(self):
        self.statements = []

    def execute(self, stmt, params=None):
        sql = str(stmt)
        self.statements.append((sql, dict(params or {})))
        if "MAX(trade_no)" in sql:
            return _Fetch((None,))
        return _Fetch(None)

    def commit(self):
        return None

    def rollback(self):
        return None

    def close(self):
        return None


def _leg(right):
    return {
        "side": "SELL",
        "option_type": right,
        "strike_price": 25000,
        "entry_price": 100,
        "entry_time": "2026-09-26T09:20:00+05:30",
        "leg_expiry_date": "2026-11-24",
    }


def _trade_body(**extra):
    body = {
        "trade_type": "STRADDLE",
        "instrument": "NIFTY",
        "spot_price_entry": 25000,
        "entry_date": "2026-09-26",
        "expiry_date": "2026-11-24",
        "legs": [_leg("CE"), _leg("PE")],
    }
    body.update(extra)
    return body


def _bind_multi_leg_db(monkeypatch, db, stored=None):
    from backend.services import multi_leg_options as mlo

    monkeypatch.setattr(mlo, "ensure_multi_leg_tables", lambda: None)
    monkeypatch.setattr(mlo, "SessionLocal", lambda: db)
    monkeypatch.setattr(
        mlo,
        "resolve_option_contract",
        lambda instrument, strike, option_type, expiry: {
            "lot_size": 75,
            "instrument_key": "NSE_FO|NIFTY",
        },
    )
    monkeypatch.setattr(mlo, "get_trade", lambda trade_id: stored if stored is not None else {"id": trade_id})
    monkeypatch.setattr(
        "backend.services.multi_leg_ws_ltp.sync_subscriptions",
        lambda **kwargs: None,
    )
    return mlo


def _sql_params(db, marker):
    matches = [params for sql, params in db.statements if marker in sql]
    assert matches, marker
    return matches[0], next(sql for sql, params in db.statements if marker in sql)


def test_create_stores_max_profit(monkeypatch):
    db = _RecordingDB()
    mlo = _bind_multi_leg_db(monkeypatch, db)
    mlo.create_trade(_trade_body(max_profit=1234.5))
    params, sql = _sql_params(db, "INSERT INTO multi_leg_trades")
    assert "max_profit" in sql
    assert params["max_profit"] == 1234.5

    empty = _RecordingDB()
    mlo = _bind_multi_leg_db(monkeypatch, empty)
    mlo.create_trade(_trade_body(max_profit=""))
    params, _sql = _sql_params(empty, "INSERT INTO multi_leg_trades")
    assert params["max_profit"] is None


def test_edit_updates_max_profit(monkeypatch):
    stored = _trade_body(max_profit=1000)
    stored["id"] = "11111111-1111-1111-1111-111111111111"
    stored["status"] = "ACTIVE"
    db = _RecordingDB()
    mlo = _bind_multi_leg_db(monkeypatch, db, stored)
    mlo.update_trade(stored["id"], _trade_body(max_profit=2500))
    params, sql = _sql_params(db, "UPDATE multi_leg_trades SET")
    assert "max_profit = :max_profit" in sql
    assert params["max_profit"] == 2500


def test_save_omitting_max_profit_keeps_previous_value(monkeypatch):
    stored = _trade_body(max_profit=1800)
    stored["id"] = "22222222-2222-2222-2222-222222222222"
    stored["status"] = "ACTIVE"
    db = _RecordingDB()
    mlo = _bind_multi_leg_db(monkeypatch, db, stored)
    body = _trade_body()
    assert "max_profit" not in body
    mlo.update_trade(stored["id"], body)
    params, sql = _sql_params(db, "UPDATE multi_leg_trades SET")
    assert "max_profit" not in sql
    assert "max_profit" not in params


def test_close_without_max_profit_does_not_clear_it(monkeypatch):
    ce = _leg("CE")
    pe = _leg("PE")
    ce["exit_price"] = 10
    ce["exit_time"] = "2026-09-26T15:30:00+05:30"
    pe["exit_price"] = 8
    pe["exit_time"] = "2026-09-26T15:30:00+05:30"
    stored = _trade_body(max_profit=1800, legs=[ce, pe])
    stored["id"] = "33333333-3333-3333-3333-333333333333"
    stored["status"] = "ACTIVE"
    db = _RecordingDB()
    mlo = _bind_multi_leg_db(monkeypatch, db, stored)
    mlo.close_trade(stored["id"], {"legs": [ce, pe]})
    updates = [(sql, params) for sql, params in db.statements if "UPDATE multi_leg_trades SET" in sql]
    assert updates
    for sql, params in updates:
        if "max_profit" in sql:
            assert params["max_profit"] == 1800
        else:
            assert "max_profit" not in params

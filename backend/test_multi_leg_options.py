"""Multi-leg journal rules: min legs, PnL sign, close gate, expiry roll, partial exit."""
from datetime import date, datetime, timezone

import pytest

from backend.services.multi_leg_options import (
    IST,
    MultiLegValidationError,
    _parse_dt,
    adjustment_alert_text,
    annotate_adjustment_alerts,
    assert_min_legs,
    assert_ready_to_close,
    assign_missing_trade_numbers,
    close_block_reason,
    default_leg_slots,
    direction_sign,
    exit_adjustment_due,
    green_zone_adjustment,
    leg_pnl,
    next_monthly_option_expiry,
    next_trade_number,
    parse_clock_ampm,
    scope_alert_to_first_ist_day,
    scope_alert_to_first_trigger_window,
    status_after_save,
    straddle_adjustment_due,
    straddle_ltp_ratio,
    straddle_ratio_leg,
    resolve_future_instrument_key,
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


def test_leg_exit_via_update_keeps_active_and_counts_realized_pnl(monkeypatch):
    ce = _leg("CE")
    pe = _leg("PE")
    ce["id"] = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    pe["id"] = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
    ce["lot_size"] = 75
    pe["lot_size"] = 75
    pe["ltp"] = 90
    stored = _trade_body(legs=[ce, pe])
    stored["id"] = "cccccccc-cccc-cccc-cccc-cccccccccccc"
    stored["status"] = "ACTIVE"
    db = _RecordingDB()
    mlo = _bind_multi_leg_db(monkeypatch, db, stored)
    body = _trade_body(legs=[
        {
            **ce,
            "exit_price": 80,
            "exit_time": "2026-09-26T14:00:00+05:30",
        },
        pe,
    ])
    mlo.update_trade(stored["id"], body)
    params, sql = _sql_params(db, "UPDATE multi_leg_trades SET")
    assert params["status"] == "ACTIVE"
    # CE short exit 80 from 100 → +20 × 75 = 1500; PE open at LTP 90 → +10 × 75 = 750.
    assert params["pnl"] == 2250.0
    leg_params = [p for s, p in db.statements if "INSERT INTO multi_leg_trade_legs" in s]
    assert leg_params[0]["exit_price"] == 80.0
    assert leg_params[0]["exit_time"] is not None
    assert leg_params[1]["exit_price"] is None


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


def _leg(right, qualifier="MAIN"):
    return {
        "qualifier": qualifier,
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


def test_default_qualifier_iron_fly_and_straddle():
    fly = default_leg_slots("IRON_FLY")
    assert fly == [
        {"qualifier": "MAIN", "option_type": "CE"},
        {"qualifier": "MAIN", "option_type": "PE"},
        {"qualifier": "WING", "option_type": "CE"},
        {"qualifier": "WING", "option_type": "PE"},
    ]
    assert len(fly) == 4
    assert default_leg_slots("IRON_CONDOR") == fly
    straddle = default_leg_slots("STRADDLE")
    assert straddle == [
        {"qualifier": "MAIN", "option_type": "CE"},
        {"qualifier": "MAIN", "option_type": "PE"},
    ]
    assert len(straddle) == 2
    assert default_leg_slots("UNCLASSIFIED") == []


def _ratio_leg(option_type, qualifier, ltp, entry_time, *, exited=False, leg_id=None, entry=10.0):
    row = {
        "id": leg_id or f"{qualifier}-{option_type}-{entry_time}",
        "qualifier": qualifier,
        "option_type": option_type,
        "side": "SELL",
        "entry_price": entry,
        "entry_time": entry_time,
        "ltp": ltp,
        "lot_size": 50,
        "exit_price": None,
        "exit_time": None,
    }
    if exited:
        row["exit_price"] = ltp
        row["exit_time"] = "2026-10-04T12:00:00+05:30"
    return row


def test_straddle_ltp_ratio_threshold_and_pair_rules():
    # Exactly 3.0 triggers; 2.9 does not.
    at_three = [
        _ratio_leg("CE", "MAIN", 30.0, "2026-10-04T09:20:00+05:30"),
        _ratio_leg("PE", "MAIN", 10.0, "2026-10-04T09:20:00+05:30"),
    ]
    assert straddle_ltp_ratio(at_three) == 3.0
    assert straddle_adjustment_due(at_three) is True
    under = [
        _ratio_leg("CE", "MAIN", 29.0, "2026-10-04T09:20:00+05:30"),
        _ratio_leg("PE", "MAIN", 10.0, "2026-10-04T09:20:00+05:30"),
    ]
    assert straddle_ltp_ratio(under) == 2.9
    assert straddle_adjustment_due(under) is False

    # Exited legs and Wings are excluded from the ratio pair.
    mixed = [
        _ratio_leg("CE", "MAIN", 100.0, "2026-10-04T09:15:00+05:30", exited=True, leg_id="ce-old"),
        _ratio_leg("CE", "WING", 5.0, "2026-10-04T09:30:00+05:30", leg_id="ce-wing"),
        _ratio_leg("CE", "MAIN", 40.0, "2026-10-04T09:25:00+05:30", leg_id="ce-main"),
        _ratio_leg("PE", "MAIN", 10.0, "2026-10-04T09:20:00+05:30", leg_id="pe-main"),
    ]
    assert straddle_ratio_leg(mixed, "CE")["id"] == "ce-main"
    assert straddle_ltp_ratio(mixed) == 4.0

    # Newest Adj CE/PE preferred over older Main on that side.
    with_adj = [
        _ratio_leg("CE", "MAIN", 12.0, "2026-10-04T09:15:00+05:30", leg_id="ce-main"),
        _ratio_leg("PE", "MAIN", 12.0, "2026-10-04T09:15:00+05:30", leg_id="pe-main"),
        _ratio_leg("CE", "ADJ", 45.0, "2026-10-04T10:00:00+05:30", leg_id="ce-adj"),
        _ratio_leg("PE", "ADJ", 15.0, "2026-10-04T10:01:00+05:30", leg_id="pe-adj"),
    ]
    assert straddle_ratio_leg(with_adj, "CE")["id"] == "ce-adj"
    assert straddle_ratio_leg(with_adj, "PE")["id"] == "pe-adj"
    assert straddle_ltp_ratio(with_adj) == 3.0
    assert straddle_adjustment_due(with_adj) is True

    # Missing or zero LTP skips the ratio.
    assert straddle_ltp_ratio([
        _ratio_leg("CE", "MAIN", 0, "2026-10-04T09:20:00+05:30"),
        _ratio_leg("PE", "MAIN", 10.0, "2026-10-04T09:20:00+05:30"),
    ]) is None
    assert straddle_ltp_ratio([
        _ratio_leg("CE", "MAIN", None, "2026-10-04T09:20:00+05:30"),
        _ratio_leg("PE", "MAIN", 10.0, "2026-10-04T09:20:00+05:30"),
    ]) is None

    # Sync / legacy rows with blank qualifier still form a Main CE/PE pair.
    blank_q = [
        _ratio_leg("CE", None, 511.0, "2026-10-05T09:43:00+05:30", leg_id="ce-sync"),
        _ratio_leg("PE", "", 377.6, "2026-10-05T09:43:00+05:30", leg_id="pe-sync"),
    ]
    assert straddle_ratio_leg(blank_q, "CE")["id"] == "ce-sync"
    assert straddle_ratio_leg(blank_q, "PE")["id"] == "pe-sync"
    assert abs(straddle_ltp_ratio(blank_q) - (511.0 / 377.6)) < 1e-9
    assert straddle_adjustment_due(blank_q) is False

    annotated = annotate_adjustment_alerts(
        [{
            "status": "ACTIVE",
            "trade_type": "STRADDLE",
            "instrument": "NIFTY",
            "legs": at_three,
        }],
        spots={},
        today=date(2026, 10, 6),
        persist=False,
    )
    assert annotated[0]["adjustment_alert"] is True
    assert annotated[0]["ltp_ratio"] == 3.0
    assert annotated[0]["adjustment_message"] == "Adjustment on Straddle"
    assert annotated[0]["adjustment_ticker_text"] == "Adjustment"
    assert annotated[0]["adj_alert_first_date"] == "2026-10-06"


def test_adjustment_alert_24h_first_trigger_window():
    # First fire stores now and shows; age >= 24h suppresses until clear + re-trigger.
    t0 = IST.localize(datetime(2026, 10, 6, 10, 0, 0))
    show, stored, dirty = scope_alert_to_first_trigger_window(True, None, now=t0)
    assert show is True and stored == t0 and dirty is True
    show, stored, dirty = scope_alert_to_first_trigger_window(True, t0, now=t0)
    assert show is True and stored == t0 and dirty is False
    # Still same IST day, but 24h elapsed → suppress (date-only was not enough).
    same_day_later = IST.localize(datetime(2026, 10, 6, 23, 30, 0))
    early = IST.localize(datetime(2026, 10, 5, 22, 0, 0))
    show, stored, dirty = scope_alert_to_first_trigger_window(
        True, early, now=same_day_later
    )
    assert show is False and stored == early and dirty is False
    # Next calendar day with age still under 24h stays visible.
    next_morning = IST.localize(datetime(2026, 10, 7, 9, 0, 0))
    show, stored, dirty = scope_alert_to_first_trigger_window(True, t0, now=next_morning)
    assert show is True and stored == t0 and dirty is False
    # Past 24h from t0.
    after = IST.localize(datetime(2026, 10, 7, 10, 0, 0))
    show, stored, dirty = scope_alert_to_first_trigger_window(True, t0, now=after)
    assert show is False and stored == t0 and dirty is False
    # Condition clear wipes the stamp so a later breach is a new generation.
    show, stored, dirty = scope_alert_to_first_trigger_window(False, t0, now=after)
    assert show is False and stored is None and dirty is True
    show, stored, dirty = scope_alert_to_first_trigger_window(
        True, None, now=IST.localize(datetime(2026, 10, 8, 11, 0, 0))
    )
    assert show is True and dirty is True

    # Legacy date-only column: IST midnight + 24h (compat wrapper).
    show, day, dirty = scope_alert_to_first_ist_day(
        True, date(2026, 10, 6), today=date(2026, 10, 6)
    )
    assert show is True and day == date(2026, 10, 6) and dirty is True
    show, day, dirty = scope_alert_to_first_ist_day(
        True, date(2026, 10, 6), today=date(2026, 10, 7)
    )
    assert show is False and day == date(2026, 10, 6)

    legs = [
        _ratio_leg("CE", "MAIN", 30.0, "2026-10-04T09:20:00+05:30"),
        _ratio_leg("PE", "MAIN", 10.0, "2026-10-04T09:20:00+05:30"),
    ]
    stale = annotate_adjustment_alerts(
        [{
            "id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            "status": "ACTIVE",
            "trade_type": "STRADDLE",
            "instrument": "NIFTY",
            "adj_alert_first_date": "2026-10-05",
            "legs": legs,
        }],
        spots={},
        today=date(2026, 10, 6),
        persist=False,
    )
    assert stale[0]["adjustment_alert"] is False
    assert stale[0]["adjustment_message"] == ""
    assert stale[0]["adjustment_ticker_text"] == ""
    assert stale[0]["adj_alert_first_date"] == "2026-10-05"
    assert stale[0]["ltp_ratio"] == 3.0

    # Age >= 24h suppresses even though first_date would still be "today" under date-only.
    expired_24h = annotate_adjustment_alerts(
        [{
            "id": "bbbbbbbb-bbbb-cccc-dddd-eeeeeeeeeeee",
            "status": "ACTIVE",
            "trade_type": "IRON_FLY",
            "instrument": "NIFTY",
            "green_zone_ce": 25000,
            "green_zone_pe": 24000,
            "adj_alert_first_date": "2026-10-06",
            "adj_alert_first_triggered_at": "2026-10-06T09:00:00+05:30",
            "legs": [
                {"qualifier": "MAIN", "option_type": "PE", "strike_price": 24500},
                {"qualifier": "MAIN", "option_type": "CE", "strike_price": 24800},
            ],
        }],
        spots={"NIFTY": 25100.0},
        now=IST.localize(datetime(2026, 10, 7, 9, 0, 0)),
        persist=False,
    )
    assert expired_24h[0]["adjustment_alert"] is False
    assert expired_24h[0]["adjustment_message"] == ""
    assert expired_24h[0]["adj_alert_first_triggered_at"] == "2026-10-06T09:00:00+05:30"
    assert expired_24h[0]["adj_alert_first_date"] == "2026-10-06"

    # Still inside the 24h window on the same day.
    live_same_day = annotate_adjustment_alerts(
        [{
            "status": "ACTIVE",
            "trade_type": "IRON_FLY",
            "instrument": "NIFTY",
            "green_zone_ce": 25000,
            "green_zone_pe": 24000,
            "adj_alert_first_triggered_at": "2026-10-06T09:00:00+05:30",
            "legs": [
                {"qualifier": "MAIN", "option_type": "PE", "strike_price": 24500},
                {"qualifier": "MAIN", "option_type": "CE", "strike_price": 24800},
            ],
        }],
        spots={"NIFTY": 25100.0},
        now=IST.localize(datetime(2026, 10, 6, 20, 0, 0)),
        persist=False,
    )
    assert live_same_day[0]["adjustment_alert"] is True
    assert "Adjustment" in live_same_day[0]["adjustment_message"]

    cleared = annotate_adjustment_alerts(
        [{
            "status": "ACTIVE",
            "trade_type": "STRADDLE",
            "instrument": "NIFTY",
            "adj_alert_first_date": "2026-10-05",
            "adj_alert_first_triggered_at": "2026-10-05T09:00:00+05:30",
            "legs": [
                _ratio_leg("CE", "MAIN", 20.0, "2026-10-04T09:20:00+05:30"),
                _ratio_leg("PE", "MAIN", 10.0, "2026-10-04T09:20:00+05:30"),
            ],
        }],
        spots={},
        today=date(2026, 10, 6),
        persist=False,
    )
    assert cleared[0]["adjustment_alert"] is False
    assert cleared[0]["adj_alert_first_date"] is None
    assert cleared[0]["adj_alert_first_triggered_at"] is None

    iron = annotate_adjustment_alerts(
        [{
            "status": "ACTIVE",
            "trade_type": "IRON_FLY",
            "instrument": "NIFTY",
            "green_zone_ce": 25000,
            "green_zone_pe": 24000,
            "exit_adj_alert_first_date": "2026-10-05",
            "legs": [
                {"qualifier": "MAIN", "option_type": "PE", "strike_price": 24500},
                {"qualifier": "MAIN", "option_type": "CE", "strike_price": 24800},
                {"qualifier": "ADJ", "option_type": "CE", "strike_price": 24600},
            ],
        }],
        spots={"NIFTY": 25100.0},
        today=date(2026, 10, 6),
        persist=False,
    )
    # Green-zone adj is a new generation today; exit adj was first true yesterday.
    assert iron[0]["adjustment_alert"] is True
    assert iron[0]["adj_alert_first_date"] == "2026-10-06"
    assert iron[0]["exit_adjustment_alert"] is False
    assert iron[0]["exit_adj_alert_first_date"] == "2026-10-05"
    assert iron[0]["exit_adjustment_message"] == ""


def test_green_zone_spot_and_exit_adjustment():
    # Spot above green CE + 50. The band edge itself does not fire.
    assert green_zone_adjustment(25051, 25000, 24000) is True
    assert green_zone_adjustment(25050, 25000, 24000) is False
    # Spot between the bands does not.
    assert green_zone_adjustment(24500, 25000, 24000) is False
    assert green_zone_adjustment(23949, 25000, 24000) is True
    assert green_zone_adjustment(26000, None, 24000) is False
    assert adjustment_alert_text("IRON_FLY", "NIFTY") == "IronFly - NIFTY Adjustment"
    assert adjustment_alert_text("IRON_FLY", "NIFTY", exit=True) == "IronFly - NIFTY Exit Adjustment"

    legs = [
        {"qualifier": "MAIN", "option_type": "PE", "strike_price": 24500},
        {"qualifier": "MAIN", "option_type": "CE", "strike_price": 24800},
        {"qualifier": "ADJ", "option_type": "CE", "strike_price": 24600},
    ]
    assert exit_adjustment_due(24501, legs) is True
    assert exit_adjustment_due(24500, legs) is False
    closed = dict(legs[2])
    closed["exit_price"] = 1
    closed["exit_time"] = "2026-09-26T15:30:00+05:30"
    assert exit_adjustment_due(25000, [legs[0], legs[1], closed]) is False
    pe_adj = [
        {"qualifier": "MAIN", "option_type": "CE", "strike_price": 24800},
        {"qualifier": "ADJ", "option_type": "PE", "strike_price": 24000},
    ]
    assert exit_adjustment_due(24799, pe_adj) is True
    assert exit_adjustment_due(24800, pe_adj) is False


def test_create_requires_qualifier_and_stores_green_zones(monkeypatch):
    db = _RecordingDB()
    mlo = _bind_multi_leg_db(monkeypatch, db)
    missing = _trade_body()
    for leg in missing["legs"]:
        leg.pop("qualifier")
    with pytest.raises(MultiLegValidationError, match="qualifier"):
        mlo.create_trade(missing)

    saved = _RecordingDB()
    mlo = _bind_multi_leg_db(monkeypatch, saved)
    mlo.create_trade(_trade_body(green_zone_ce=25000, green_zone_pe=24000, legs=[
        _leg("CE", "MAIN"),
        _leg("PE", "MAIN"),
    ]))
    params, sql = _sql_params(saved, "INSERT INTO multi_leg_trades")
    assert params["green_zone_ce"] == 25000
    assert params["green_zone_pe"] == 24000
    assert "qualifier" in sql or any(
        "qualifier" in row_sql and row_params.get("qualifier") == "MAIN"
        for row_sql, row_params in saved.statements
    )
    leg_params = [p for s, p in saved.statements if "INSERT INTO multi_leg_trade_legs" in s]
    assert [p["qualifier"] for p in leg_params] == ["MAIN", "MAIN"]


def test_save_omitting_green_zones_keeps_previous_value(monkeypatch):
    stored = _trade_body(green_zone_ce=25000, green_zone_pe=24000)
    stored["id"] = "44444444-4444-4444-4444-444444444444"
    stored["status"] = "ACTIVE"
    stored["legs"][0]["id"] = "55555555-5555-5555-5555-555555555555"
    stored["legs"][1]["id"] = "66666666-6666-6666-6666-666666666666"
    stored["legs"][0]["qualifier"] = "MAIN"
    db = _RecordingDB()
    mlo = _bind_multi_leg_db(monkeypatch, db, stored)
    body = _trade_body()
    body["legs"][0]["id"] = stored["legs"][0]["id"]
    body["legs"][1]["id"] = stored["legs"][1]["id"]
    body["legs"][0]["qualifier"] = None
    assert "green_zone_ce" not in body
    mlo.update_trade(stored["id"], body)
    params, sql = _sql_params(db, "UPDATE multi_leg_trades SET")
    assert "green_zone_ce" not in sql
    assert "green_zone_pe" not in sql
    leg_params = [p for s, p in db.statements if "INSERT INTO multi_leg_trade_legs" in s]
    assert leg_params[0]["qualifier"] == "MAIN"


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


def _fut_row(und, key, y, m, d, kind="FUT", segment="NSE_FO", weekly=False):
    exp_ms = int(datetime(y, m, d, 10, 0, tzinfo=timezone.utc).timestamp() * 1000)
    return {
        "instrument_type": kind,
        "segment": segment,
        "weekly": weekly,
        "underlying_symbol": und,
        "instrument_key": key,
        "trading_symbol": und + " FUT",
        "expiry": exp_ms,
    }


def test_future_key_is_same_expiry_month_not_the_option():
    rows = [
        _fut_row("NIFTY", "NSE_FO|NIFTY26OCTFUT", 2026, 10, 27),
        _fut_row("NIFTY", "NSE_FO|NIFTY26NOVFUT", 2026, 11, 24),
        _fut_row("BANKNIFTY", "NSE_FO|BANKNIFTY26OCTFUT", 2026, 10, 27),
        _fut_row("NIFTY", "NSE_FO|NIFTY26OCT23500CE", 2026, 10, 27, kind="CE"),
    ]
    assert resolve_future_instrument_key("NIFTY", date(2026, 10, 27), rows=rows) == "NSE_FO|NIFTY26OCTFUT"
    assert resolve_future_instrument_key("NIFTY", "2026-10-27", rows=rows) == "NSE_FO|NIFTY26OCTFUT"
    assert resolve_future_instrument_key("BANKNIFTY", date(2026, 10, 27), rows=rows) == "NSE_FO|BANKNIFTY26OCTFUT"
    assert resolve_future_instrument_key("NIFTY", date(2026, 12, 29), rows=rows) is None

    same_month = [
        _fut_row("NIFTY", "NSE_FO|NIFTY26OCTFUT", 2026, 10, 28),
        _fut_row("NIFTY", "NSE_FO|NIFTY26NOVFUT", 2026, 11, 24),
    ]
    assert resolve_future_instrument_key("NIFTY", date(2026, 10, 27), rows=same_month) == "NSE_FO|NIFTY26OCTFUT"

    exact_over_other = [
        _fut_row("NIFTY", "NSE_FO|EARLY", 2026, 10, 8),
        _fut_row("NIFTY", "NSE_FO|EXACT", 2026, 10, 27),
    ]
    assert resolve_future_instrument_key("NIFTY", date(2026, 10, 27), rows=exact_over_other) == "NSE_FO|EXACT"

    weekly_only = [_fut_row("NIFTY", "NSE_FO|WEEKLY", 2026, 10, 27, weekly=True)]
    assert resolve_future_instrument_key("NIFTY", date(2026, 10, 27), rows=weekly_only) is None

    mcx = [_fut_row("SILVERM", "MCX_FO|SILVERM", 2026, 11, 30, segment="MCX_FO")]
    assert resolve_future_instrument_key("SILVERMINI", date(2026, 11, 30), rows=mcx) == "MCX_FO|SILVERM"

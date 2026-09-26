"""Upstox multi-leg import: dedupe, grouping, classification, orphans, assign."""
from datetime import date, datetime, timedelta

import pytz
import pytest

from backend.services.multi_leg_options import MultiLegValidationError
from backend.services.multi_leg_upstox_sync import (
    GROUP_WINDOW,
    assign_orphan,
    build_contract_index,
    build_sync_plan,
    classify_leg_count,
    group_new_legs,
)

IST = pytz.timezone("Asia/Kolkata")
EXPIRY = date(2026, 9, 29)
TODAY = date(2026, 9, 26)
LEG_ID = "11111111-1111-1111-1111-111111111111"
TRADE_ID = "22222222-2222-2222-2222-222222222222"


def _stamp(minute, second=0):
    return datetime(2026, 9, 26, 10, minute, second)


def _contract(key, right, strike, lot=75, instrument="NIFTY", expiry=EXPIRY):
    return {
        "instrument_key": key,
        "underlying_symbol": instrument,
        "instrument_type": right,
        "strike_price": strike,
        "expiry": expiry,
        "lot_size": lot,
    }


def _order(oid, key, side, price, when, status="complete", qty=75, symbol="NIFTY26SEP25000CE"):
    return {
        "order_id": oid,
        "instrument_token": key,
        "transaction_type": side,
        "average_price": price,
        "filled_quantity": qty,
        "status": status,
        "order_timestamp": when.strftime("%Y-%m-%d %H:%M:%S"),
        "trading_symbol": symbol,
    }


def _index(*rows):
    return build_contract_index(list(rows))


def _plan(orders, index, existing_ids=(), existing_trades=()):
    return build_sync_plan(
        orders,
        index,
        existing_order_ids=existing_ids,
        existing_trades=existing_trades,
        today=TODAY,
    )


def test_classify_two_four_and_other_counts():
    # Iron Condor detection will be revisited later: 4 legs are IRON_FLY.
    assert classify_leg_count(2) == "STRADDLE"
    assert classify_leg_count(4) == "IRON_FLY"
    assert classify_leg_count(4) != "IRON_CONDOR"
    assert classify_leg_count(1) == "UNCLASSIFIED"
    assert classify_leg_count(3) == "UNCLASSIFIED"
    assert classify_leg_count(5) == "UNCLASSIFIED"


def test_duplicate_upstox_order_id_is_skipped():
    index = _index(_contract("NSE_FO|1", "CE", 25000), _contract("NSE_FO|2", "PE", 25000))
    orders = [
        _order("OID-1", "NSE_FO|1", "SELL", 100, _stamp(0)),
        _order("OID-2", "NSE_FO|2", "SELL", 90, _stamp(1), symbol="NIFTY26SEP25000PE"),
    ]
    first = _plan(orders, index)
    assert first["new_trades"] == 1
    assert first["duplicates_skipped"] == 0
    second = _plan(orders, index, existing_ids=["OID-1", "OID-2"])
    assert second["new_legs"] == 0
    assert second["new_trades"] == 0
    assert second["orphans"] == 0
    assert second["duplicates_skipped"] == 2
    assert second["creates"] == []


def test_two_legs_within_five_minutes_become_straddle_when_no_trade_exists():
    index = _index(_contract("NSE_FO|1", "CE", 25000), _contract("NSE_FO|2", "PE", 25000))
    orders = [
        _order("OID-1", "NSE_FO|1", "SELL", 100, _stamp(0)),
        _order("OID-2", "NSE_FO|2", "SELL", 90, _stamp(4), symbol="NIFTY26SEP25000PE"),
    ]
    plan = _plan(orders, index)
    assert plan["new_legs"] == 2
    assert plan["new_trades"] == 1
    assert plan["orphans"] == 0
    assert plan["creates"][0]["trade_type"] == "STRADDLE"
    assert len(plan["creates"][0]["legs"]) == 2
    assert {leg["upstox_order_id"] for leg in plan["creates"][0]["legs"]} == {"OID-1", "OID-2"}


def test_four_legs_become_iron_fly():
    rows = [
        _contract("NSE_FO|1", "CE", 24900),
        _contract("NSE_FO|2", "PE", 24900),
        _contract("NSE_FO|3", "CE", 25100),
        _contract("NSE_FO|4", "PE", 25100),
    ]
    orders = [
        _order("A", "NSE_FO|1", "BUY", 10, _stamp(0), symbol="NIFTY26SEP24900CE"),
        _order("B", "NSE_FO|2", "BUY", 11, _stamp(1), symbol="NIFTY26SEP24900PE"),
        _order("C", "NSE_FO|3", "SELL", 40, _stamp(2), symbol="NIFTY26SEP25100CE"),
        _order("D", "NSE_FO|4", "SELL", 38, _stamp(3), symbol="NIFTY26SEP25100PE"),
    ]
    plan = _plan(orders, _index(*rows))
    assert plan["new_trades"] == 1
    assert plan["new_legs"] == 4
    assert plan["orphans"] == 0
    assert plan["creates"][0]["trade_type"] == "IRON_FLY"


def test_one_or_three_legs_become_unclassified():
    rows = [
        _contract("NSE_FO|1", "CE", 25000),
        _contract("NSE_FO|2", "PE", 25000),
        _contract("NSE_FO|3", "CE", 25100),
    ]
    one = _plan([_order("A", "NSE_FO|1", "SELL", 10, _stamp(0))], _index(*rows))
    assert one["creates"][0]["trade_type"] == "UNCLASSIFIED"
    assert one["new_legs"] == 1
    three_orders = [
        _order("A", "NSE_FO|1", "SELL", 10, _stamp(0)),
        _order("B", "NSE_FO|2", "SELL", 11, _stamp(1), symbol="NIFTY26SEP25000PE"),
        _order("C", "NSE_FO|3", "BUY", 4, _stamp(2), symbol="NIFTY26SEP25100CE"),
    ]
    three = _plan(three_orders, _index(*rows))
    assert three["creates"][0]["trade_type"] == "UNCLASSIFIED"
    assert three["new_legs"] == 3
    assert three["new_trades"] == 1


def test_existing_underlying_expiry_becomes_orphans_not_a_new_trade():
    index = _index(_contract("NSE_FO|1", "CE", 25000), _contract("NSE_FO|2", "PE", 25000))
    orders = [
        _order("OID-1", "NSE_FO|1", "SELL", 100, _stamp(0)),
        _order("OID-2", "NSE_FO|2", "SELL", 90, _stamp(1), symbol="NIFTY26SEP25000PE"),
    ]
    plan = _plan(orders, index, existing_trades={("NIFTY", EXPIRY)})
    assert plan["new_trades"] == 0
    assert plan["new_legs"] == 0
    assert plan["creates"] == []
    assert plan["orphans"] == 2
    assert {leg["upstox_order_id"] for leg in plan["orphan_legs"]} == {"OID-1", "OID-2"}
    assert all(leg["instrument"] == "NIFTY" and leg["expiry"] == EXPIRY for leg in plan["orphan_legs"])


def test_legs_more_than_five_minutes_apart_are_separate_groups():
    legs = [
        {
            "instrument": "NIFTY",
            "expiry": EXPIRY,
            "entry_time": IST.localize(_stamp(0)),
            "upstox_order_id": "A",
        },
        {
            "instrument": "NIFTY",
            "expiry": EXPIRY,
            "entry_time": IST.localize(_stamp(0) + GROUP_WINDOW),
            "upstox_order_id": "B",
        },
        {
            "instrument": "NIFTY",
            "expiry": EXPIRY,
            "entry_time": IST.localize(_stamp(0) + GROUP_WINDOW + timedelta(seconds=1)),
            "upstox_order_id": "C",
        },
    ]
    within = group_new_legs(legs[:2])
    assert len(within) == 1
    apart = group_new_legs([legs[0], legs[2]])
    assert len(apart) == 2
    assert [group[0]["upstox_order_id"] for group in apart] == ["A", "C"]

    index = _index(_contract("NSE_FO|1", "CE", 25000), _contract("NSE_FO|2", "PE", 24900))
    orders = [
        _order("A", "NSE_FO|1", "SELL", 10, _stamp(0)),
        _order("C", "NSE_FO|2", "SELL", 9, _stamp(0) + GROUP_WINDOW + timedelta(seconds=1), symbol="NIFTY26SEP24900PE"),
    ]
    plan = _plan(orders, index)
    # The later fill is its own group. The first group already created the
    # underlying+expiry trade, so the later leg is an orphan, not a second trade
    # and not merged into one 2-leg STRADDLE.
    assert plan["new_trades"] == 1
    assert plan["creates"][0]["trade_type"] == "UNCLASSIFIED"
    assert len(plan["creates"][0]["legs"]) == 1
    assert plan["orphans"] == 1
    assert plan["orphan_legs"][0]["upstox_order_id"] == "C"


def test_missing_lot_is_skipped_without_failing_the_group():
    rows = [
        _contract("NSE_FO|1", "CE", 25000, lot=None),
        _contract("NSE_FO|2", "PE", 25000, lot=65),
    ]
    # Same right has no fallback lot, so the CE is skipped and the PE still imports.
    orders = [
        _order("A", "NSE_FO|1", "SELL", 10, _stamp(0)),
        _order("B", "NSE_FO|2", "SELL", 9, _stamp(1), symbol="NIFTY26SEP25000PE"),
    ]
    plan = _plan(orders, _index(*rows))
    assert plan["skipped_no_lot"] == 1
    assert plan["new_legs"] == 1
    assert plan["creates"][0]["trade_type"] == "UNCLASSIFIED"
    assert plan["creates"][0]["legs"][0]["upstox_order_id"] == "B"


def test_assign_sets_trade_id(monkeypatch):
    captured = []

    class Result:
        rowcount = 1

        def fetchone(self):
            return (1,)

    class DB:
        def execute(self, stmt, params=None):
            captured.append((str(stmt), params or {}))
            return Result()

        def commit(self):
            return None

        def rollback(self):
            return None

        def close(self):
            return None

    monkeypatch.setattr("backend.services.multi_leg_upstox_sync.ensure_multi_leg_tables", lambda: None)
    monkeypatch.setattr("backend.services.multi_leg_upstox_sync.SessionLocal", lambda: DB())
    monkeypatch.setattr("backend.services.multi_leg_upstox_sync._refresh_subscriptions", lambda: None)
    out = assign_orphan(LEG_ID, TRADE_ID)
    updates = [item for item in captured if "SET trade_id" in item[0]]
    assert updates
    assert updates[0][1]["trade_id"] == TRADE_ID
    assert updates[0][1]["leg_id"] == LEG_ID
    assert "trade_id IS NULL" in updates[0][0]
    assert out["trade_id"] == TRADE_ID
    assert out["leg_id"] == LEG_ID
    with pytest.raises(MultiLegValidationError):
        assign_orphan("not-a-uuid", TRADE_ID)

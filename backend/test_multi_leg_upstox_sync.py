"""Upstox multi-leg import: dedupe, grouping, classification, orphans, assign."""
from datetime import date, datetime, timedelta

import pytz
import pytest

from backend.services.multi_leg_options import MultiLegNotFound, MultiLegValidationError
from backend.services.multi_leg_upstox_sync import (
    GROUP_WINDOW,
    _load_existing,
    assign_orphan,
    build_contract_index,
    build_sync_plan,
    classify_leg_count,
    dismiss_orphan,
    group_new_legs,
    historical_row_to_order,
    parse_trade_date,
    promote_orphan_as_ladder_main,
)

IST = pytz.timezone("Asia/Kolkata")
EXPIRY = date(2026, 9, 29)
TODAY = date(2026, 9, 26)
LEG_ID = "11111111-1111-1111-1111-111111111111"
TRADE_ID = "22222222-2222-2222-2222-222222222222"
MERGED_LEG_ID = "33333333-3333-3333-3333-333333333333"


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


def test_sync_date_filter_does_not_import_another_day():
    index = _index(_contract("NSE_FO|1", "CE", 25000))
    later = _order("NEXT", "NSE_FO|1", "SELL", 12, _stamp(5))
    # 20:30 UTC on the 26th is 02:00 IST on the 27th.
    later["order_timestamp"] = "2026-09-26T20:30:00+00:00"
    orders = [
        _order("TODAY", "NSE_FO|1", "SELL", 10, _stamp(0)),
        _order("YDAY", "NSE_FO|1", "SELL", 9, datetime(2026, 9, 25, 15, 10)),
        later,
    ]
    plan = _plan(orders, index)
    imported = {leg["upstox_order_id"] for item in plan["creates"] for leg in item["legs"]}
    imported.update(leg["upstox_order_id"] for leg in plan["orphan_legs"])
    assert imported == {"TODAY"}
    assert plan["new_legs"] == 1
    assert plan["new_trades"] == 1
    other_day = build_sync_plan(
        orders,
        index,
        existing_order_ids=(),
        existing_trades=(),
        today=date(2026, 9, 25),
    )
    yday_ids = {leg["upstox_order_id"] for item in other_day["creates"] for leg in item["legs"]}
    assert yday_ids == {"YDAY"}
    assert "TODAY" not in yday_ids


def test_historical_row_from_another_date_is_dropped():
    index = _index(_contract("NSE_FO|25000", "CE", 25000))
    row = {
        "exchange": "NSE",
        "segment": "FO",
        "option_type": "CE",
        "quantity": 75,
        "trade_id": "T-OTHER",
        "trade_date": "2026-09-25",
        "transaction_type": "SELL",
        "strike_price": 25000,
        "expiry": "2026-09-29",
        "price": 100,
        "symbol": "NIFTY",
    }
    assert historical_row_to_order(row, TODAY, index) is None
    same = dict(row)
    same["trade_date"] = TODAY.isoformat()
    same["trade_id"] = "T-TODAY"
    mapped = historical_row_to_order(same, TODAY, index)
    assert mapped["order_id"] == "T-TODAY"
    assert mapped["order_timestamp"].startswith(TODAY.isoformat())
    assert mapped["instrument_token"] == "NSE_FO|25000"
    with pytest.raises(MultiLegValidationError, match="YYYY-MM-DD"):
        parse_trade_date("26-09-2026")


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


def test_promote_orphan_creates_ladder_main(monkeypatch):
    captured = []
    orphan_row = {
        "id": LEG_ID,
        "side": "BUY",
        "option_type": "CE",
        "strike_price": 25000,
        "leg_expiry_date": EXPIRY,
        "entry_price": 120.5,
        "entry_time": IST.localize(datetime(2026, 9, 26, 10, 15, 0)),
        "lot_size": 75,
        "instrument_key": "NSE_FO|25000",
        "upstox_order_id": "OID-ORPHAN",
    }

    class MappingResult:
        rowcount = 1

        def fetchone(self):
            return orphan_row

        def mappings(self):
            return self

    class DB:
        def execute(self, stmt, params=None):
            captured.append((str(stmt), params or {}))
            return MappingResult()

        def commit(self):
            return None

        def rollback(self):
            return None

        def close(self):
            return None

    monkeypatch.setattr("backend.services.multi_leg_upstox_sync.ensure_multi_leg_tables", lambda: None)
    monkeypatch.setattr("backend.services.multi_leg_upstox_sync.SessionLocal", lambda: DB())
    monkeypatch.setattr("backend.services.multi_leg_upstox_sync._refresh_subscriptions", lambda: None)
    monkeypatch.setattr("backend.services.multi_leg_upstox_sync.allocate_trade_no", lambda _db: 42)
    monkeypatch.setattr(
        "backend.services.multi_leg_upstox_sync.journal_contract_index",
        lambda: build_contract_index([
            {
                "underlying_symbol": "NIFTY",
                "instrument_type": "CE",
                "strike_price": 25000,
                "expiry": EXPIRY,
                "lot_size": 75,
                "instrument_key": "NSE_FO|25000",
            }
        ]),
    )
    monkeypatch.setattr(
        "backend.services.multi_leg_upstox_sync._spot_prices",
        lambda _names: {"NIFTY": 24980.0},
    )

    out = promote_orphan_as_ladder_main(LEG_ID)
    inserts = [item for item in captured if "INSERT INTO multi_leg_trades" in item[0]]
    updates = [item for item in captured if "qualifier = 'MAIN'" in item[0]]
    assert inserts
    assert inserts[0][1]["instrument"] == "NIFTY"
    assert inserts[0][1]["spot"] == 24980.0
    assert inserts[0][1]["trade_no"] == 42
    assert updates
    assert updates[0][1]["leg_id"] == LEG_ID
    assert updates[0][1]["trade_id"] == out["trade_id"]
    assert out["trade_type"] == "LADDER"
    assert out["trade_no"] == 42
    assert out["instrument"] == "NIFTY"
    with pytest.raises(MultiLegValidationError):
        promote_orphan_as_ladder_main("not-a-uuid")


def test_deleted_orphan_is_not_recreated_on_later_sync(monkeypatch):
    """Dismissing an orphan stores its order id, so the next sync skips that fill."""
    memory = {
        "legs": {
            LEG_ID: {"trade_id": None, "upstox_order_id": "OID-ORPHAN"},
            MERGED_LEG_ID: {"trade_id": TRADE_ID, "upstox_order_id": "OID-MERGED"},
        },
        "dismissed": set(),
    }

    class Result:
        def __init__(self, rows=None, rowcount=0):
            self._rows = rows or []
            self.rowcount = rowcount

        def fetchone(self):
            return self._rows[0] if self._rows else None

        def fetchall(self):
            return list(self._rows)

        def mappings(self):
            return self

        def all(self):
            return list(self._rows)

    class DB:
        def execute(self, stmt, params=None):
            sql = " ".join(str(stmt).split())
            params = params or {}
            if "DELETE FROM multi_leg_trade_legs" in sql:
                assert "trade_id IS NULL" in sql
                leg = memory["legs"].get(params["leg_id"])
                if not leg or leg["trade_id"] is not None:
                    return Result(rowcount=0)
                del memory["legs"][params["leg_id"]]
                return Result(rowcount=1)
            if "INSERT INTO multi_leg_dismissed_upstox_orders" in sql:
                memory["dismissed"].add(params["upstox_order_id"])
                return Result(rowcount=1)
            if "trade_id IS NULL" in sql and "upstox_order_id" in sql:
                leg = memory["legs"].get(params["leg_id"])
                if not leg or leg["trade_id"] is not None:
                    return Result([])
                return Result([(leg["upstox_order_id"],)])
            if "multi_leg_dismissed_upstox_orders" in sql:
                rows = [
                    (leg["upstox_order_id"],)
                    for leg in memory["legs"].values()
                    if leg.get("upstox_order_id")
                ]
                rows.extend((oid,) for oid in memory["dismissed"])
                return Result(rows)
            if "FROM multi_leg_trades" in sql:
                return Result([{"instrument": "NIFTY", "expiry_date": EXPIRY}])
            raise AssertionError(sql)

        def commit(self):
            return None

        def rollback(self):
            return None

        def close(self):
            return None

    monkeypatch.setattr("backend.services.multi_leg_upstox_sync.ensure_multi_leg_tables", lambda: None)
    monkeypatch.setattr("backend.services.multi_leg_upstox_sync.SessionLocal", lambda: DB())

    out = dismiss_orphan(LEG_ID)
    assert out["deleted"] == LEG_ID
    assert out["upstox_order_id"] == "OID-ORPHAN"
    assert LEG_ID not in memory["legs"]
    assert memory["dismissed"] == {"OID-ORPHAN"}

    # Same underlying and expiry already has a trade, so this fill would be an
    # orphan again if the dismissal were not part of the sync dedupe set.
    ids, keys = _load_existing(DB())
    assert "OID-ORPHAN" in ids
    assert "OID-MERGED" in ids
    assert ("NIFTY", EXPIRY) in keys
    index = _index(_contract("NSE_FO|1", "CE", 25000))
    plan = build_sync_plan(
        [_order("OID-ORPHAN", "NSE_FO|1", "SELL", 100, _stamp(0))],
        index,
        existing_order_ids=ids,
        existing_trades=keys,
        today=TODAY,
    )
    assert plan["orphans"] == 0
    assert plan["creates"] == []
    assert plan["orphan_legs"] == []
    assert plan["duplicates_skipped"] == 1

    with pytest.raises(MultiLegNotFound):
        dismiss_orphan(MERGED_LEG_ID)
    assert MERGED_LEG_ID in memory["legs"]
    assert "OID-MERGED" not in memory["dismissed"]

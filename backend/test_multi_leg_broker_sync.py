"""Broker sync for multi-leg options: Dhan venues, Delta options, and dedupe."""
from datetime import date

from backend.services.multi_leg_broker_sync import (
    build_option_plan,
    leg_fingerprint,
    parse_broker_rows,
    parse_sync_date,
)

DAY = date(2026, 11, 9)
EXPIRY = "2026-11-26"


def _dhan(segment, symbol, *, right="NA", strike=None, price=10, when="2026-11-09 10:15:03", qty=65, trade_id="0", expiry=EXPIRY):
    return {
        "exchangeSegment": segment,
        "transactionType": "BUY",
        "tradingSymbol": symbol,
        "drvOptionType": right,
        "drvStrikePrice": strike,
        "drvExpiryDate": expiry,
        "tradedQuantity": qty,
        "tradedPrice": price,
        "exchangeTime": when,
        "exchangeTradeId": trade_id,
        "securityId": "9001",
    }


def test_dhan_imports_nse_bse_and_mcx_options_and_skips_future_and_equity():
    rows = [
        _dhan("NSE_FNO", "RELIANCE-Nov2026-1400-PE", right="PUT", strike=1400, price=12.5, qty=250),
        _dhan(
            "NSE_FNO",
            "NIFTY-Nov2026-24500-CE",
            right="CALL",
            strike=24500,
            price=120,
            when="2026-11-09 10:16:00",
        ),
        _dhan(
            "BSE_FNO",
            "SENSEX-Nov2026-80000-CE",
            right="CALL",
            strike=80000,
            price=210,
            when="2026-11-09 10:17:00",
            qty=20,
        ),
        _dhan(
            "MCX_COMM",
            "CRUDEOIL-Nov2026-5500-CE",
            right="CALL",
            strike=5500,
            price=40,
            when="2026-11-09 10:18:00",
            qty=1,
        ),
        # Symbol suffix is enough when drvOptionType is NA.
        _dhan(
            "NSE_FNO",
            "TCS25NOV3500CE",
            right="NA",
            strike=None,
            price=55,
            when="2026-11-09 10:19:00",
            qty=175,
        ),
        _dhan("MCX_COMM", "GOLDM25DECFUT", right="NA", strike=None, price=72000, when="2026-11-09 11:00:00", qty=1),
        _dhan("NSE_EQ", "RELIANCE", right="NA", strike=None, price=1280, when="2026-11-09 11:05:00", qty=1),
        _dhan("NSE_FNO", "NIFTY-Nov2026-FUT", right="NA", strike=None, price=24550, when="2026-11-09 11:06:00", qty=65),
        _dhan("NSE_CURRENCY", "USDINR-Nov2026-84-CE", right="CALL", strike=84, price=0.2, when="2026-11-09 11:07:00", qty=1),
    ]
    parsed = parse_broker_rows(rows, DAY, "dhan")
    legs = parsed["legs"]
    assert sorted(leg["instrument"] for leg in legs) == ["CRUDEOIL", "NIFTY", "RELIANCE", "SENSEX", "TCS"]
    by_name = {leg["instrument"]: leg for leg in legs}
    assert by_name["RELIANCE"]["option_type"] == "PE"
    assert by_name["RELIANCE"]["strike_price"] == 1400
    assert by_name["NIFTY"]["option_type"] == "CE"
    assert by_name["SENSEX"]["option_type"] == "CE"
    assert by_name["CRUDEOIL"]["option_type"] == "CE"
    assert by_name["TCS"]["option_type"] == "CE"
    assert by_name["TCS"]["strike_price"] == 3500
    assert "GOLDM" not in by_name
    plan = build_option_plan(legs)
    assert plan["new_legs"] == 5
    assert plan["duplicates_skipped"] == 0
    stored = [leg for item in plan["creates"] for leg in item["legs"]]
    assert all(leg["upstox_order_id"] != "0" for leg in stored)
    assert all(not str(leg["upstox_order_id"]).startswith("0") for leg in stored)


def test_dhan_exchange_trade_id_zero_does_not_collapse_distinct_fills():
    rows = [
        _dhan(
            "NSE_FNO",
            "NIFTY-Nov2026-24500-CE",
            right="CALL",
            strike=24500,
            price=100,
            when="2026-11-09 10:15:01",
            trade_id="0",
        ),
        _dhan(
            "NSE_FNO",
            "NIFTY-Nov2026-24500-PE",
            right="PUT",
            strike=24500,
            price=90,
            when="2026-11-09 10:15:02",
            trade_id="0",
        ),
        _dhan(
            "NSE_FNO",
            "RELIANCE-Nov2026-1400-CE",
            right="CALL",
            strike=1400,
            price=15,
            when="2026-11-09 10:15:01",
            trade_id=0,
        ),
    ]
    legs = parse_broker_rows(rows, DAY, "dhan")["legs"]
    assert len(legs) == 3
    ids = [leg["upstox_order_id"] for leg in legs]
    assert len(set(ids)) == 3
    assert "0" not in ids
    assert all(not leg_fingerprint(leg).endswith("|0") for leg in legs)
    plan = build_option_plan(legs)
    assert plan["new_legs"] == 3
    assert plan["duplicates_skipped"] == 0


def test_delta_imports_crypto_options_and_skips_perpetual_and_future():
    rows = [
        {
            "id": "btc-opt",
            "product_symbol": "C-BTC-99000-271126",
            "contract_type": "call_options",
            "side": "sell",
            "price": 600,
            "size": 2,
            "created_at": "2026-11-09T10:15:00+05:30",
        },
        {
            "id": "eth-opt",
            "product_symbol": "P-ETH-3500-271126",
            "contract_type": "put_options",
            "side": "buy",
            "price": 40,
            "size": 3,
            "created_at": "2026-11-09T10:16:00+05:30",
        },
        {
            "id": "sol-opt",
            "product_symbol": "C-SOL-180-271126",
            "contract_type": "call_options",
            "side": "sell",
            "price": 5,
            "size": 4,
            "created_at": "2026-11-09T10:17:00+05:30",
        },
        {
            "id": "btc-perp",
            "product_symbol": "BTCUSD",
            "contract_type": "perpetual_futures",
            "side": "buy",
            "price": 90000,
            "size": 1,
            "created_at": "2026-11-09T10:18:00+05:30",
        },
        {
            "id": "btc-fut",
            "product_symbol": "BTC-271126",
            "contract_type": "futures",
            "side": "sell",
            "price": 90100,
            "size": 1,
            "created_at": "2026-11-09T10:19:00+05:30",
        },
    ]
    legs = parse_broker_rows(rows, DAY, "delta")["legs"]
    assert sorted(leg["instrument"] for leg in legs) == ["BITCOIN", "ETHEREUM", "SOL"]
    by_name = {leg["instrument"]: leg for leg in legs}
    assert by_name["BITCOIN"]["option_type"] == "CE"
    assert by_name["BITCOIN"]["strike_price"] == 99000
    assert by_name["BITCOIN"]["side"] == "SELL"
    assert by_name["BITCOIN"]["instrument_key"] == "C-BTC-99000-271126"
    assert by_name["ETHEREUM"]["option_type"] == "PE"
    assert by_name["ETHEREUM"]["side"] == "BUY"
    assert by_name["SOL"]["option_type"] == "CE"
    assert all(leg["upstox_order_id"] != "0" for leg in legs)
    plan = build_option_plan(legs)
    assert plan["new_legs"] == 3
    assert {item["instrument"] for item in plan["creates"]} == {"BITCOIN", "ETHEREUM", "SOL"}


def test_dedup_skips_same_instrument_time_and_price():
    row = _dhan(
        "NSE_FNO",
        "RELIANCE-Nov2026-1400-PE",
        right="PUT",
        strike=1400,
        price=12.5,
        when="2026-11-09 10:15:03",
        trade_id="0",
    )
    again = dict(row)
    again["exchangeTradeId"] = "0"
    other_id = dict(row)
    other_id["exchangeTradeId"] = "998877"
    different_price = dict(row)
    different_price["tradedPrice"] = 13
    different_price["exchangeTime"] = "2026-11-09 10:15:03"

    first = parse_broker_rows([row], DAY, "dhan")["legs"]
    assert len(first) == 1
    fingerprint = leg_fingerprint(first[0])
    plan = build_option_plan(
        parse_broker_rows([again, other_id, different_price], DAY, "dhan")["legs"],
        existing_fingerprints=[fingerprint],
    )
    assert plan["duplicates_skipped"] == 2
    assert plan["new_legs"] == 1
    stored = [leg for item in plan["creates"] for leg in item["legs"]]
    assert len(stored) == 1
    assert stored[0]["entry_price"] == 13

    # The same contract, time, and price is one leg even inside a single payload.
    doubled = build_option_plan(parse_broker_rows([row, row], DAY, "dhan")["legs"])
    assert doubled["new_legs"] == 1
    assert doubled["duplicates_skipped"] == 1


def test_trade_date_accepts_iso_and_indian_day_first():
    assert parse_sync_date("2026-11-09") == DAY
    assert parse_sync_date("09/11/2026") == DAY
    assert parse_sync_date("9/11/2026") == DAY


def test_upstox_history_shape_keeps_stock_and_mcx_options():
    rows = [
        {
            "exchange": "NSE",
            "segment": "FO",
            "option_type": "CE",
            "symbol": "RELIANCE",
            "strike_price": 1400,
            "expiry": "2026-11-26",
            "quantity": 250,
            "price": 12,
            "transaction_type": "SELL",
            "trade_id": "T-REL",
            "trade_date": "2026-11-09",
            "order_timestamp": "2026-11-09 10:15:00",
        },
        {
            "exchange": "MCX",
            "segment": "COM",
            "option_type": "PE",
            "symbol": "CRUDEOIL",
            "strike_price": 5500,
            "expiry": "2026-11-17",
            "quantity": 1,
            "price": 30,
            "transaction_type": "BUY",
            "trade_id": "T-CO",
            "trade_date": "2026-11-09",
            "_segment_hint": "COM",
        },
        {
            "exchange": "NSE",
            "segment": "FO",
            "instrument_type": "FUT",
            "symbol": "NIFTY-Nov2026-FUT",
            "quantity": 65,
            "price": 24550,
            "transaction_type": "BUY",
            "trade_id": "T-FUT",
            "trade_date": "2026-11-09",
            "order_timestamp": "2026-11-09 11:00:00",
        },
    ]
    legs = parse_broker_rows(rows, DAY, "upstox")["legs"]
    assert sorted(leg["instrument"] for leg in legs) == ["CRUDEOIL", "RELIANCE"]
    by_name = {leg["instrument"]: leg for leg in legs}
    assert by_name["RELIANCE"]["upstox_order_id"] == "T-REL"
    assert by_name["RELIANCE"]["side"] == "SELL"
    assert by_name["CRUDEOIL"]["option_type"] == "PE"


def test_sync_dhan_and_delta_persist_only_option_legs(monkeypatch):
    """Fetch is mocked. Storage goes through the same plan the Upstox sync writes."""
    dhan_rows = [
        _dhan("NSE_FNO", "RELIANCE-Nov2026-1400-PE", right="PUT", strike=1400, price=12.5, qty=250),
        _dhan("NSE_FNO", "NIFTY-Nov2026-24500-CE", right="CALL", strike=24500, price=120, when="2026-11-09 10:16:00"),
        _dhan("BSE_FNO", "SENSEX-Nov2026-80000-CE", right="CALL", strike=80000, price=210, when="2026-11-09 10:17:00", qty=20),
        _dhan("MCX_COMM", "CRUDEOIL-Nov2026-5500-CE", right="CALL", strike=5500, price=40, when="2026-11-09 10:18:00", qty=1),
        _dhan("MCX_COMM", "GOLDM25DECFUT", right="NA", strike=None, price=72000, when="2026-11-09 11:00:00", qty=1),
        _dhan("NSE_EQ", "RELIANCE", right="NA", strike=None, price=1280, when="2026-11-09 11:05:00", qty=1),
    ]
    delta_rows = [
        {
            "product_symbol": "C-BTC-99000-271126",
            "contract_type": "call_options",
            "side": "sell",
            "price": 600,
            "size": 2,
            "created_at": "2026-11-09T10:15:00+05:30",
        },
        {
            "product_symbol": "P-ETH-3500-271126",
            "contract_type": "put_options",
            "side": "buy",
            "price": 40,
            "size": 1,
            "created_at": "2026-11-09T10:16:00+05:30",
        },
        {
            "product_symbol": "BTCUSD",
            "contract_type": "perpetual_futures",
            "side": "buy",
            "price": 90000,
            "size": 1,
            "created_at": "2026-11-09T10:18:00+05:30",
        },
    ]

    class Result:
        def fetchall(self):
            return []

        def mappings(self):
            return self

        def all(self):
            return []

    class DB:
        def execute(self, *_args, **_kwargs):
            return Result()

        def close(self):
            return None

    stored = []
    monkeypatch.setattr("backend.services.multi_leg_broker_sync.ensure_multi_leg_tables", lambda: None)
    monkeypatch.setattr("backend.database.SessionLocal", lambda: DB())
    monkeypatch.setattr("backend.services.multi_leg_broker_sync._persist_plan", lambda plan, _spots: stored.append(plan))
    monkeypatch.setattr("backend.services.multi_leg_broker_sync._refresh_subscriptions", lambda: None)
    monkeypatch.setattr("backend.services.multi_leg_broker_sync._spot_prices", lambda names: {name: None for name in names})
    monkeypatch.setattr("backend.services.multi_leg_broker_sync._enrich_spots", lambda _spots, _names: None)
    monkeypatch.setattr("backend.services.multi_leg_broker_sync._fetch_dhan_rows", lambda _day: dhan_rows)
    monkeypatch.setattr("backend.services.multi_leg_broker_sync._fetch_delta_rows", lambda _day: delta_rows)

    from backend.services.multi_leg_broker_sync import sync_multi_leg_from_broker

    dhan = sync_multi_leg_from_broker("09/11/2026", "Dhan")
    assert dhan["broker"] == "dhan"
    assert dhan["new_legs"] == 4
    dhan_names = sorted(leg["instrument"] for item in stored[0]["creates"] for leg in item["legs"])
    assert dhan_names == ["CRUDEOIL", "NIFTY", "RELIANCE", "SENSEX"]

    delta = sync_multi_leg_from_broker("2026-11-09", "Delta Exchange India")
    assert delta["broker"] == "delta"
    assert delta["new_legs"] == 2
    delta_names = sorted(leg["instrument"] for item in stored[1]["creates"] for leg in item["legs"])
    assert delta_names == ["BITCOIN", "ETHEREUM"]

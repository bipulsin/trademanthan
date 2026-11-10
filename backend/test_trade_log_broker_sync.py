"""Trade log broker sync: futures filter, dedup, and insert."""
from __future__ import annotations

from datetime import date

from backend.services.trade_log_broker_sync import row_to_fill, sync_from_broker

DAY = date(2026, 11, 9)


def _dhan(segment, symbol, side="BUY", qty=65, price=24550.5, when="2026-11-09 10:15:03", **extra):
    row = {
        "exchangeSegment": segment,
        "transactionType": side,
        "tradingSymbol": symbol,
        "drvOptionType": extra.pop("drvOptionType", "NA"),
        "tradedQuantity": qty,
        "tradedPrice": price,
        "exchangeTime": when,
        "exchangeTradeId": extra.pop("exchangeTradeId", f"{segment}-{symbol}-{side}"),
    }
    row.update(extra)
    return row


def _upstox(segment, symbol, key, side="BUY", qty=30, price=52000, **extra):
    row = {
        "exchange": "NSE" if not str(key).startswith("BSE") and segment != "COM" else "MCX",
        "segment": segment,
        "instrument_token": key,
        "trading_symbol": symbol,
        "instrument_type": extra.pop("instrument_type", "FUT"),
        "transaction_type": side,
        "quantity": qty,
        "average_price": price,
        "order_timestamp": "2026-11-09 11:05:00",
        "trade_id": extra.pop("trade_id", f"{key}-{symbol}"),
        "status": "complete",
    }
    row.update(extra)
    return row


def test_futures_filter_keeps_nse_and_drops_mcx_bse_and_the_rest():
    nifty = row_to_fill(_dhan("NSE_FNO", "NIFTY-Nov2026-FUT", qty=65, price=24550.5), DAY)
    finnifty = row_to_fill(
        _dhan("NSE_FNO", "FINNIFTY-Nov2026-FUT", qty=40, price=21000, when="2026-11-09 10:16:00"),
        DAY,
    )
    reliance = row_to_fill(
        _dhan("NSE_FNO", "RELIANCE25NOVFUT", qty=250, price=1280.5, when="2026-11-09 10:20:00"),
        DAY,
    )
    sensex = row_to_fill(
        _dhan("BSE_FNO", "SENSEX-Nov2026-FUT", qty=10, price=80000, when="2026-11-09 10:21:00"),
        DAY,
    )
    bankex = row_to_fill(
        _upstox("FO", "BANKEX25NOVFUT", "BSE_FO|12", qty=15, price=60000),
        DAY,
    )
    banknifty = row_to_fill(
        _upstox("FO", "BANKNIFTY25NOVFUT", "NSE_FO|44088", side="SELL", qty=30, price=52100),
        DAY,
    )
    option_call = row_to_fill(
        _dhan(
            "NSE_FNO",
            "NIFTY-Nov2026-24500-CE",
            drvOptionType="CALL",
            qty=65,
            price=120,
        ),
        DAY,
    )
    option_symbol = row_to_fill(
        _upstox("FO", "NIFTY25NOV24500CE", "NSE_FO|999", instrument_type="OPTIDX", price=80),
        DAY,
    )
    opt_product = row_to_fill(
        _dhan("NSE_FNO", "RELIANCE-Nov2026-1400-PE", instrumentType="OPTSTK", drvOptionType="PUT"),
        DAY,
    )
    equity = row_to_fill(_dhan("NSE_EQ", "RELIANCE", qty=1, price=1280), DAY)
    mcx = row_to_fill(
        _dhan("MCX_COMM", "GOLDM25DECFUT", qty=1, price=72000, when="2026-11-09 11:00:00"),
        DAY,
    )
    currency = row_to_fill(
        {
            "exchange": "NSE",
            "segment": "CD",
            "instrument_token": "NCD_FO|42",
            "trading_symbol": "USDINR25NOVFUT",
            "instrument_type": "FUT",
            "transaction_type": "BUY",
            "quantity": 1,
            "average_price": 84,
            "order_timestamp": "2026-11-09 10:00:00",
            "trade_id": "fx1",
            "status": "complete",
        },
        DAY,
    )
    crude_no_exchange = row_to_fill(
        {
            "transactionType": "BUY",
            "tradingSymbol": "CRUDEOIL25NOVFUT",
            "tradedQuantity": 1,
            "tradedPrice": 5500,
            "exchangeTime": "2026-11-09 11:10:00",
            "exchangeTradeId": "crude-bare",
        },
        DAY,
    )
    gold_mislabeled_nse = row_to_fill(
        _dhan("NSE_FNO", "GOLDM25DECFUT", qty=1, price=72000, when="2026-11-09 11:11:00"),
        DAY,
    )
    nifty_no_venue = row_to_fill(
        {
            "transactionType": "BUY",
            "tradingSymbol": "NIFTY-Nov2026-FUT",
            "tradedQuantity": 65,
            "tradedPrice": 24550.5,
            "exchangeTime": "2026-11-09 10:15:03",
            "exchangeTradeId": "nifty-bare",
        },
        DAY,
    )

    assert nifty is not None and nifty.underlying == "NIFTY" and nifty.side == "BUY"
    assert finnifty is not None and finnifty.underlying == "FINNIFTY"
    assert reliance is not None and reliance.underlying == "RELIANCE"
    assert banknifty is not None and banknifty.underlying == "BANKNIFTY" and banknifty.side == "SELL"
    assert sensex is None
    assert bankex is None
    assert option_call is None
    assert option_symbol is None
    assert opt_product is None
    assert equity is None
    assert mcx is None
    assert currency is None
    assert crude_no_exchange is None
    assert gold_mislabeled_nse is None
    assert nifty_no_venue is None


def test_mcx_and_bse_fills_are_not_inserted():
    rows = [
        _dhan("NSE_FNO", "RELIANCE25NOVFUT", qty=250, price=1280.25, when="2026-11-09 09:20:01"),
        _dhan("NSE_FNO", "NIFTY-Nov2026-FUT", qty=65, price=24550.5),
        _dhan("MCX_COMM", "NATURALGAS25NOVFUT", qty=1, price=250, when="2026-11-09 11:20:00"),
        _dhan("BSE_FNO", "SENSEX-Nov2026-FUT", qty=10, price=80000, when="2026-11-09 10:21:00"),
        {
            "transactionType": "SELL",
            "tradingSymbol": "COPPER25NOVFUT",
            "tradedQuantity": 1,
            "tradedPrice": 800,
            "exchangeTime": "2026-11-09 11:30:00",
            "exchangeTradeId": "copper-bare",
        },
    ]
    fills = [fill for fill in (row_to_fill(row, DAY) for row in rows) if fill]
    inserted = []
    result = sync_from_broker(
        "2026-11-09",
        "dhan",
        fills=fills,
        existing=[],
        insert_fn=inserted.append,
    )
    assert sorted(row["symbol"] for row in inserted) == ["NIFTY", "RELIANCE"]
    assert result["inserted"] == 2
    assert all(row["source"] == "broker_dhan" for row in inserted)


def test_dedup_skips_same_instrument_datetime_and_entry_price():
    fill = row_to_fill(_dhan("NSE_FNO", "NIFTY-Nov2026-FUT"), DAY)
    assert fill is not None
    existing = [
        {
            "id": 9,
            "session_date": "2026-11-09",
            "symbol": "NIFTY",
            "contract": "NIFTY-Nov2026-FUT",
            "direction": "LONG",
            "entry_time": "10:15:03",
            "entry_price": 24550.5,
            "exit_time": None,
            "exit_price": None,
        }
    ]
    inserted = []
    result = sync_from_broker(
        "09/11/2026",
        "dhan",
        fills=[fill],
        existing=existing,
        insert_fn=inserted.append,
    )
    assert inserted == []
    assert result["inserted"] == 0
    assert result["skipped"] == 1
    assert "already in the trade log" in result["message"]


def test_new_futures_trade_is_inserted():
    fill = row_to_fill(
        _dhan("NSE_FNO", "RELIANCE25NOVFUT", qty=250, price=1280.25, when="2026-11-09 09:20:01"),
        DAY,
    )
    assert fill is not None
    inserted = []
    result = sync_from_broker(
        "2026-11-09",
        "upstox",
        fills=[fill],
        existing=[],
        insert_fn=inserted.append,
    )
    assert result["inserted"] == 1
    assert result["skipped"] == 0
    assert result["open_trades"] == 1
    assert len(inserted) == 1
    row = inserted[0]
    assert row["symbol"] == "RELIANCE"
    assert row["contract"] == "RELIANCE25NOVFUT"
    assert row["direction"] == "LONG"
    assert row["qty"] == 250
    assert row["entry_time"] == "09:20:01"
    assert row["entry_price"] == 1280.25
    assert row["exit_time"] is None
    assert row["exit_price"] is None
    assert row["source"] == "broker_upstox"


def test_round_trip_stores_entry_and_exit():
    buy = row_to_fill(
        _dhan("NSE_FNO", "BANKNIFTY-Nov2026-FUT", qty=15, price=52000, when="2026-11-09 10:00:00"),
        DAY,
    )
    sell = row_to_fill(
        _dhan(
            "NSE_FNO",
            "BANKNIFTY-Nov2026-FUT",
            side="SELL",
            qty=15,
            price=52150,
            when="2026-11-09 14:10:00",
            exchangeTradeId="bn-sell",
        ),
        DAY,
    )
    inserted = []
    result = sync_from_broker(
        "2026-11-09",
        "dhan",
        fills=[buy, sell],
        existing=[],
        insert_fn=inserted.append,
    )
    assert result["inserted"] == 1
    assert result["closed_trades"] == 1
    row = inserted[0]
    assert row["symbol"] == "BANKNIFTY"
    assert row["direction"] == "LONG"
    assert row["entry_time"] == "10:00:00"
    assert row["entry_price"] == 52000
    assert row["exit_time"] == "14:10:00"
    assert row["exit_price"] == 52150
    assert row["qty"] == 15


def test_nov9_screenshot_pairs_by_execution_time_and_keeps_intraday():
    """Dhan screen order is not time order. Margin and Intraday NSE futures all import.

    List order matches the 09 Nov 2026 Dhan futures screen (sell TCS first).
    Exchange timestamps are the ones returned by get_trade_history: the earlier
    fill is the entry, so TCS is LONG even though the sell row is listed first.
    """
    cnc = row_to_fill(
        _dhan("NSE_FNO", "RELIANCE25NOVFUT", productType="CNC", qty=250, price=1280),
        DAY,
    )
    assert cnc is not None

    rows = [
        _dhan(
            "NSE_FNO",
            "TCS NOV FUT NSE",
            side="SELL",
            qty=225,
            price=2480.30,
            when="2026-11-09T12:16:46",
            productType="MARGIN",
            securityId="61724",
        ),
        _dhan(
            "NSE_FNO",
            "TCS NOV FUT NSE",
            side="BUY",
            qty=225,
            price=2485.00,
            when="2026-11-09T11:41:42",
            productType="MARGIN",
            securityId="61724",
        ),
        _dhan(
            "NSE_FNO",
            "AMBER NOV FUT NSE",
            side="BUY",
            qty=100,
            price=6136.50,
            when="2026-11-09T10:10:15",
            productType="INTRADAY",
            securityId="61483",
        ),
        _dhan(
            "NSE_FNO",
            "AMBER NOV FUT NSE",
            side="SELL",
            qty=100,
            price=6085.00,
            when="2026-11-09T09:59:54",
            productType="INTRADAY",
            securityId="61483",
        ),
        _dhan(
            "NSE_FNO",
            "LAURUSLABS NOV FUT NSE",
            side="SELL",
            qty=425,
            price=2423.10,
            when="2026-11-09T09:33:52",
            productType="INTRADAY",
            securityId="61629",
        ),
        _dhan(
            "NSE_FNO",
            "SBIN NOV FUT NSE",
            side="SELL",
            qty=750,
            price=1038.25,
            when="2026-11-09T09:33:11",
            productType="INTRADAY",
            securityId="61706",
        ),
        _dhan(
            "NSE_FNO",
            "LAURUSLABS NOV FUT NSE",
            side="BUY",
            qty=425,
            price=2412.50,
            when="2026-11-09T09:29:41",
            productType="INTRADAY",
            securityId="61629",
        ),
        _dhan(
            "NSE_FNO",
            "SBIN NOV FUT NSE",
            side="BUY",
            qty=750,
            price=1034.55,
            when="2026-11-09T09:24:16",
            productType="INTRADAY",
            securityId="61706",
        ),
        _dhan(
            "MCX_COMM",
            "SILVERM NOV FUT",
            side="SELL",
            qty=5,
            price=189610,
            when="2026-11-09T16:49:48",
            productType="MARGIN",
        ),
    ]
    fills = [fill for fill in (row_to_fill(row, DAY) for row in rows) if fill]
    assert sorted(fill.underlying for fill in fills) == [
        "AMBER",
        "AMBER",
        "LAURUSLABS",
        "LAURUSLABS",
        "SBIN",
        "SBIN",
        "TCS",
        "TCS",
    ]
    inserted = []
    result = sync_from_broker(
        "2026-11-09",
        "dhan",
        fills=fills,
        existing=[],
        insert_fn=inserted.append,
    )
    by_symbol = {row["symbol"]: row for row in inserted}
    assert sorted(by_symbol) == ["AMBER", "LAURUSLABS", "SBIN", "TCS"]
    assert result["inserted"] == 4
    assert result["skipped"] == 0

    tcs = by_symbol["TCS"]
    assert tcs["direction"] == "LONG"
    assert tcs["entry_time"] == "11:41:42"
    assert tcs["entry_price"] == 2485.0
    assert tcs["exit_time"] == "12:16:46"
    assert tcs["exit_price"] == 2480.3
    assert tcs["qty"] == 225

    amber = by_symbol["AMBER"]
    assert amber["direction"] == "SHORT"
    assert amber["entry_time"] == "09:59:54"
    assert amber["entry_price"] == 6085.0
    assert amber["exit_time"] == "10:10:15"
    assert amber["exit_price"] == 6136.5
    assert amber["qty"] == 100

    laurus = by_symbol["LAURUSLABS"]
    assert laurus["direction"] == "LONG"
    assert laurus["entry_time"] == "09:29:41"
    assert laurus["entry_price"] == 2412.5
    assert laurus["exit_time"] == "09:33:52"
    assert laurus["exit_price"] == 2423.1

    sbin = by_symbol["SBIN"]
    assert sbin["direction"] == "LONG"
    assert sbin["entry_time"] == "09:24:16"
    assert sbin["entry_price"] == 1034.55
    assert sbin["exit_time"] == "09:33:11"
    assert sbin["exit_price"] == 1038.25


def test_broker_row_with_wrong_entry_is_updated_manual_row_is_kept():
    rows = [
        _dhan(
            "NSE_FNO",
            "TCS NOV FUT",
            side="SELL",
            qty=225,
            price=2480.30,
            when="2026-11-09T12:16:46",
            productType="MARGIN",
            securityId="61724",
        ),
        _dhan(
            "NSE_FNO",
            "TCS NOV FUT",
            side="BUY",
            qty=225,
            price=2485.00,
            when="2026-11-09T11:41:42",
            productType="MARGIN",
            securityId="61724",
        ),
        _dhan(
            "NSE_FNO",
            "SBIN NOV FUT",
            side="BUY",
            qty=750,
            price=1034.55,
            when="2026-11-09T09:24:16",
            productType="INTRADAY",
            securityId="61706",
        ),
        _dhan(
            "NSE_FNO",
            "SBIN NOV FUT",
            side="SELL",
            qty=750,
            price=1038.25,
            when="2026-11-09T09:33:11",
            productType="INTRADAY",
            securityId="61706",
        ),
    ]
    fills = [fill for fill in (row_to_fill(row, DAY) for row in rows) if fill]
    existing = [
        {
            "id": 191,
            "session_date": "2026-11-09",
            "symbol": "TCS",
            "contract": "TCS FUT 23 NOV 26",
            "direction": "SHORT",
            "qty": 225,
            "entry_time": "12:16:46",
            "entry_price": 2480.3,
            "exit_time": None,
            "exit_price": None,
            "source": "broker_dhan",
        },
        {
            "id": 50,
            "session_date": "2026-11-09",
            "symbol": "SBIN",
            "contract": "SBIN",
            "direction": "SHORT",
            "qty": 1,
            "entry_time": "09:00:00",
            "entry_price": 1000,
            "exit_time": None,
            "exit_price": None,
            "source": "tradelog_ui",
        },
    ]
    updated = []
    inserted = []

    def update_fn(row, plan):
        updated.append(
            (
                row["id"],
                plan.underlying,
                plan.direction,
                plan.entry_time,
                plan.entry_price,
                plan.exit_time,
                plan.exit_price,
            )
        )

    result = sync_from_broker(
        "2026-11-09",
        "dhan",
        fills=fills,
        existing=existing,
        insert_fn=inserted.append,
        update_fn=update_fn,
    )
    assert updated == [(191, "TCS", "LONG", "11:41:42", 2485.0, "12:16:46", 2480.3)]
    assert [row["symbol"] for row in inserted] == ["SBIN"]
    assert inserted[0]["direction"] == "LONG"
    assert inserted[0]["entry_price"] == 1034.55
    assert result["updated"] == 1
    assert result["inserted"] == 1
    assert updated[0][0] != 50

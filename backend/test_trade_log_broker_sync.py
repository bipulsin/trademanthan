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


def test_futures_filter_keeps_stock_and_index_and_drops_the_rest():
    nifty = row_to_fill(_dhan("NSE_FNO", "NIFTY-Nov2026-FUT", qty=65, price=24550.5), DAY)
    reliance = row_to_fill(
        _dhan("NSE_FNO", "RELIANCE25NOVFUT", qty=250, price=1280.5, when="2026-11-09 10:20:00"),
        DAY,
    )
    sensex = row_to_fill(
        _dhan("BSE_FNO", "SENSEX-Nov2026-FUT", qty=10, price=80000, when="2026-11-09 10:21:00"),
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

    assert nifty is not None and nifty.underlying == "NIFTY" and nifty.side == "BUY"
    assert reliance is not None and reliance.underlying == "RELIANCE"
    assert sensex is not None and sensex.underlying == "SENSEX"
    assert banknifty is not None and banknifty.underlying == "BANKNIFTY" and banknifty.side == "SELL"
    assert option_call is None
    assert option_symbol is None
    assert opt_product is None
    assert equity is None
    assert mcx is None
    assert currency is None


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

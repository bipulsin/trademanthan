"""Commodities Div broker sync: MCX filter, Buy/Sell pairing, idempotent reconcile."""
from __future__ import annotations

from datetime import date, datetime

from backend.services.commodities_div.broker_sync import (
    Fill,
    _row_to_fill,
    plans_from_fills,
    reconcile,
    scale_traded_qty,
)


DAY = date(2026, 11, 9)


def _fill(side, qty, price, minute, symbol="CRUDEOIL25NOVFUT", fill_id=None, exchange_ok=True):
    return Fill(
        instrument=symbol,
        symbol_raw=symbol,
        side=side,
        qty=qty,
        price=price,
        ts=datetime(2026, 11, 9, 10, minute, 0),
        fill_id=fill_id or f"{side}-{minute}-{qty}",
        instrument_key="MCX_FO|1" if exchange_ok else None,
    )


def test_only_buy_is_open_bull():
    plans = plans_from_fills([_fill("BUY", 100, 5500, 5)], broker="dhan", trade_day=DAY)
    assert len(plans) == 1
    assert plans[0].open_trade is True
    assert plans[0].direction == "BULL"
    assert plans[0].entry_price == 5500
    assert plans[0].symbol_mapped == "CRUDEOIL"
    assert plans[0].qty == 100


def test_only_sell_is_open_bear():
    plans = plans_from_fills([_fill("SELL", 100, 80, 6, symbol="NATURALGAS25NOVFUT")], broker="upstox", trade_day=DAY)
    assert len(plans) == 1
    assert plans[0].open_trade is True
    assert plans[0].direction == "BEAR"
    assert plans[0].symbol_mapped == "NATURALGAS"


def test_round_trip_buy_then_sell_is_history():
    plans = plans_from_fills(
        [_fill("BUY", 100, 5500, 5, fill_id="b1"), _fill("SELL", 100, 5600, 20, fill_id="s1")],
        broker="dhan",
        trade_day=DAY,
    )
    assert len(plans) == 1
    assert plans[0].open_trade is False
    assert plans[0].direction == "BULL"
    assert plans[0].entry_price == 5500
    assert plans[0].exit_price == 5600
    assert plans[0].qty == 100


def test_short_cover_is_bear_history():
    plans = plans_from_fills(
        [_fill("SELL", 50, 100, 1, fill_id="s"), _fill("BUY", 50, 90, 2, fill_id="b")],
        broker="upstox",
        trade_day=DAY,
    )
    assert len(plans) == 1
    assert plans[0].open_trade is False
    assert plans[0].direction == "BEAR"
    assert plans[0].entry_price == 100
    assert plans[0].exit_price == 90


def test_partial_close_keeps_remainder_in_trade():
    plans = plans_from_fills(
        [_fill("BUY", 100, 10, 1, fill_id="b"), _fill("SELL", 40, 12, 2, fill_id="s")],
        broker="dhan",
        trade_day=DAY,
    )
    assert len(plans) == 2
    closed = next(p for p in plans if not p.open_trade)
    opened = next(p for p in plans if p.open_trade)
    assert closed.qty == 40
    assert closed.entry_price == 10
    assert closed.exit_price == 12
    assert opened.qty == 60
    assert opened.direction == "BULL"


def test_reentry_after_round_trip_stays_open():
    plans = plans_from_fills(
        [
            _fill("BUY", 10, 5, 1, fill_id="b1"),
            _fill("SELL", 10, 6, 2, fill_id="s1"),
            _fill("BUY", 4, 7, 3, fill_id="b2"),
        ],
        broker="dhan",
        trade_day=DAY,
    )
    assert len(plans) == 2
    assert any(not p.open_trade and p.qty == 10 for p in plans)
    opened = next(p for p in plans if p.open_trade)
    assert opened.qty == 4
    assert opened.entry_price == 7


def test_resync_same_key_skips():
    plans = plans_from_fills(
        [_fill("BUY", 100, 5500, 5, fill_id="b1"), _fill("SELL", 100, 5600, 20, fill_id="s1")],
        broker="dhan",
        trade_day=DAY,
    )
    again = plans_from_fills(
        [_fill("BUY", 100, 5500, 5, fill_id="b1"), _fill("SELL", 100, 5600, 20, fill_id="s1")],
        broker="dhan",
        trade_day=DAY,
    )
    assert plans[0].sync_key == again[0].sync_key
    existing = [
        {
            "id": 7,
            "symbol_mapped": "CRUDEOIL",
            "symbol_raw": "CRUDEOIL25NOVFUT",
            "contract": "CRUDEOIL25NOVFUT",
            "direction": "BULL",
            "status": "History",
            "entry_price": 5500,
            "exit_price": 5600,
            "trade_taken_at": datetime(2026, 11, 9, 10, 5, 0),
            "exit_at": datetime(2026, 11, 9, 10, 20, 0),
            "lot_size": 100,
            "broker_sync_key": plans[0].sync_key,
        }
    ]
    result = reconcile(again, existing, broker="dhan", trade_day=DAY)
    assert result["inserts"] == []
    assert result["updates"] == []
    assert result["deletes"] == []
    assert len(result["skips"]) == 1
    assert result["skips"][0]["stamp"] is False


def test_open_workspace_row_is_updated_not_duplicated():
    plans = plans_from_fills([_fill("BUY", 100, 5500, 5, fill_id="b1")], broker="upstox", trade_day=DAY)
    existing = [
        {
            "id": 3,
            "symbol_mapped": "CRUDEOIL",
            "symbol_raw": "CRUDEOIL",
            "contract": "CRUDEOIL25NOVFUT",
            "direction": "BULL",
            "status": "In-Trade",
            "entry_price": 5400,
            "exit_price": None,
            "trade_taken_at": datetime(2026, 11, 9, 9, 30, 0),
            "lot_size": 100,
            "broker_sync_key": None,
        }
    ]
    result = reconcile(plans, existing, broker="upstox", trade_day=DAY)
    assert result["inserts"] == []
    assert len(result["updates"]) == 1
    assert result["updates"][0]["id"] == 3
    assert result["updates"][0]["close"] is False


def test_mcx_filter_drops_nse_and_keeps_dhan_mcx():
    nse = _row_to_fill(
        {
            "exchangeSegment": "NSE_EQ",
            "transactionType": "BUY",
            "tradingSymbol": "RELIANCE",
            "tradedQuantity": 1,
            "tradedPrice": 100,
            "exchangeTime": "2026-11-09 10:00:00",
            "exchangeTradeId": "nse1",
        },
        DAY,
    )
    mcx = _row_to_fill(
        {
            "exchangeSegment": "MCX_COMM",
            "transactionType": "SELL",
            "tradingSymbol": "GOLDM25DECFUT",
            "tradedQuantity": 1,
            "tradedPrice": 72000,
            "exchangeTime": "2026-11-09 11:00:00",
            "exchangeTradeId": "mcx1",
        },
        DAY,
    )
    assert nse is None
    assert mcx is not None
    assert mcx.side == "SELL"
    assert mcx.symbol_raw == "GOLDM25DECFUT"


def test_scale_lot_versus_units():
    assert scale_traded_qty(1, 100) == 100
    assert scale_traded_qty(100, 100) == 100
    assert scale_traded_qty(2, 1250) == 2500
    assert scale_traded_qty(1250, 1250) == 1250

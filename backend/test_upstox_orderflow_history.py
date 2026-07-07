"""WS orderflow history must not break latest-snapshot consumers."""
from unittest.mock import MagicMock, patch

from backend.services.upstox_market_feed import (
    _persist_orderflow_1m,
    _persist_orderflow_latest,
    _persist_ws_1m_candle,
)


def test_persist_ws_1m_also_calls_orderflow_1m():
    candle = {
        "open": 100.0,
        "high": 100.5,
        "low": 99.5,
        "close": 100.2,
        "oi_open": 1000,
        "oi_high": 1100,
        "oi_low": 990,
        "oi_close": 1050,
        "volume": 500,
        "bid_depth_qty": 200,
        "ask_depth_qty": 100,
        "tbq": 300,
        "tsq": 200,
        "candle_source": "ltp_tick",
    }
    with patch("backend.services.upstox_market_feed.SessionLocal") as mock_sl, patch(
        "backend.services.upstox_market_feed._persist_orderflow_1m"
    ) as mock_of1m:
        db = MagicMock()
        mock_sl.return_value = db
        _persist_ws_1m_candle("NSE_FO|1", "2026-07-07T09:30:00+05:30", candle)
        db.execute.assert_called()
        db.commit.assert_called()
        mock_of1m.assert_called_once_with("NSE_FO|1", "2026-07-07T09:30:00+05:30", candle)


def test_persist_orderflow_latest_unchanged_shape():
    fields = {
        "bid_depth_qty": 10,
        "ask_depth_qty": 5,
        "depth_imbalance_ratio": 2.0,
        "tbq": 100,
        "tsq": 50,
        "pressure_ratio": 2.0,
        "oi": 999,
        "ltp": 100.0,
        "oi_change": 5,
    }
    with patch("backend.services.upstox_market_feed.SessionLocal") as mock_sl:
        db = MagicMock()
        mock_sl.return_value = db
        _persist_orderflow_latest("NSE_FO|1", fields)
        sql = db.execute.call_args[0][0].text
        assert "upstox_ws_orderflow_latest" in sql
        db.commit.assert_called()


def test_persist_orderflow_1m_writes_archive_table():
    candle = {
        "close": 100.0,
        "oi_open": 1000,
        "oi_close": 1100,
        "bid_depth_qty": 80,
        "ask_depth_qty": 40,
        "tbq": 200,
        "tsq": 100,
    }
    with patch("backend.services.upstox_market_feed.SessionLocal") as mock_sl:
        db = MagicMock()
        mock_sl.return_value = db
        _persist_orderflow_1m("NSE_FO|1", "2026-07-07T09:30:00+05:30", candle)
        sql = db.execute.call_args[0][0].text
        assert "upstox_ws_orderflow_1m" in sql

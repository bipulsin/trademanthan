"""Breakfast live scheduler, Upstox gate, and freeze persistence tests."""
from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytz
import pytest

from backend.services.breakfast_upstox_gate import (
    PRIORITY_WINDOW_END,
    PRIORITY_WINDOW_START,
    breakfast_upstox_allowed,
    breakfast_upstox_priority_owner,
    is_breakfast_priority_window,
    reset_blocked_log_cache,
)
from backend.services.breakfast_strategy.live_tick import (
    TICK_MINUTES,
    _upto_hhmm_for_tick,
    reset_session_cache_for_tests,
    run_breakfast_freeze_lock,
    run_breakfast_minute_tick,
)

IST = pytz.timezone("Asia/Kolkata")


@pytest.fixture(autouse=True)
def _reset_gate_and_cache():
    reset_blocked_log_cache()
    reset_session_cache_for_tests()
    yield
    reset_blocked_log_cache()
    reset_session_cache_for_tests()


def test_priority_window_boundaries():
    inside = IST.localize(datetime(2026, 8, 31, 9, 17, 0))
    before = IST.localize(datetime(2026, 8, 31, 9, 14, 59))
    after = IST.localize(datetime(2026, 8, 31, 9, 20, 31))
    assert PRIORITY_WINDOW_START.hour == 9 and PRIORITY_WINDOW_START.minute == 15
    assert PRIORITY_WINDOW_END.hour == 9 and PRIORITY_WINDOW_END.minute == 20
    assert is_breakfast_priority_window(inside)
    assert not is_breakfast_priority_window(before)
    assert not is_breakfast_priority_window(after)


def test_gate_blocks_non_breakfast_owner():
    now = IST.localize(datetime(2026, 8, 31, 9, 17, 0))
    with patch("backend.services.breakfast_upstox_gate._now_ist", return_value=now):
        assert not breakfast_upstox_allowed(caller="kavach")


def test_gate_allows_breakfast_owner():
    now = IST.localize(datetime(2026, 8, 31, 9, 17, 0))
    with patch("backend.services.breakfast_upstox_gate._now_ist", return_value=now):
        with breakfast_upstox_priority_owner():
            assert breakfast_upstox_allowed(caller="breakfast")


def test_upto_hhmm_for_tick_minute_close():
    assert _upto_hhmm_for_tick(16) == (9, 17)
    assert _upto_hhmm_for_tick(19) == (9, 20)
    assert _upto_hhmm_for_tick(20) == (9, 21)


def test_tick_minutes_constant():
    assert TICK_MINUTES == (16, 17, 18, 19, 20)


@patch("backend.services.breakfast_strategy.live_tick._is_trading_day", return_value=True)
@patch("backend.services.breakfast_strategy.live_tick.fetch_1m_parallel")
@patch("backend.services.breakfast_strategy.live_tick.load_arbitrage_by_sector")
@patch("backend.services.breakfast_strategy.live_tick.build_instrument_indexes")
@patch("backend.services.breakfast_strategy.live_tick.select_breakfast_picks")
def test_tick_re_picks_sectors_each_minute(
    mock_select,
    mock_indexes,
    mock_load_sector,
    mock_fetch,
    _trading,
):
    mock_load_sector.return_value = {"NSE_FO|BANK": [{"stock": "HDFCBANK"}]}
    mock_indexes.return_value = ({}, {})
    mock_fetch.return_value = {"NSE_INDEX|Nifty 50": [{"timestamp": "t", "open": 1, "high": 1, "low": 1, "close": 1}]}
    mock_select.return_value = MagicMock(
        long_side=True,
        nifty_bias="positive",
        nifty_bias_pct=0.1,
        ranked_sectors=[],
        sector_picks=[],
    )

    with patch(
        "backend.services.breakfast_strategy.live_tick._rank_picked_sectors",
        return_value=(["NSE_FO|BANK"], True),
    ), patch(
        "backend.services.breakfast_strategy.live_tick._resolve_stock_keys",
        return_value=({"NSE_FO|BANK": ["HDFCBANK"]}, ["NSE_FO|HDFC"]),
    ), patch(
        "backend.services.breakfast_strategy.live_tick.forming_bar_from_1m_upto",
        return_value={"open": 1, "high": 1, "low": 1, "close": 1},
    ), patch(
        "backend.services.breakfast_strategy.live_tick._build_stock_overrides_from_1m",
        return_value=({}, {}),
    ):
        out = run_breakfast_minute_tick(16)
        assert out["ok"]
        assert mock_fetch.call_count >= 2


@patch("backend.services.breakfast_strategy.live_tick.run_breakfast_minute_tick")
@patch("backend.services.breakfast_strategy.live_tick.persist_live_signals")
@patch("backend.services.breakfast_strategy.live_tick.persist_session_lock")
@patch("backend.services.breakfast_strategy.live_tick.fetch_session_lock", return_value=None)
@patch("backend.services.breakfast_strategy.live_tick._is_trading_day", return_value=True)
def test_freeze_persists_lock_and_signals(mock_trading, _lock, mock_persist_lock, mock_persist_sig, mock_tick):
    mock_tick.return_value = {"ok": True}
    snap = {
        "session_date": "2026-08-31",
        "sectors": [{"sector_label": "Bank", "stocks": [{"symbol": "HDFCBANK"}]}],
        "nifty": {"bias_pct": 0.1},
    }
    with patch("backend.services.breakfast_strategy.live_tick.get_live_tick_snapshot", return_value=snap):
        out = run_breakfast_freeze_lock()
    assert out["lock_status"] == "locked"
    mock_persist_sig.assert_called_once()
    mock_persist_lock.assert_called_once()


@patch("backend.services.breakfast_strategy.live_tick.run_breakfast_minute_tick")
@patch("backend.services.breakfast_strategy.live_tick.persist_session_lock")
@patch("backend.services.breakfast_strategy.live_tick.fetch_session_lock", return_value=None)
@patch("backend.services.breakfast_strategy.live_tick._is_trading_day", return_value=True)
def test_freeze_failure_ui_when_no_sectors(_trading, _lock, mock_persist_lock, mock_tick):
    mock_tick.return_value = {"ok": True}
    with patch(
        "backend.services.breakfast_strategy.live_tick.get_live_tick_snapshot",
        return_value={"session_date": "2026-08-31", "sectors": [], "nifty": {}},
    ):
        out = run_breakfast_freeze_lock()
    assert out["lock_status"] == "failed"
    mock_persist_lock.assert_called()

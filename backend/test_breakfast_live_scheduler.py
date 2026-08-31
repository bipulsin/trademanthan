"""Breakfast live scheduler, Upstox gate, and freeze persistence tests."""
from __future__ import annotations

from datetime import date, datetime
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
    BREAKFAST_WS_1M_STALE_SEC,
    TICK_MINUTES,
    _merge_today_ws_with_cached,
    _resolve_candles_ws_primary,
    _upto_hhmm_for_tick,
    _ws_usable_for_forming,
    reset_session_cache_for_tests,
    run_breakfast_freeze_lock,
    run_breakfast_minute_tick,
    run_breakfast_ws_warmup,
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


def test_breakfast_ws_stale_sec_is_90():
    assert BREAKFAST_WS_1M_STALE_SEC == 90.0


def test_merge_today_ws_with_cached_keeps_prior_sessions():
    session = date(2026, 9, 1)
    cached = [{"timestamp": "2026-08-29T15:15:00+05:30", "open": 1, "close": 1}]
    ws = [{"timestamp": "2026-09-01T09:15:00+05:30", "open": 2, "high": 2, "low": 2, "close": 2}]
    merged = _merge_today_ws_with_cached(ws, cached, session)
    assert len(merged) == 2
    assert merged[0]["timestamp"].startswith("2026-08-29")
    assert merged[1]["timestamp"].startswith("2026-09-01")


@patch("backend.services.breakfast_strategy.live_tick.load_cached_1m", return_value=[])
@patch("backend.services.upstox_market_feed.get_ws_feed_row")
def test_ws_usable_rejects_stale_feed(mock_feed, _cache):
    from pathlib import Path

    mock_feed.return_value = {"age_sec": 95.0}
    ok, reason = _ws_usable_for_forming(
        "NSE_INDEX|Nifty 50",
        [{"timestamp": "2026-09-01T09:15:00+05:30", "open": 1, "high": 1, "low": 1, "close": 1}],
        session_date=date(2026, 9, 1),
        upto_hhmm=(9, 17),
        cache_dir=Path("/tmp"),
    )
    assert not ok
    assert reason and reason.startswith("ws_feed_stale_")


@patch("backend.services.breakfast_strategy.live_tick.fetch_1m_parallel")
@patch("backend.services.breakfast_strategy.live_tick.load_cached_1m", return_value=[])
@patch("backend.services.upstox_market_feed.get_ws_1m_bars_for_session")
@patch("backend.services.upstox_market_feed.get_ws_feed_row")
def test_resolve_candles_ws_primary_fallback_per_instrument(mock_feed, mock_ws_bars, _cache, mock_fetch):
    from pathlib import Path

    session = date(2026, 9, 1)
    bar = {"timestamp": "2026-09-01T09:15:00+05:30", "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1}
    mock_ws_bars.return_value = [bar]
    mock_feed.side_effect = [
        {"age_sec": 1.0},
        None,
    ]
    mock_fetch.return_value = {"NSE_FO|BANK": [bar]}

    candles, log = _resolve_candles_ws_primary(
        MagicMock(),
        Path("/tmp"),
        ["NSE_INDEX|Nifty 50", "NSE_FO|BANK"],
        session_date=session,
        upto_hhmm=(9, 17),
        tick_minute=16,
    )
    assert "NSE_INDEX|Nifty 50" in candles
    assert "NSE_FO|BANK" in candles
    assert mock_fetch.call_count == 1
    sources = {r["instrument_key"]: r["source"] for r in log}
    assert sources["NSE_INDEX|Nifty 50"] == "ws"
    assert sources["NSE_FO|BANK"] == "rest_fallback"


@patch("backend.services.upstox_market_feed.ensure_market_feed_running")
@patch("backend.services.breakfast_strategy.live_tick._warmup_instrument_keys", return_value=["k1", "k2"])
@patch("backend.services.breakfast_strategy.live_tick._is_trading_day", return_value=True)
def test_ws_warmup_starts_feed(_trading, _keys, mock_ensure):
    out = run_breakfast_ws_warmup()
    assert out["ok"]
    assert out["instrument_count"] == 2
    mock_ensure.assert_called_once_with(["k1", "k2"])


@patch("backend.services.breakfast_strategy.live_tick._is_trading_day", return_value=True)
@patch("backend.services.breakfast_strategy.live_tick._resolve_candles_ws_primary")
@patch("backend.services.breakfast_strategy.live_tick.load_arbitrage_by_sector")
@patch("backend.services.breakfast_strategy.live_tick.build_instrument_indexes")
@patch("backend.services.breakfast_strategy.live_tick.select_breakfast_picks")
def test_tick_re_picks_sectors_each_minute(
    mock_select,
    mock_indexes,
    mock_load_sector,
    mock_resolve,
    _trading,
):
    mock_load_sector.return_value = {"NSE_FO|BANK": [{"stock": "HDFCBANK"}]}
    mock_indexes.return_value = ({}, {})
    mock_resolve.return_value = (
        {"NSE_INDEX|Nifty 50": [{"timestamp": "t", "open": 1, "high": 1, "low": 1, "close": 1}]},
        [{"minute": 16, "instrument_key": "NSE_INDEX|Nifty 50", "source": "ws", "reason": None}],
    )
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
        assert mock_resolve.call_count >= 2
        assert out.get("data_source") == "ws_1m"


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

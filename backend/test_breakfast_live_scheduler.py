"""Breakfast live scheduler, Upstox gate, and freeze persistence tests."""
from __future__ import annotations

from datetime import date, datetime
from unittest.mock import MagicMock, patch

import pytz
import pytest

from backend.services.breakfast_upstox_gate import (
    PRIORITY_WINDOW_END,
    PRIORITY_WINDOW_START,
    breakfast_exclusivity_active,
    breakfast_upstox_allowed,
    breakfast_upstox_priority_owner,
    defer_job_for_breakfast_exclusivity,
    is_breakfast_priority_window,
    reset_blocked_log_cache,
)
from backend.services.breakfast_strategy.live_tick import (
    BREAKFAST_WS_1M_STALE_SEC,
    FREEZE_AT,
    SCHEDULER_TICK_MINUTES,
    TICK_MINUTES,
    _merge_today_ws_with_cached,
    _resolve_candles_rest_5m,
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


@patch("backend.services.breakfast_upstox_gate._today_freeze_lock_status", return_value=None)
def test_exclusivity_from_910_until_lock(_lock):
    at_910 = IST.localize(datetime(2026, 8, 31, 9, 10, 0))
    at_909 = IST.localize(datetime(2026, 8, 31, 9, 9, 59))
    at_921 = IST.localize(datetime(2026, 8, 31, 9, 21, 0))
    assert breakfast_exclusivity_active(at_910)
    assert not breakfast_exclusivity_active(at_909)
    assert breakfast_exclusivity_active(at_921)
    assert not is_breakfast_priority_window(at_921)


@patch("backend.services.breakfast_upstox_gate._today_freeze_lock_status", return_value="locked")
def test_exclusivity_ends_when_lock_locked(_lock):
    at_921 = IST.localize(datetime(2026, 8, 31, 9, 21, 0))
    assert not breakfast_exclusivity_active(at_921)


@patch("backend.services.breakfast_upstox_gate._today_freeze_lock_status", return_value="failed")
def test_exclusivity_ends_when_lock_failed(_lock):
    at_925 = IST.localize(datetime(2026, 8, 31, 9, 25, 0))
    assert not breakfast_exclusivity_active(at_925)


@patch("backend.services.breakfast_upstox_gate._today_freeze_lock_status", return_value=None)
def test_exclusivity_hard_ceiling_0925_without_lock(_lock):
    at_924 = IST.localize(datetime(2026, 8, 31, 9, 24, 59))
    at_925 = IST.localize(datetime(2026, 8, 31, 9, 25, 0))
    assert breakfast_exclusivity_active(at_924)
    assert not breakfast_exclusivity_active(at_925)


@patch("backend.services.breakfast_upstox_gate._today_freeze_lock_status", return_value="locked")
def test_exclusivity_ends_when_lock_locked_at_920(_lock):
    at_920 = IST.localize(datetime(2026, 8, 31, 9, 20, 0))
    assert not breakfast_exclusivity_active(at_920)


@patch("backend.services.breakfast_upstox_gate._today_freeze_lock_status", return_value=None)
def test_gate_blocks_during_exclusivity_before_915(_lock):
    now = IST.localize(datetime(2026, 8, 31, 9, 12, 0))
    with patch("backend.services.breakfast_upstox_gate._now_ist", return_value=now):
        assert not breakfast_upstox_allowed(caller="kavach")
        assert defer_job_for_breakfast_exclusivity("centralized_market_data_10m")


@patch("backend.services.breakfast_upstox_gate._today_freeze_lock_status", return_value="locked")
def test_gate_allows_after_lock_outside_priority_window(_lock):
    now = IST.localize(datetime(2026, 8, 31, 9, 21, 0))
    with patch("backend.services.breakfast_upstox_gate._now_ist", return_value=now):
        assert breakfast_upstox_allowed(caller="kavach")
        assert not defer_job_for_breakfast_exclusivity("centralized_market_data_10m")


def test_gate_blocks_non_breakfast_owner():
    now = IST.localize(datetime(2026, 8, 31, 9, 17, 0))
    with patch("backend.services.breakfast_upstox_gate._now_ist", return_value=now):
        assert not breakfast_upstox_allowed(caller="kavach")


def test_gate_allows_breakfast_owner():
    now = IST.localize(datetime(2026, 8, 31, 9, 17, 0))
    with patch("backend.services.breakfast_upstox_gate._now_ist", return_value=now):
        with breakfast_upstox_priority_owner():
            assert breakfast_upstox_allowed(caller="breakfast")


def test_owner_contextvar_copied_to_thread_pool():
    import contextvars
    from concurrent.futures import ThreadPoolExecutor

    from backend.services.breakfast_upstox_gate import breakfast_priority_owner_active

    def _worker() -> bool:
        return breakfast_priority_owner_active()

    with breakfast_upstox_priority_owner():
        ctx = contextvars.copy_context()
        with ThreadPoolExecutor(max_workers=2) as pool:
            assert pool.submit(ctx.run, _worker).result() is True
    assert breakfast_priority_owner_active() is False


def test_upto_hhmm_for_tick_minute_close():
    assert _upto_hhmm_for_tick(16) == (9, 17)
    assert _upto_hhmm_for_tick(19) == (9, 20)
    assert _upto_hhmm_for_tick(20) == (9, 21)


def test_tick_minutes_constant():
    assert TICK_MINUTES == (16, 17, 18, 19, 20)
    assert SCHEDULER_TICK_MINUTES == (16, 17, 18, 19)
    assert FREEZE_AT.hour == 9 and FREEZE_AT.minute == 20 and FREEZE_AT.second == 5


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
    assert sources["NSE_INDEX|Nifty 50"] == "ws_1m"
    assert sources["NSE_FO|BANK"] == "rest_1m"


@patch("backend.services.upstox_market_feed.ensure_market_feed_running")
@patch("backend.services.breakfast_strategy.live_tick._warmup_instrument_keys", return_value=["k1", "k2"])
@patch("backend.services.breakfast_strategy.live_tick._is_trading_day", return_value=True)
def test_ws_warmup_starts_feed(_trading, _keys, mock_ensure):
    out = run_breakfast_ws_warmup()
    assert out["ok"]
    assert out["instrument_count"] == 2
    mock_ensure.assert_called_once_with(["k1", "k2"])


@patch("backend.services.upstox_market_feed.feed_status", return_value={"universe_keys": 160})
@patch("backend.services.upstox_market_feed.ensure_market_feed_running")
@patch(
    "backend.services.breakfast_strategy.live_tick.breakfast_index_instrument_keys",
    return_value=["NSE_INDEX|Nifty 50"] + [f"NSE_INDEX|S{i}" for i in range(16)],
)
@patch("backend.services.breakfast_strategy.live_tick._is_trading_day", return_value=True)
def test_ws_resubscribe_915_unions_indexes(_trading, mock_keys, mock_ensure, _status):
    from backend.services.breakfast_strategy.live_tick import run_breakfast_ws_resubscribe_915

    out = run_breakfast_ws_resubscribe_915()
    assert out["ok"]
    assert out["index_keys_confirmed"] == 17
    mock_ensure.assert_called_once()
    args, kwargs = mock_ensure.call_args
    assert kwargs.get("union") is True
    assert len(args[0]) == 17


@patch("backend.services.breakfast_strategy.live_tick._is_trading_day", return_value=True)
@patch("backend.services.breakfast_strategy.live_tick._resolve_candles_ws_primary")
@patch("backend.services.breakfast_strategy.live_tick.load_arbitrage_by_sector")
@patch("backend.services.breakfast_strategy.live_tick.build_instrument_indexes")
@patch("backend.services.breakfast_strategy.live_tick.load_stored_prev_closes_and_wicks", return_value=({}, {}, {}))
@patch("backend.services.breakfast_strategy.live_tick.select_breakfast_picks_prevclose")
def test_tick_re_picks_sectors_each_minute(
    mock_select,
    _prev,
    mock_indexes,
    mock_load_sector,
    mock_resolve,
    _trading,
):
    mock_load_sector.return_value = {"NSE_FO|BANK": [{"stock": "HDFCBANK"}]}
    mock_indexes.return_value = ({}, {})
    mock_resolve.return_value = (
        {"NSE_INDEX|Nifty 50": [{"timestamp": "t", "open": 1, "high": 1, "low": 1, "close": 1}]},
        [{"minute": 16, "instrument_key": "NSE_INDEX|Nifty 50", "source": "ws_1m", "reason": None}],
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


def test_freeze_cron_trigger_timezone_is_ist():
    """CronTrigger must carry Asia/Kolkata explicitly; scheduler timezone is not inherited."""
    from backend.services.breakfast_strategy.live_scheduler import _freeze_cron_trigger

    trigger = _freeze_cron_trigger()
    tz_name = str(trigger.timezone)
    assert "Asia/Kolkata" in tz_name
    assert tz_name != "Etc/UTC"
    next_fire = trigger.get_next_fire_time(None, IST.localize(datetime(2026, 9, 1, 9, 0, 0)))
    assert next_fire is not None
    assert next_fire.hour == 9 and next_fire.minute == 20 and next_fire.second == 5


@patch("backend.services.breakfast_strategy.live_tick.run_breakfast_minute_tick")
@patch("backend.services.breakfast_strategy.live_tick.persist_session_lock")
@patch("backend.services.breakfast_strategy.live_tick.fetch_session_lock", return_value=None)
@patch("backend.services.breakfast_strategy.live_tick._is_trading_day", return_value=True)
def test_freeze_failure_ui_when_no_sectors(_trading, _lock, mock_persist_lock, mock_tick):
    from backend.services.breakfast_strategy import live as live_mod

    live_mod._FROZEN_STATE.clear()
    mock_tick.return_value = {"ok": True}
    with patch(
        "backend.services.breakfast_strategy.live_tick.get_live_tick_snapshot",
        return_value={"session_date": "2026-08-31", "sectors": [], "nifty": {}},
    ):
        out = run_breakfast_freeze_lock()
    assert out["lock_status"] == "failed"
    mock_persist_lock.assert_called()
    assert "2026-08-31" not in live_mod._FROZEN_STATE


@patch("backend.services.breakfast_strategy.live_tick.fetch_5m_parallel")
@patch("backend.services.breakfast_strategy.live_tick.fetch_1m_parallel")
def test_resolve_candles_freeze_uses_rest_5m_not_1m(mock_fetch, mock_5m, caplog):
    from pathlib import Path

    from backend.services.breakfast_strategy.live_tick import FREEZE_SOURCE_VALUES

    session = date(2026, 9, 1)
    bar_5m = {
        "timestamp": "2026-09-01T09:15:00+05:30",
        "open": 100,
        "high": 101,
        "low": 99,
        "close": 100.5,
        "volume": 10,
    }
    mock_5m.return_value = {"NSE_INDEX|Nifty 50": [bar_5m]}
    with caplog.at_level("INFO"):
        candles, log = _resolve_candles_rest_5m(
            MagicMock(),
            Path("/tmp"),
            ["NSE_INDEX|Nifty 50"],
            session_date=session,
            tick_minute=20,
        )
    mock_fetch.assert_not_called()
    assert candles["NSE_INDEX|Nifty 50"] == [bar_5m]
    assert log[0]["source"] == "rest_5m"
    assert log[0]["source"] in FREEZE_SOURCE_VALUES
    assert any("source=rest_5m" in r.message for r in caplog.records)


@patch("backend.services.breakfast_strategy.live_tick.fetch_5m_parallel")
@patch("backend.services.breakfast_strategy.live_tick.fetch_1m_parallel")
def test_resolve_candles_none_when_5m_empty(mock_fetch, mock_5m, caplog):
    from pathlib import Path

    mock_5m.return_value = {"NSE_INDEX|Nifty 50": []}
    with caplog.at_level("INFO"):
        _candles, log = _resolve_candles_rest_5m(
            MagicMock(),
            Path("/tmp"),
            ["NSE_INDEX|Nifty 50"],
            session_date=date(2026, 9, 1),
            tick_minute=20,
        )
    mock_fetch.assert_not_called()
    assert log[0]["source"] == "none"
    assert any("source=none" in r.message for r in caplog.records)


@patch("backend.services.breakfast_strategy.live_tick.fetch_1m_parallel")
@patch("backend.services.breakfast_strategy.live_tick._resolve_candles_ws_primary")
@patch("backend.services.breakfast_strategy.live_tick.fetch_5m_parallel")
@patch("backend.services.breakfast_strategy.live_tick._is_trading_day", return_value=True)
@patch("backend.services.breakfast_strategy.live_tick.load_arbitrage_by_sector")
@patch("backend.services.breakfast_strategy.live_tick.build_instrument_indexes")
@patch("backend.services.breakfast_strategy.live_tick._all_sector_keys", return_value=[])
@patch("backend.services.breakfast_strategy.live_tick.load_stored_prev_closes_and_wicks", return_value=({}, {}, {}))
@patch("backend.services.breakfast_strategy.live_tick.UpstoxService")
def test_minute_20_tick_does_not_call_1m(
    _ux,
    _prev,
    _sector_keys,
    mock_indexes,
    mock_load_sector,
    _trading,
    mock_5m,
    mock_ws_resolve,
    mock_fetch,
):
    mock_load_sector.return_value = {}
    mock_indexes.return_value = ({}, {})
    bar_5m = {
        "timestamp": "2026-09-01T09:15:00+05:30",
        "open": 100,
        "high": 101,
        "low": 99,
        "close": 100.5,
        "volume": 10,
    }
    mock_5m.return_value = {"NSE_INDEX|Nifty 50": [bar_5m]}
    with patch(
        "backend.services.breakfast_strategy.live_tick._rank_picked_sectors",
        return_value=([], True),
    ), patch(
        "backend.services.breakfast_strategy.live_tick._build_stock_overrides_from_1m",
        return_value=({}, {}),
    ), patch(
        "backend.services.breakfast_strategy.live_tick.select_breakfast_picks_prevclose",
        return_value=None,
    ), patch(
        "backend.services.breakfast_strategy.live_tick._now_ist",
        return_value=IST.localize(datetime(2026, 9, 1, 9, 20, 5)),
    ):
        out = run_breakfast_minute_tick(20)
    assert out["ok"]
    mock_ws_resolve.assert_not_called()
    mock_fetch.assert_not_called()
    assert mock_5m.call_count >= 1
    assert out.get("data_source") == "rest_5m"


def test_nifty_bias_unknown_not_long_when_missing_pct():
    from backend.services.breakfast_strategy.engine import nifty_bias_from_bar

    bar = {"open": 0, "high": 0, "low": 0, "close": 0}
    default_bias, _ = nifty_bias_from_bar(bar)
    assert default_bias == "positive"
    unknown, pct = nifty_bias_from_bar(bar, missing="unknown")
    assert unknown == "unknown"
    assert pct == 0.0


@patch("backend.services.breakfast_strategy.live_tick.fetch_1m_parallel")
@patch("backend.services.breakfast_strategy.live_tick._resolve_candles_ws_primary")
@patch("backend.services.breakfast_strategy.live_tick.fetch_5m_parallel")
@patch("backend.services.breakfast_strategy.live_tick._is_trading_day", return_value=True)
@patch("backend.services.breakfast_strategy.live_tick.load_arbitrage_by_sector")
@patch("backend.services.breakfast_strategy.live_tick.build_instrument_indexes")
@patch("backend.services.breakfast_strategy.live_tick._all_sector_keys", return_value=["NSE_INDEX|Nifty Bank"])
@patch("backend.services.breakfast_strategy.live_tick.load_stored_prev_closes_and_wicks", return_value=({}, {}, {}))
@patch("backend.services.breakfast_strategy.live_tick.UpstoxService")
def test_minute_20_fetches_selected_stocks_via_rest_5m(
    _ux,
    _prev,
    _sector_keys,
    mock_indexes,
    mock_load_sector,
    _trading,
    mock_5m,
    mock_ws_resolve,
    mock_fetch,
):
    mock_load_sector.return_value = {"NSE_INDEX|Nifty Bank": [{"stock": "HDFCBANK"}]}
    mock_indexes.return_value = ({}, {})
    bar_5m = {
        "timestamp": "2026-09-01T09:15:00+05:30",
        "open": 100,
        "high": 101,
        "low": 99,
        "close": 100.5,
        "volume": 10,
    }
    mock_5m.side_effect = lambda ux, cache_dir, keys, **kw: {k: [bar_5m] for k in keys}
    with patch(
        "backend.services.breakfast_strategy.live_tick._rank_picked_sectors",
        return_value=(["NSE_INDEX|Nifty Bank"], True),
    ), patch(
        "backend.services.breakfast_strategy.live_tick._members_for_books",
        return_value={"NSE_INDEX|Nifty Bank": [{"stock": "HDFCBANK"}]},
    ), patch(
        "backend.services.breakfast_strategy.live_tick._resolve_stock_keys",
        return_value=({"NSE_INDEX|Nifty Bank": ["HDFCBANK"]}, ["NSE_FO|HDFCBANK"]),
    ), patch(
        "backend.services.breakfast_strategy.live_tick._build_stock_overrides_from_1m",
        return_value=({}, {}),
    ), patch(
        "backend.services.breakfast_strategy.live_tick.select_breakfast_picks_prevclose",
        return_value=None,
    ), patch(
        "backend.services.breakfast_strategy.live_tick._now_ist",
        return_value=IST.localize(datetime(2026, 9, 1, 9, 20, 5)),
    ):
        out = run_breakfast_minute_tick(20)
    assert out["ok"]
    mock_ws_resolve.assert_not_called()
    mock_fetch.assert_not_called()
    fetched_batches = [c.args[2] for c in mock_5m.call_args_list]
    flat = [ik for batch in fetched_batches for ik in batch]
    assert "NSE_INDEX|Nifty 50" in flat
    assert "NSE_INDEX|Nifty Bank" in flat
    assert "NSE_FO|HDFCBANK" in flat
    assert out.get("data_source") == "rest_5m"


def test_live_payload_nifty_pct_uses_prev_close_not_auction():
    from datetime import datetime

    from backend.services.breakfast_strategy.live_tick import _build_payload_from_selection

    now = IST.localize(datetime(2026, 9, 1, 9, 20, 5))
    bar = {"open": 100, "high": 102, "low": 99, "close": 101, "volume": 1}
    payload = _build_payload_from_selection(
        now=now,
        session_date=date(2026, 9, 1),
        phase="frozen",
        sel=None,
        nifty_bar=bar,
        tick_minute=20,
        elapsed_sec=1.0,
        data_source="rest_5m",
        nifty_prev_close=50.0,
    )
    assert payload["nifty"]["bias"] == "positive"
    assert payload["nifty"]["bias_pct"] == round((101.0 - 50.0) / 50.0 * 100.0, 3)
    assert payload["nifty"]["bias_pct"] != 1.0
    assert payload["nifty"]["direction"] == "LONG"


def test_live_payload_negative_nifty_pct_is_short_not_long():
    from datetime import datetime

    from backend.services.breakfast_strategy.live_tick import _build_payload_from_selection

    now = IST.localize(datetime(2026, 9, 1, 23, 45))
    bar = {"open": 24077.55, "high": 24100, "low": 24000, "close": 24041.15, "volume": 1}
    payload = _build_payload_from_selection(
        now=now,
        session_date=date(2026, 9, 1),
        phase="frozen",
        sel=None,
        nifty_bar=bar,
        tick_minute=20,
        elapsed_sec=1.0,
        data_source="rest_5m",
        nifty_prev_close=24077.55,
    )
    assert payload["nifty"]["bias"] == "negative"
    assert payload["nifty"]["bias_pct"] < 0
    assert payload["nifty"]["direction"] == "SHORT"


def test_members_for_books_uses_db_wick_only():
    from backend.services.breakfast_prev_close import WICK_LONG_DOWN, WICK_LONG_UP
    from backend.services.breakfast_strategy.live_tick import _members_for_books

    bank = "NSE_INDEX|Nifty Bank"
    it = "NSE_INDEX|Nifty IT"
    members = {
        bank: [{"stock": "AAA"}, {"stock": "BBB"}],
        it: [{"stock": "CCC"}, {"stock": "DDD"}],
    }
    wicks = {
        "AAA": WICK_LONG_DOWN,
        "BBB": WICK_LONG_UP,
        "CCC": WICK_LONG_UP,
        "DDD": WICK_LONG_DOWN,
    }
    out = _members_for_books(members, wicks, [(bank, True), (it, False)])
    assert [m["stock"] for m in out[bank]] == ["AAA"]
    assert [m["stock"] for m in out[it]] == ["CCC"]


@patch("backend.services.breakfast_prev_close.classify_daily_wick")
@patch("backend.services.breakfast_strategy.live_tick.fetch_1m_parallel")
@patch("backend.services.breakfast_strategy.live_tick._resolve_candles_ws_primary")
@patch("backend.services.breakfast_strategy.live_tick.fetch_5m_parallel")
@patch("backend.services.breakfast_strategy.live_tick._is_trading_day", return_value=True)
@patch("backend.services.breakfast_strategy.live_tick.load_arbitrage_by_sector")
@patch("backend.services.breakfast_strategy.live_tick.build_instrument_indexes")
@patch(
    "backend.services.breakfast_strategy.live_tick._all_sector_keys",
    return_value=["NSE_INDEX|Nifty Bank", "NSE_INDEX|Nifty IT"],
)
@patch("backend.services.breakfast_strategy.live_tick.UpstoxService")
def test_freeze_stock_rest_only_wick_filtered_same_side(
    _ux,
    _sector_keys,
    mock_indexes,
    mock_load_sector,
    _trading,
    mock_5m,
    mock_ws_resolve,
    mock_fetch,
    mock_classify,
):
    from backend.services.breakfast_prev_close import WICK_LONG_DOWN, WICK_LONG_UP

    bank = "NSE_INDEX|Nifty Bank"
    it = "NSE_INDEX|Nifty IT"
    mock_load_sector.return_value = {
        bank: [{"stock": "AAA"}, {"stock": "BBB"}],
        it: [{"stock": "CCC"}, {"stock": "DDD"}],
    }
    mock_indexes.return_value = ({}, {})
    bar_5m = {
        "timestamp": "2026-09-01T09:15:00+05:30",
        "open": 100,
        "high": 101,
        "low": 99,
        "close": 100.5,
        "volume": 10,
    }
    mock_5m.side_effect = lambda ux, cache_dir, keys, **kw: {k: [bar_5m] for k in keys}
    prev = {"NSE_INDEX|Nifty 50": 100.0, bank: 100.0, it: 100.0}
    wicks = {
        "AAA": WICK_LONG_DOWN,
        "BBB": WICK_LONG_UP,
        "CCC": WICK_LONG_UP,
        "DDD": WICK_LONG_DOWN,
    }

    def _resolve(picked, members, *a, **k):
        iks = []
        for skey in picked:
            for m in members.get(skey, []):
                iks.append("NSE_FO|" + m["stock"])
        return ({s: [m["stock"] for m in members.get(s, [])] for s in picked}, iks)

    with patch(
        "backend.services.breakfast_strategy.live_tick.load_stored_prev_closes_and_wicks",
        return_value=(prev, {"AAA": 100.0, "BBB": 100.0, "CCC": 100.0, "DDD": 100.0}, wicks),
    ), patch(
        "backend.services.breakfast_strategy.live_tick._rank_picked_sectors",
        return_value=([bank, it], True),
    ), patch(
        "backend.services.breakfast_strategy.live_tick._resolve_stock_keys",
        side_effect=_resolve,
    ), patch(
        "backend.services.breakfast_strategy.live_tick._build_stock_overrides_from_1m",
        return_value=({}, {}),
    ), patch(
        "backend.services.breakfast_strategy.live_tick.select_breakfast_picks_prevclose",
        return_value=None,
    ), patch(
        "backend.services.breakfast_strategy.live_tick._now_ist",
        return_value=IST.localize(datetime(2026, 9, 1, 9, 20, 5)),
    ):
        out = run_breakfast_minute_tick(20)
    assert out["ok"]
    mock_classify.assert_not_called()
    mock_fetch.assert_not_called()
    batches = [list(c.args[2]) for c in mock_5m.call_args_list]
    assert batches[0] == ["NSE_INDEX|Nifty 50", bank, it]
    stock_keys = batches[1] if len(batches) > 1 else []
    assert stock_keys == ["NSE_FO|AAA", "NSE_FO|DDD"]
    assert "NSE_FO|BBB" not in stock_keys
    assert "NSE_FO|CCC" not in stock_keys
    assert out["rest_call_budget"] == {"indexes": 3, "stocks": 2}

"""Breakfast live — WS vs REST cross-check and live state smoke tests."""
from __future__ import annotations

from datetime import date, datetime
from unittest.mock import MagicMock, patch

import pytz

import pytest

from backend.services.breakfast_strategy.candles import aggregate_1m_to_session_5m, bars_ohlc_close_match
from backend.services.breakfast_strategy.live import build_live_state, validate_ws_vs_rest
from backend.services.breakfast_strategy.live_persist import (
    live_state_from_persisted_rows,
    persist_live_signals,
    rows_from_live_state,
    update_manual_capture,
)
from backend.services.upstox_service import UpstoxService

IST = pytz.timezone("Asia/Kolkata")

SAMPLE_STATE = {
    "session_date": "2026-08-28",
    "server_time": "2026-08-28T09:20:08+05:30",
    "nifty": {"bias_pct": 0.12},
    "sectors": [
        {
            "sector_label": "Bank",
            "sector_rank": 1,
            "move_pct": 0.45,
            "direction": "LONG",
            "stocks": [
                {
                    "symbol": "HDFCBANK",
                    "direction": "LONG",
                    "rank_in_sector": 1,
                    "move_pct_at_entry": 1.2,
                    "ltp": 1650.5,
                    "anchor_price": 1640.0,
                    "tp_price": 1656.4,
                    "sl_price": 1635.6,
                    "lot_size": 550,
                    "instrument_key": "NSE_FO|HDFCBANK",
                },
            ],
        },
    ],
}


def test_aggregate_1m_to_5m():
    sd = date(2026, 8, 1)
    bars = [
        {"timestamp": "2026-08-01T09:15:00+05:30", "open": 100, "high": 101, "low": 99.5, "close": 100.5, "volume": 10},
        {"timestamp": "2026-08-01T09:16:00+05:30", "open": 100.5, "high": 102, "low": 100, "close": 101.5, "volume": 12},
        {"timestamp": "2026-08-01T09:17:00+05:30", "open": 101.5, "high": 103, "low": 101, "close": 102, "volume": 8},
    ]
    bar = aggregate_1m_to_session_5m(bars, sd)
    assert bar is not None
    assert bar["open"] == 100
    assert bar["close"] == 102
    assert bar["high"] == 103
    assert bar["low"] == 99.5


def test_bars_ohlc_close_match_tolerance():
    a = {"open": 100, "high": 101, "low": 99, "close": 100.5}
    b = {"open": 100.01, "high": 101.02, "low": 99.01, "close": 100.51}
    assert bars_ohlc_close_match(a, b)
    c = {"open": 100, "high": 101, "low": 99, "close": 101.0}
    assert not bars_ohlc_close_match(a, c)


def test_build_live_state_weekend():
    sat = IST.localize(datetime(2026, 8, 29, 9, 18))
    out = build_live_state(replay_at=sat)
    assert out["state"] == "off_session"


def test_build_ws_stock_overrides_ws_only_no_rest():
    """Live forming must not fan out per-stock REST quotes (WS-only overrides)."""
    from backend.services.breakfast_strategy.live import _build_ws_stock_overrides

    with patch(
        "backend.services.breakfast_strategy.live.get_ws_forming_5m_bar",
        return_value={"open": 100, "high": 101, "low": 99, "close": 100.5, "volume": 1},
    ), patch(
        "backend.services.breakfast_strategy.live._resolve_session_bar",
    ) as mock_resolve, patch(
        "backend.services.breakfast_strategy.live.resolve_stock_instrument",
        return_value=MagicMock(instrument_key="NSE_FO|TEST"),
    ):
        overrides, anchors = _build_ws_stock_overrides(
            stocks_by_sector={"NSE_FO|BANK": [{"stock": "HDFCBANK"}]},
            candidate_sector_keys=["NSE_FO|BANK"],
            session_date=date(2026, 8, 31),
            stock_candles_by_key={"NSE_FO|TEST": []},
            fut_by_und={},
            eq_by_symbol={},
        )
        mock_resolve.assert_not_called()


def test_build_live_state_pre_live_window_fast():
    """Before 9:00 IST on a weekday — must not hit Upstox/WS (fast off-session)."""
    replay = IST.localize(datetime(2026, 8, 31, 1, 15))
    out = build_live_state(replay_at=replay)
    assert out["state"] == "off_session"
    assert out["phase"] == "waiting"
    assert "9:00" in (out.get("banner") or "")


@pytest.mark.skip(reason="requires PostgreSQL arbitrage_master")
def test_live_replay_forming_phase():
    """Replay timestamp lands in forming phase with banner text."""
    replay = IST.localize(datetime(2026, 8, 28, 9, 17))
    out = build_live_state(replay_at=replay)
    assert out.get("phase") == "forming"
    assert "FORMING" in (out.get("banner") or "")


def test_rows_from_live_state_builds_row():
    rows = rows_from_live_state(SAMPLE_STATE, "matched")
    assert len(rows) == 1
    r = rows[0]
    assert r["symbol"] == "HDFCBANK"
    assert r["direction"] == "LONG"
    assert r["sector_rank"] == 1
    assert r["rank_at_lock"] == 1
    assert r["websocket_rest_cross_check_status"] == "matched"
    assert r["stock_move_pct_at_lock"] == 1.2


def test_live_state_from_persisted_rows_roundtrip():
    db_rows = rows_from_live_state(SAMPLE_STATE, "matched")
    state = live_state_from_persisted_rows("2026-08-28", db_rows)
    assert state["session_date"] == "2026-08-28"
    assert state["nifty"]["direction"] == "LONG"
    assert len(state["sectors"]) == 1
    assert state["sectors"][0]["stocks"][0]["symbol"] == "HDFCBANK"


@patch("backend.services.breakfast_strategy.live._load_persisted_live_state")
def test_build_live_state_frozen_missing_data(mock_load):
    mock_load.return_value = None
    replay = IST.localize(datetime(2026, 8, 31, 10, 4))
    out = build_live_state(replay_at=replay)
    assert out["state"] == "off_session"
    assert out["phase"] == "frozen"
    assert out.get("data_missing")
    assert "2026-08-31" in (out.get("data_missing_reason") or "")
    assert "No picks captured" in (out.get("banner") or "")


@patch("backend.services.breakfast_strategy.live._load_persisted_live_state")
def test_build_live_state_frozen_loads_persisted(mock_load):
    mock_load.return_value = live_state_from_persisted_rows(
        "2026-08-31",
        rows_from_live_state(SAMPLE_STATE, "matched"),
    )
    replay = IST.localize(datetime(2026, 8, 31, 10, 4))
    out = build_live_state(replay_at=replay)
    assert out["nifty"]["direction"] == "LONG"
    assert len(out["sectors"]) == 1
    assert out["from_persisted"]


@patch("backend.services.breakfast_strategy.live_persist._insert_signal")
@patch("backend.services.breakfast_strategy.live_persist.ensure_breakfast_live_signals_table")
def test_persist_live_signals_idempotent(mock_ensure, mock_insert):
    mock_insert.side_effect = [True, False]
    out = persist_live_signals(SAMPLE_STATE, "matched")
    assert out == {"inserted": 1, "skipped": 0}
    out2 = persist_live_signals(SAMPLE_STATE, "matched")
    assert out2 == {"inserted": 0, "skipped": 1}
    mock_ensure.assert_called()


@patch("backend.services.breakfast_strategy.live_persist.SessionLocal")
@patch("backend.services.breakfast_strategy.live_persist.ensure_breakfast_live_signals_table")
def test_update_manual_capture_sets_trade_taken(mock_ensure, mock_session_local):
    mock_db = MagicMock()
    mock_session_local.return_value = mock_db
    mock_db.execute.return_value.mappings.return_value.first.return_value = {
        "symbol": "HDFCBANK",
        "direction": "LONG",
        "trade_taken": True,
        "manual_entry_price": 1651.0,
    }
    updated = update_manual_capture(
        "2026-08-28",
        "HDFCBANK",
        "LONG",
        {"manual_entry_price": 1651.0, "manual_entry_time": "2026-08-28T09:21:00+05:30"},
    )
    assert updated is not None
    assert updated["trade_taken"] is True
    mock_db.commit.assert_called_once()


def test_validate_ws_vs_rest_smoke():
    """Structure check only — match may be false off-hours when WS bar is absent."""
    out = validate_ws_vs_rest(UpstoxService.NIFTY50_KEY, date(2026, 8, 20))
    assert out["instrument_key"] == UpstoxService.NIFTY50_KEY
    assert out["session_date"] == "2026-08-20"
    assert "rest_bar" in out
    assert "ws_bar" in out
    assert "match" in out
    assert isinstance(out["match"], bool)
    # Off-hours / no live WS: ws_bar is often None and match is False — not a failure.

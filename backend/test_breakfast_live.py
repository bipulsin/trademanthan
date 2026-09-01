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


@patch("backend.services.breakfast_strategy.live.fetch_session_lock", return_value=None)
def test_build_live_state_pre_live_window_fast(_lock):
    """Before 9:00 IST on a weekday — must not hit Upstox/WS (fast off-session)."""
    from backend.services.breakfast_strategy import live as live_mod

    live_mod._FROZEN_STATE.clear()
    live_mod._LAST_SESSION_STATE = None
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


@patch("backend.services.breakfast_strategy.live._tick_snapshot_for_session", return_value=None)
@patch("backend.services.breakfast_strategy.live.fetch_session_lock", return_value=None)
@patch("backend.services.breakfast_strategy.live._load_persisted_live_state")
@patch("backend.services.breakfast_strategy.live.build_off_cycle_preview_state")
def test_build_live_state_frozen_missing_data(mock_off_cycle, mock_load, _lock, _tick, caplog):
    from backend.services.breakfast_strategy import live as live_mod

    live_mod._FROZEN_STATE.clear()
    live_mod._LAST_SESSION_STATE = None
    live_mod._OFF_CYCLE_SNAPSHOT = None
    mock_load.return_value = None
    mock_off_cycle.return_value = {
        "ok": True,
        "state": "off_cycle",
        "phase": "frozen",
        "off_cycle": True,
        "banner": "Off cycle data as of 31-Aug-2026 10:04",
        "session_date": "2026-08-31",
        "nifty": {"direction": "LONG", "bias_pct": 0.1},
        "sectors": [{"sector_label": "Bank", "stocks": [{"symbol": "HDFCBANK"}]}],
    }
    replay = IST.localize(datetime(2026, 8, 31, 10, 4))
    with caplog.at_level("WARNING"):
        out = build_live_state(replay_at=replay)
    assert out["state"] == "off_cycle"
    assert out.get("off_cycle")
    assert "Off cycle data" in (out.get("banner") or "")
    assert out["nifty"]["direction"] == "LONG"
    assert len(out["sectors"]) == 1
    mock_off_cycle.assert_called_once()
    assert any("taking off-cycle path" in r.message for r in caplog.records)


LOCKED_PAYLOAD = {
    "session_date": "2026-08-31",
    "state": "locked",
    "phase": "frozen",
    "banner": "LOCKED — 9:20 CONFIRMED",
    "nifty": {"direction": "LONG", "bias_pct": 0.12, "bias": "LONG"},
    "sectors": SAMPLE_STATE["sectors"],
}


def test_payload_from_lock_row_parses_json_string():
    import json as json_mod

    from backend.services.breakfast_strategy.live import _payload_from_lock_row

    parsed = _payload_from_lock_row({"payload_json": json_mod.dumps(LOCKED_PAYLOAD)})
    assert parsed["nifty"]["direction"] == "LONG"
    assert _payload_from_lock_row({"payload_json": LOCKED_PAYLOAD})["session_date"] == "2026-08-31"
    assert _payload_from_lock_row({"payload_json": None}) is None
    assert _payload_from_lock_row(None) is None


def _clear_frozen_live():
    from backend.services.breakfast_strategy import live as live_mod

    live_mod._FROZEN_STATE.clear()
    live_mod._LAST_SESSION_STATE = None
    live_mod._OFF_CYCLE_SNAPSHOT = None


@patch("backend.services.breakfast_strategy.live._tick_snapshot_for_session", return_value=None)
@patch("backend.services.breakfast_strategy.live._load_persisted_live_state", return_value=None)
@patch("backend.services.breakfast_strategy.live.build_off_cycle_preview_state")
@patch(
    "backend.services.breakfast_strategy.live.fetch_session_lock",
    return_value={"lock_status": "locked", "payload_json": LOCKED_PAYLOAD},
)
def test_build_live_state_frozen_uses_lock_payload_json(mock_lock, mock_off_cycle, _persist, _tick):
    """Locked row with payload_json must not take the slow off-cycle path."""
    _clear_frozen_live()
    replay = IST.localize(datetime(2026, 8, 31, 10, 4))
    out = build_live_state(replay_at=replay)
    assert out["nifty"]["direction"] == "LONG"
    assert out["sectors"][0]["stocks"][0]["symbol"] == "HDFCBANK"
    assert not out.get("off_cycle")
    mock_off_cycle.assert_not_called()


@patch("backend.services.breakfast_strategy.live.fetch_session_lock", return_value=None)
@patch("backend.services.breakfast_strategy.live._load_persisted_live_state", return_value=None)
@patch("backend.services.breakfast_strategy.live.build_off_cycle_preview_state")
@patch(
    "backend.services.breakfast_strategy.live._tick_snapshot_for_session",
    return_value={
        "session_date": "2026-08-31",
        "phase": "bar_closing",
        "nifty": {"direction": "SHORT", "bias_pct": -0.2},
        "sectors": [{"sector_label": "IT", "direction": "SHORT", "stocks": [{"symbol": "TCS"}]}],
    },
)
def test_build_live_state_frozen_uses_tick_snapshot_during_gap(mock_tick, mock_off_cycle, _persist, _lock):
    """Post-9:20:30 persist gap: serve in-memory tick snapshot, never off-cycle."""
    _clear_frozen_live()
    replay = IST.localize(datetime(2026, 8, 31, 9, 21))
    out = build_live_state(replay_at=replay)
    assert out["nifty"]["direction"] == "SHORT"
    assert out["sectors"][0]["stocks"][0]["symbol"] == "TCS"
    assert not out.get("off_cycle")
    mock_off_cycle.assert_not_called()
    mock_tick.assert_called_once()


@patch("backend.services.breakfast_strategy.live._tick_snapshot_for_session", return_value=None)
@patch("backend.services.breakfast_strategy.live._load_persisted_live_state", return_value=None)
@patch("backend.services.breakfast_strategy.live.build_off_cycle_preview_state")
@patch(
    "backend.services.breakfast_strategy.live.fetch_session_lock",
    return_value={"lock_status": "locked", "payload_json": None},
)
def test_build_live_state_frozen_locked_never_off_cycle(mock_lock, mock_off_cycle, _persist, _tick):
    """lock_status=locked must never call build_off_cycle_preview_state."""
    _clear_frozen_live()
    replay = IST.localize(datetime(2026, 8, 31, 10, 4))
    out = build_live_state(replay_at=replay)
    mock_off_cycle.assert_not_called()
    assert not out.get("off_cycle")
    assert out.get("state") == "locked"
    assert out.get("phase") == "frozen"


@patch("backend.services.breakfast_strategy.live.fetch_session_lock", return_value=None)
@patch("backend.services.breakfast_strategy.live._load_persisted_live_state")
def test_build_live_state_frozen_loads_persisted(mock_load, _lock):
    _clear_frozen_live()
    mock_load.return_value = live_state_from_persisted_rows(
        "2026-08-31",
        rows_from_live_state(SAMPLE_STATE, "matched"),
    )
    replay = IST.localize(datetime(2026, 8, 31, 10, 4))
    out = build_live_state(replay_at=replay)
    assert out["nifty"]["direction"] == "LONG"
    assert len(out["sectors"]) == 1
    assert out["from_persisted"]


@patch("backend.services.breakfast_strategy.live._tick_snapshot_for_session", return_value=None)
@patch("backend.services.breakfast_strategy.live._load_persisted_live_state", return_value=None)
@patch("backend.services.breakfast_strategy.live.build_off_cycle_preview_state")
@patch(
    "backend.services.breakfast_strategy.live.fetch_session_lock",
    return_value={"lock_status": "failed", "failure_reason": "no_sectors_at_freeze"},
)
def test_failed_freeze_does_not_block_5m_preview(mock_lock, mock_off_cycle, _persist, _tick):
    """Failed freeze must not stick in _FROZEN_STATE; /live still builds 5m off-cycle preview."""
    from backend.services.breakfast_strategy.live import ingest_frozen_snapshot

    _clear_frozen_live()
    ingest_frozen_snapshot(
        {
            "session_date": "2026-08-31",
            "state": "lock_failed",
            "lock_failed": True,
            "sectors": [],
            "nifty": {"direction": "UNKNOWN", "bias": "unknown"},
        }
    )
    from backend.services.breakfast_strategy import live as live_mod

    assert "2026-08-31" not in live_mod._FROZEN_STATE
    mock_off_cycle.return_value = {
        "ok": True,
        "state": "off_cycle",
        "phase": "frozen",
        "off_cycle": True,
        "banner": "Off cycle data as of 31-Aug-2026 10:04",
        "session_date": "2026-08-31",
        "nifty": {"direction": "SHORT", "bias_pct": -0.2},
        "sectors": [{"sector_label": "IT", "stocks": [{"symbol": "TCS"}]}],
    }
    replay = IST.localize(datetime(2026, 8, 31, 10, 4))
    out = build_live_state(replay_at=replay)
    mock_off_cycle.assert_called_once()
    assert out.get("lock_failed")
    assert out["nifty"]["direction"] == "SHORT"
    assert out["sectors"][0]["stocks"][0]["symbol"] == "TCS"


def test_leftover_failed_frozen_cache_still_allows_preview():
    """Guard: even a leftover failed cache entry must not skip off-cycle preview."""
    from backend.services.breakfast_strategy import live as live_mod

    _clear_frozen_live()
    live_mod._FROZEN_STATE["2026-08-31"] = {
        "session_date": "2026-08-31",
        "state": "lock_failed",
        "lock_failed": True,
        "sectors": [],
    }
    with patch(
        "backend.services.breakfast_strategy.live.fetch_session_lock",
        return_value={"lock_status": "failed", "failure_reason": "no_data"},
    ), patch(
        "backend.services.breakfast_strategy.live._load_persisted_live_state",
        return_value=None,
    ), patch(
        "backend.services.breakfast_strategy.live._tick_snapshot_for_session",
        return_value=None,
    ), patch(
        "backend.services.breakfast_strategy.live.build_off_cycle_preview_state",
        return_value={
            "ok": True,
            "state": "off_cycle",
            "off_cycle": True,
            "banner": "Off cycle",
            "nifty": {"direction": "LONG"},
            "sectors": [{"stocks": [{"symbol": "HDFCBANK"}]}],
        },
    ) as mock_off:
        out = build_live_state(replay_at=IST.localize(datetime(2026, 8, 31, 10, 4)))
    mock_off.assert_called_once()
    assert out["sectors"][0]["stocks"][0]["symbol"] == "HDFCBANK"


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

"""Breakfast live — WS vs REST cross-check and live state smoke tests."""
from __future__ import annotations

from datetime import date, datetime
from unittest.mock import MagicMock, patch

import pytz

import pytest

from backend.services.breakfast_strategy.candles import aggregate_1m_to_session_5m, bars_ohlc_close_match
from backend.services.breakfast_strategy.live import build_live_state, validate_ws_vs_rest
from backend.services.breakfast_strategy.live_persist import (
    assign_selected_sector_ranks,
    compact_live_sector_cards,
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
                    "first_5m_open": 1640.0,
                    "first_5m_high": 1652.0,
                    "first_5m_low": 1638.0,
                    "first_5m_close": 1650.5,
                    "first_5m_ts": "2026-08-28T09:15:00+05:30",
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
    assert r["first_5m_open"] == 1640.0
    assert r["first_5m_high"] == 1652.0
    assert r["first_5m_low"] == 1638.0
    assert r["first_5m_close"] == 1650.5
    assert r["first_5m_ts"].startswith("2026-08-28T09:15")


def test_live_state_from_persisted_rows_roundtrip():
    db_rows = rows_from_live_state(SAMPLE_STATE, "matched")
    state = live_state_from_persisted_rows(
        "2026-08-28", db_rows, wick_by_symbol={"HDFCBANK": "Long_Down_Wick"}
    )
    assert state["session_date"] == "2026-08-28"
    assert state["nifty"]["direction"] == "LONG"
    assert len(state["sectors"]) == 1
    assert state["sectors"][0]["stocks"][0]["symbol"] == "HDFCBANK"
    assert state["sectors"][0]["stocks"][0]["first_5m_close"] == 1650.5
    assert state["sectors"][0]["stocks"][0]["wick"] == "Long_Down_Wick"
    assert state["sectors"][0]["selected_rank"] == 1


def test_assign_selected_sector_ranks_by_abs_pct():
    secs = [
        {"sector_label": "IT", "move_pct": -0.9},
        {"sector_label": "Bank", "move_pct": 0.4},
    ]
    assign_selected_sector_ranks(secs)
    assert secs[0]["selected_rank"] == 1
    assert secs[1]["selected_rank"] == 2


def test_compact_and_rank_selected_after_cascade():
    secs = [
        {"sector_label": "IT", "move_pct": -2.47, "stocks": [{"symbol": "HCLTECH"}]},
        {"sector_label": "Realty", "move_pct": -1.85, "stocks": []},
        {"sector_label": "Auto", "move_pct": -1.76, "stocks": [{"symbol": "M&M"}]},
    ]
    out = compact_live_sector_cards(secs)
    assert [s["sector_label"] for s in out] == ["IT", "Auto"]
    assign_selected_sector_ranks(out)
    assert out[0]["selected_rank"] == 1
    assert out[1]["selected_rank"] == 2


def test_lock_failed_banner_uses_frozen_as_of():
    from backend.services.breakfast_strategy.live import _lock_failed_preview_banner

    banner = _lock_failed_preview_banner(
        "no_filtered_stocks:color",
        {
            "server_time": "2026-09-03T09:20:05+05:30",
            "nifty": {"direction": "LONG", "bias_pct": 0.2},
            "sectors": [],
        },
    )
    assert banner.startswith("LOCK FAILED — no_filtered_stocks:color")
    assert "frozen as of" in banner
    assert "Off cycle" not in banner
    empty = _lock_failed_preview_banner("no_data", {"nifty": {}, "sectors": []})
    assert empty == "LOCK FAILED — no_data"


@patch("backend.services.breakfast_strategy.live._tick_snapshot_for_session", return_value=None)
@patch("backend.services.breakfast_strategy.live.fetch_session_lock", return_value=None)
@patch("backend.services.breakfast_strategy.live._load_persisted_live_state")
@patch("backend.services.breakfast_strategy.live.build_off_cycle_preview_state")
def test_build_live_state_frozen_missing_data(mock_off_cycle, mock_load, _lock, _tick, caplog):
    """After 9:20 with no lock row, do not invent off-cycle picks."""
    from backend.services.breakfast_strategy import live as live_mod

    live_mod._FROZEN_STATE.clear()
    live_mod._LAST_SESSION_STATE = None
    live_mod._OFF_CYCLE_SNAPSHOT = None
    mock_load.return_value = None
    replay = IST.localize(datetime(2026, 8, 31, 10, 4))
    out = build_live_state(replay_at=replay)
    mock_off_cycle.assert_not_called()
    assert not out.get("off_cycle")
    assert out.get("phase") == "frozen"
    assert out.get("sectors") == []
    assert "9:20" in (out.get("banner") or "")


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
        wick_by_symbol={},
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
    return_value={
        "lock_status": "failed",
        "failure_reason": "no_filtered_stocks:color",
        "payload_json": {
            "session_date": "2026-08-31",
            "state": "lock_failed",
            "lock_failed": True,
            "failure_reason": "no_filtered_stocks:color",
            "server_time": "2026-08-31T09:20:05+05:30",
            "nifty": {"direction": "LONG", "open": 25000.0, "close": 25010.0, "bias": "positive"},
            "sectors": [],
        },
    },
)
def test_failed_freeze_serves_stored_920_snapshot(mock_lock, mock_off_cycle, _persist, _tick):
    """Failed freeze stays sticky — serve 9:20 payload_json, never off-cycle refresh."""
    from backend.services.breakfast_strategy.live import ingest_frozen_snapshot
    from backend.services.breakfast_strategy import live as live_mod

    _clear_frozen_live()
    ingest_frozen_snapshot(
        {
            "session_date": "2026-08-31",
            "state": "lock_failed",
            "lock_failed": True,
            "failure_reason": "no_filtered_stocks:color",
            "nifty": {"direction": "LONG", "open": 25000.0, "close": 25010.0},
            "sectors": [],
            "server_time": "2026-08-31T09:20:05+05:30",
        }
    )
    assert "2026-08-31" in live_mod._FROZEN_STATE
    replay = IST.localize(datetime(2026, 8, 31, 10, 4))
    out = build_live_state(replay_at=replay)
    mock_off_cycle.assert_not_called()
    assert out.get("lock_failed")
    assert out["nifty"]["direction"] == "LONG"
    assert out["nifty"]["open"] == 25000.0
    assert out.get("sectors") == []
    assert not out.get("off_cycle")
    assert out.get("refresh_allowed") is False


def test_leftover_failed_frozen_cache_is_sticky():
    """A failed 9:20 cache entry is the day's Live view — no off-cycle rebuild."""
    from backend.services.breakfast_strategy import live as live_mod

    _clear_frozen_live()
    live_mod._FROZEN_STATE["2026-08-31"] = {
        "session_date": "2026-08-31",
        "state": "lock_failed",
        "lock_failed": True,
        "failure_reason": "no_data",
        "nifty": {"direction": "UNKNOWN"},
        "sectors": [],
        "banner": "LOCK FAILED — no_data",
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
    ) as mock_off:
        out = build_live_state(replay_at=IST.localize(datetime(2026, 8, 31, 10, 4)))
    mock_off.assert_not_called()
    assert out.get("lock_failed")
    assert out.get("nifty", {}).get("direction") == "UNKNOWN"
    assert out.get("sectors") == []


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


def test_nifty_live_card_negative_pct_is_short_not_blue_long():
    from backend.services.breakfast_strategy.live import _nifty_live_card

    bar = {"open": 24077.55, "high": 24100, "low": 24000, "close": 24041.15, "volume": 1}
    bias, pct, direction = _nifty_live_card(nifty_bar=bar, nifty_prev=24077.55, sel=None)
    assert bias == "negative"
    assert pct < 0
    assert direction == "SHORT"
    assert direction != "LONG"


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


def test_ensure_5m_cached_force_keeps_disk_on_empty_fetch(tmp_path):
    """force=True must not wipe a warm 9:15 bar when Upstox returns empty."""
    from backend.services.breakfast_strategy.candles import (
        ensure_5m_cached,
        first_5m_bar,
        save_cached_5m,
    )

    ik = "NSE_INDEX|Nifty Auto"
    sd = date(2026, 9, 3)
    bar = {
        "timestamp": "2026-09-03T09:15:00+05:30",
        "open": 1,
        "high": 2,
        "low": 1,
        "close": 1.5,
        "volume": 0,
    }
    save_cached_5m(tmp_path, ik, [bar])

    class UX:
        def get_historical_candles_by_instrument_key(self, *a, **k):
            return []

    out = ensure_5m_cached(
        UX(),
        tmp_path,
        ik,
        range_end=sd,
        session_dates=[sd],
        force=True,
        throttle_sec=0.0,
    )
    assert first_5m_bar(out, sd) is not None
    assert float(first_5m_bar(out, sd)["close"]) == 1.5

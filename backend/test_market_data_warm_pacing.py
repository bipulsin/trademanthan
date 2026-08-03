"""Tests for market-data candle-warm rotation and overlap helpers."""
from __future__ import annotations

from datetime import datetime

from backend.services.market_data import engine as md_engine


def test_rotate_universe_rows_changes_head(monkeypatch):
    rows = [{"stock": s} for s in ["AAA", "BBB", "CCC", "DDD", "EEE"]]
    monkeypatch.setattr(
        md_engine,
        "_now_ist",
        lambda: datetime(2026, 8, 3, 11, 25, tzinfo=md_engine.IST),
    )
    rotated, offset = md_engine._rotate_universe_rows(rows, execution="scheduled_10m")
    assert 0 <= offset < 5
    assert len(rotated) == 5
    assert {r["stock"] for r in rotated} == {r["stock"] for r in rows}
    rotated2, offset2 = md_engine._rotate_universe_rows(rows, execution="scheduled_10m")
    assert offset == offset2
    assert rotated[0]["stock"] == rotated2[0]["stock"]


def test_candle_rl_max_wait_override_roundtrip():
    from backend.services import upstox_rate_limiter as rl

    rl.set_candle_rl_max_wait_override(300.0)
    with rl._max_wait_override_lock:
        assert rl._max_wait_override == 300.0
    rl.set_candle_rl_max_wait_override(None)
    with rl._max_wait_override_lock:
        assert rl._max_wait_override is None

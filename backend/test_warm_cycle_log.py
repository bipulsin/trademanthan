"""Unit tests for candle warm-cycle ring buffer."""
from __future__ import annotations

from backend.services.market_data import warm_cycle_log as wcl


def test_record_and_recent_warm_cycles(monkeypatch):
    monkeypatch.setattr(wcl, "_CYCLES", wcl.deque(maxlen=72))
    wcl.record_warm_cycle(
        {
            "execution": "test",
            "candle_keys_requested": 10,
            "candle_errors": 2,
            "candle_deny_pct": 20.0,
            "candle_denied_symbols": ["AAA", "BBB"],
            "updated_at_ist": "2026-07-31 11:32:00",
        }
    )
    rows = wcl.recent_warm_cycles(limit=5)
    assert len(rows) == 1
    assert rows[0]["candle_errors"] == 2
    assert rows[0]["candle_denied_symbols"] == ["AAA", "BBB"]
    assert wcl.latest_warm_cycle()["execution"] == "test"

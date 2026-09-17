"""Unit tests for Kavach live EMA5 entry snapshots (no DB / WS required)."""
from unittest.mock import patch

from backend.services.daily_checklist_live_entries import live_entry_snapshots


def test_live_entry_snapshots_empty_symbols():
    with patch(
        "backend.services.daily_checklist_live_entries.arm_live_feed",
        return_value={"ok": True, "feed_armed": True},
    ):
        out = live_entry_snapshots([])
    assert out["ok"] is True
    assert out["entries"] == []


def test_live_entry_snapshots_from_rocket_ws():
    def fake_get(sym):
        if sym == "AAA":
            return {"ema5": 1234.567, "age_sec": 1.2}
        return None

    with patch(
        "backend.services.daily_checklist_live_entries.arm_live_feed",
        return_value={"ok": True, "feed_armed": True},
    ), patch(
        "backend.services.rocket_ws_live.get_live_10m",
        side_effect=fake_get,
    ):
        out = live_entry_snapshots(["aaa", "BBB", "aaa"])

    assert out["ok"] is True
    by_sym = {e["symbol"]: e for e in out["entries"]}
    assert set(by_sym) == {"AAA", "BBB"}
    assert by_sym["AAA"]["entry"] == 1234.57
    assert by_sym["AAA"]["ema5"] == 1234.57
    assert by_sym["AAA"]["source"] == "rocket_ws"
    assert by_sym["BBB"]["entry"] is None
    assert by_sym["BBB"]["source"] is None

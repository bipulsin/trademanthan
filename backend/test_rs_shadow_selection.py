"""Tests for shadow RS selection logging — must not touch live checklist tables."""
from unittest.mock import MagicMock, patch

from backend.services.rs_shadow_selection import (
    SHADOW_CHECKPOINTS,
    vw_score,
    _volume_weighted_top5,
)


def test_vw_score_bull_higher_volume_boosts():
    low = vw_score(2.0, 0.8, "BULL")
    high = vw_score(2.0, 1.5, "BULL")
    assert high > low


def test_vw_score_bear_uses_negative_rs():
    assert vw_score(2.0, 1.0, "BEAR") < vw_score(1.0, 1.0, "BEAR")


def test_volume_weighted_top5_picks_five():
    pool = [
        {"symbol": f"S{i}", "relative_strength": float(i), "volume_ratio": 1.0}
        for i in range(8)
    ]
    picks = _volume_weighted_top5(pool, "BULL")
    assert len(picks) == 5
    assert picks[0]["symbol"] == "S7"


def test_shadow_checkpoints_match_anchor_subset():
    assert set(SHADOW_CHECKPOINTS) <= {"09:25", "09:45", "10:15", "10:30", "12:30", "14:30"}


def test_run_shadow_no_live_snapshot_writes():
    """Shadow module must not import morning lock writers."""
    import backend.services.rs_shadow_selection as mod

    src = open(mod.__file__).read()
    assert "snapshot_lock" not in src or "get_locked_symbols" in src
    assert "INSERT INTO daily_snapshot" not in src
    assert "INSERT INTO snapshot_lock" not in src
    assert "rs_conviction_board" not in src


def test_run_shadow_logs_only(monkeypatch):
    mock_db = MagicMock()
    mock_db.execute.return_value.mappings.return_value.all.return_value = []
    mock_db.execute.return_value.mappings.return_value.first.return_value = None

    with patch("backend.services.rs_shadow_selection.SessionLocal", return_value=mock_db):
        from backend.services.rs_shadow_selection import run_shadow_selection_log

        out = run_shadow_selection_log("09:25")
    assert out["ok"] is False
    assert out.get("reason") == "no_scan_near_checkpoint"

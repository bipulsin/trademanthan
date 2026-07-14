"""Unit tests for READY shadow review helpers (no DB)."""
from types import SimpleNamespace

from backend.services.ready_shadow_review import (
    EXPORT_FIELDS,
    _apply_filter,
    _is_ready_state,
    _rollup,
    _row_to_view,
)


def test_is_ready_state():
    assert _is_ready_state("READY")
    assert _is_ready_state("READY(RECHECK)")
    assert not _is_ready_state("WAIT FOR PULLBACK")
    assert not _is_ready_state(None)


def test_row_to_view_shadow_exclude_needs_classification():
    r = SimpleNamespace(
        id=42,
        session_date="2026-07-14",
        symbol="bdl",
        direction="SHORT",
        rendered_state="READY",
        pre_gate_state="READY",
        in_lock=True,
        lock_rank=3,
        lock_direction="BEAR",
        lock_mismatch=False,
        vwap_slope_score=12.5,
        steep_ok=False,
        flip_flop=True,
        whipsaw_crosses=3,
        quality_pass=False,
        vwap_gate_enabled=False,
        vwap_would_block=True,
        vwap_gate_applied=False,
        logged_at=None,
        outcome_classification=None,
        note=None,
        reviewed_at=None,
    )
    v = _row_to_view(r)
    assert v["log_id"] == 42
    assert v["symbol"] == "BDL"
    assert v["rendered_ready"] is True
    assert v["shadow_would_exclude"] is True
    assert v["needs_classification"] is True
    assert v["lock_mismatch"] is False


def test_row_to_view_lock_mismatch():
    r = SimpleNamespace(
        id=1,
        session_date="2026-07-14",
        symbol="X",
        direction="LONG",
        rendered_state="READY",
        pre_gate_state="READY",
        in_lock=False,
        lock_rank=None,
        lock_direction=None,
        lock_mismatch=True,
        vwap_slope_score=80,
        steep_ok=True,
        flip_flop=False,
        whipsaw_crosses=0,
        quality_pass=True,
        vwap_gate_enabled=False,
        vwap_would_block=False,
        vwap_gate_applied=False,
        logged_at=None,
        outcome_classification=None,
        note=None,
        reviewed_at=None,
    )
    v = _row_to_view(r)
    assert v["lock_mismatch"] is True
    assert v["shadow_would_include"] is True
    assert v["needs_classification"] is False


def test_filters_and_rollup():
    rows = [
        {"lock_mismatch": True, "shadow_would_exclude": False, "shadow_would_include": True, "needs_classification": False, "outcome_classification": None},
        {"lock_mismatch": False, "shadow_would_exclude": True, "shadow_would_include": False, "needs_classification": True, "outcome_classification": "correct exclusion"},
        {"lock_mismatch": False, "shadow_would_exclude": False, "shadow_would_include": True, "needs_classification": False, "outcome_classification": None},
    ]
    assert len(_apply_filter(rows, "mismatches")) == 1
    assert len(_apply_filter(rows, "shadow_excludes")) == 1
    roll = _rollup(rows)
    assert roll["total_rows"] == 3
    assert roll["mismatch_count"] == 1
    assert roll["shadow_exclude_count"] == 1
    assert roll["shadow_include_count"] == 2
    assert roll["needs_classification_count"] == 1
    assert roll["classified_count"] == 1


def test_export_fields_stable():
    assert "outcome_classification" in EXPORT_FIELDS
    assert "shadow_would_exclude" in EXPORT_FIELDS
    assert "lock_mismatch" in EXPORT_FIELDS
    assert EXPORT_FIELDS[0] == "log_id"

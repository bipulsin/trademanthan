"""Tests for ignition universe expansion."""
from backend.services.kavach_ignition_universe import (
    SCOPE_CORE_BOARD,
    SCOPE_LOCKED_OR_TOP5,
    ignition_scope,
)


def test_ignition_scope_default_locked_or_top5():
    assert ignition_scope() in (SCOPE_LOCKED_OR_TOP5, SCOPE_CORE_BOARD)
    from backend.services.rs_conviction_config import DEFAULTS

    assert DEFAULTS.get("ignition_scope") == SCOPE_LOCKED_OR_TOP5

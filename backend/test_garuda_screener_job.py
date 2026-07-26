"""Unit tests for Garuda confluence lookup helpers (no DB)."""
from __future__ import annotations

from backend.services.garuda_screener.job import (
    CONFLUENCE_MATCH,
    CONFLUENCE_NO_MATCH,
    CONFLUENCE_NOT_AVAILABLE,
)
from backend.services.garuda_screener.config import EXCLUDED_SYMBOLS, TOP_N


def test_garuda_constants():
    assert TOP_N == 6
    assert "EXIDEIND" in EXCLUDED_SYMBOLS
    assert CONFLUENCE_MATCH == "MATCH"
    assert CONFLUENCE_NO_MATCH == "NO_MATCH"
    assert CONFLUENCE_NOT_AVAILABLE == "NOT_AVAILABLE"


def test_rank_top_n_assigns_rank():
    from backend.services.garuda_screener.screener import rank_top_n

    rows = [
        {"symbol": "A", "imbalance_confirmed": True, "rank_score": 90, "strength": {"percentile": 90}, "momentum": {"percentile_roc3": 80}},
        {"symbol": "B", "imbalance_confirmed": True, "rank_score": 80, "strength": {"percentile": 80}, "momentum": {"percentile_roc3": 70}},
        {"symbol": "C", "imbalance_confirmed": False, "rank_score": 99, "strength": {"percentile": 99}, "momentum": {"percentile_roc3": 99}},
    ]
    out = rank_top_n(rows, top_n=6)
    assert out["top_symbols"] == ["A", "B"]
    assert out["top_n"][0]["rank"] == 1
    assert out["n_imbalance_confirmed"] == 2

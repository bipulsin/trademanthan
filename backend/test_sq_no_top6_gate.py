"""SQ eligibility does not require Garuda Top-6 membership."""
from __future__ import annotations

from backend.services.structural_quality_ready import evaluate_sq_for_stock


def test_evaluate_sq_without_top6_rank(monkeypatch):
    # Minimal: grade A/B + scores present; top6_rank absent → still attempts score_bars
    calls = {}

    def fake_score_bars_through(bars, **kwargs):
        calls["ok"] = True
        return {
            "total": 80.0,
            "OW": 80,
            "VW": 80,
            "EW": 80,
            "rs_score": 80,
            "garuda_score": 60,
            "grade_bonus": 15,
            "confidence_grade": "A",
        }

    monkeypatch.setattr(
        "backend.services.structural_quality_ready.score_bars_through",
        fake_score_bars_through,
    )
    monkeypatch.setattr(
        "backend.services.structural_quality_ready.enrich_session_10m_bars",
        lambda *a, **k: [{"close": 100, "vwap": 99}],
    )
    br = evaluate_sq_for_stock(
        db=None,
        stock={"symbol": "TEST", "direction": "LONG"},
        session_date="2026-08-03",
        candles=[{"close": 100}],
        garuda_meta={"rank_score": 55.0, "side": "LONG", "top6_rank": None},
        rs_meta={"confidence_grade": "A", "trade_score": 80.0},
    )
    assert br is not None
    assert br["meets_threshold"] is True
    assert br["garuda_top6_rank"] is None
    assert calls.get("ok") is True


def test_evaluate_sq_still_needs_garuda_score(monkeypatch):
    monkeypatch.setattr(
        "backend.services.structural_quality_ready._locf_garuda_rank",
        lambda *a, **k: None,
    )
    br = evaluate_sq_for_stock(
        db=None,
        stock={"symbol": "TEST", "direction": "LONG"},
        session_date="2026-08-03",
        candles=[],
        garuda_meta={"side": "LONG"},  # no rank_score
        rs_meta={"confidence_grade": "A", "trade_score": 80.0},
    )
    assert br is None

"""Tests for Vajra automated validation checklist engine."""
from backend.services.vajra.validation_engine import evaluate_checklist, extension_risk_level
from test_vajra_engine import _synthetic_candles


def test_extension_risk_levels():
    assert extension_risk_level(30) == "LOW"
    assert extension_risk_level(50) == "MEDIUM"
    assert extension_risk_level(70) == "HIGH"


def test_evaluate_checklist_returns_items():
    candles = _synthetic_candles(80, trend=0.35)
    out = evaluate_checklist(
        candles,
        direction="LONG",
        market_index={"nifty_pct": 0.2, "bank_pct": 0.1},
        sector_alignment="neutral",
    )
    assert out["auto_available"] is True
    items = out["items"]
    assert len(items) == 12
    keys = {it["key"] for it in items}
    assert "vwap_reclaimed" in keys
    assert "pullback_shallow" in keys
    for it in items:
        assert it["status"] in ("pass", "warn", "fail")
        assert 0 <= it["confidence"] <= 100
        assert it["tooltip"]
        assert it["metric"]


def test_checklist_bool_from_pass_only():
    candles = _synthetic_candles(80, trend=0.35)
    out = evaluate_checklist(candles, direction="SHORT", sector_alignment="aligned")
    checklist = out["checklist"]
    for it in out["items"]:
        assert checklist[it["key"]] == (it["status"] == "pass")

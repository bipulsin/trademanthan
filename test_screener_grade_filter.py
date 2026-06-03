"""Screener shows A+, A, B+, and B grades."""
from backend.services.vajra.ranking import build_screener_display
from backend.services.vajra.setup_classifier import quality_grade, screener_grade_allowed


def test_screener_grade_allowed():
    assert screener_grade_allowed({"quality_grade": "A+"})
    assert screener_grade_allowed({"quality_grade": "A"})
    assert screener_grade_allowed({"quality_grade": "B+"})
    assert screener_grade_allowed({"quality_grade": "B"})
    assert not screener_grade_allowed({"quality_grade": "C"})


def test_quality_grade_assigns_b_plus():
    row = {
        "qualification_state": "ARMED",
        "conviction_score": 64,
        "setup_quality_score": 60,
        "extension_risk_score": 40,
    }
    assert quality_grade(row) == "B+"


def test_build_screener_display_filters_grades():
    rows = [
        {
            "stock": "AAA",
            "qualification_state": "EXECUTABLE",
            "quality_grade": "A+",
            "execution_bias": "LONG",
            "directional_conviction": True,
            "market_phase": "Bull Expansion",
            "setup_quality_score": 80,
            "confidence": 85,
        },
        {
            "stock": "BBB",
            "qualification_state": "EXECUTABLE",
            "quality_grade": "B",
            "execution_bias": "LONG",
            "directional_conviction": True,
            "market_phase": "Bull Expansion",
            "setup_quality_score": 70,
            "confidence": 75,
        },
    ]
    out = build_screener_display(rows, top_n=8)
    stocks = {r.get("stock") for r in out["rows"]}
    assert "AAA" in stocks
    assert "BBB" in stocks

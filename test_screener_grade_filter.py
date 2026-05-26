"""Screener shows A+ and A grades only."""
from backend.services.vajra.ranking import build_screener_display
from backend.services.vajra.setup_classifier import screener_grade_allowed


def test_screener_grade_allowed():
    assert screener_grade_allowed({"quality_grade": "A+"})
    assert screener_grade_allowed({"quality_grade": "A"})
    assert not screener_grade_allowed({"quality_grade": "B"})
    assert not screener_grade_allowed({"quality_grade": "C"})


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
    assert "BBB" not in stocks

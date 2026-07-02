"""Unit tests for Kavach unified confidence grade."""
import pytest

from backend.services.kavach_confidence import (
    REGIME_TRANSITION,
    REGIME_TREND,
    compute_confidence_grade,
    format_confidence_display,
)


@pytest.mark.parametrize(
    "score,vol,purity,regime,expected",
    [
        (88, "High", 62, REGIME_TREND, "A"),
        (88, "Low", 0, REGIME_TREND, "D"),
        (80, "Low", 0, REGIME_TRANSITION, "C*"),
        (88, "Low", 62, REGIME_TREND, "C"),
        (88, "High", 37, REGIME_TREND, "C"),
        (64, "High", 62, REGIME_TREND, "D"),
        (88, "Average", 62, REGIME_TREND, "B"),
    ],
)
def test_confidence_grade_spec(score, vol, purity, regime, expected):
    grade, floor = compute_confidence_grade(score, vol, purity, regime)
    display = format_confidence_display(grade, floor)
    assert display == expected

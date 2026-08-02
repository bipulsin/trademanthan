"""Unit tests for additive SQ composite formula."""
from backend.services.structural_quality_score import (
    composite_total,
    grade_ab_ok,
    grade_bonus,
)


def test_grade_bonus_table():
    assert grade_bonus("A+") == 25
    assert grade_bonus("A") == 20
    assert grade_bonus("B") == 15
    assert grade_bonus("C") == 10
    assert grade_bonus("D") == 0
    assert grade_bonus("D!") == 0


def test_grade_ab_ok():
    assert grade_ab_ok("A+")
    assert grade_ab_ok("B")
    assert not grade_ab_ok("C")
    assert not grade_ab_ok("D")


def test_composite_maxish():
    # 0.15*100*5 + 25 = 100
    t = composite_total(
        rs_score=100, garuda_score=100, ow=100, vw=100, ew=100, grade="A+"
    )
    assert t == 100.0


def test_composite_threshold_ballpark():
    t = composite_total(
        rs_score=85, garuda_score=60, ow=90, vw=100, ew=100, grade="A"
    )
    # 0.15*(85+60+90+100+100)+20 = 0.15*435+20 = 65.25+20 = 85.25
    assert abs(t - 85.25) < 0.01

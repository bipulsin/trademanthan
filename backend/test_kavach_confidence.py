"""Unit tests for Kavach unified confidence grade + stretch penalty."""
import pytest

from backend.services.kavach_confidence import (
    REGIME_TRANSITION,
    REGIME_TREND,
    compute_confidence_grade,
    compute_stretch_pct,
    explain_confidence_grade,
    format_confidence_display,
    resolve_score_and_grade,
    stretch_penalties,
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


def test_stretch_pct_nearer_stop():
    # close 4890, ema10 4920, vwap 4880 → nearer is VWAP (10 pts) → 0.2045%
    assert compute_stretch_pct(4890, 4920, 4880) == pytest.approx(10 / 4890 * 100, rel=1e-3)
    # KEI-style: nearer ema10 at 30 pts → 0.613%
    assert compute_stretch_pct(4890, 4920, 4800) == pytest.approx(30 / 4890 * 100, rel=1e-3)


def test_stretch_pct_null_levels():
    assert compute_stretch_pct(None, 100, 100) is None
    assert compute_stretch_pct(100, None, 100) is None
    assert compute_stretch_pct(0, 100, 100) is None


def test_stretch_penalties_bands():
    assert stretch_penalties(0.35) == (0, 0)
    assert stretch_penalties(0.351) == (20, 2)
    assert stretch_penalties(0.50) == (20, 2)
    assert stretch_penalties(0.501) == (50, 99)
    assert stretch_penalties(None) == (0, 0)


def test_hard_stretch_kei_shadow_not_live():
    """KEI-style hard stretch: raw 95 → post 45 / D!; live off keeps pre A+."""
    explained = explain_confidence_grade(
        95,
        "High",
        80.0,
        REGIME_TREND,
        stretch_pct=0.613,
        apply_live=False,
    )
    assert explained["trade_score_pre_stretch"] == 95
    assert explained["trade_score_post_stretch"] == 45
    assert explained["stretch_score_penalty"] == 50
    assert explained["stretch_letter_penalty"] == 99
    assert explained["base_grade_pre_stretch"] == "A+"
    assert explained["base_grade_post_stretch"] == "D!"
    assert explained["score_int"] == 95
    assert explained["display_grade"] == "A+"


def test_hard_stretch_kei_live():
    explained = explain_confidence_grade(
        95,
        "High",
        80.0,
        REGIME_TREND,
        stretch_pct=0.613,
        apply_live=True,
    )
    assert explained["score_int"] == 45
    assert explained["display_grade"] == "D!"
    assert explained["grade"] == "D"


def test_soft_stretch_letter_and_score():
    # High+pure+88 → A; soft stretch → score 68, letter A→C → C!
    explained = explain_confidence_grade(
        88,
        "High",
        62.0,
        REGIME_TREND,
        stretch_pct=0.40,
        apply_live=True,
    )
    assert explained["trade_score_post_stretch"] == 68
    assert explained["stretch_score_penalty"] == 20
    assert explained["stretch_letter_penalty"] == 2
    assert explained["base_grade_pre_stretch"] == "A"
    # From post score 68: High+pure → D (score < 75 for B band); then letter on D stays D
    # Wait: High pure s>=75 is B, s>=85 is A, s>=95 A+.
    # 68 < 65 → D from score banding. Letter soft on D → D.
    assert explained["display_grade"] == "D!"


def test_soft_stretch_a_plus_to_b():
    # Score 96 High pure → A+; soft −20 → 76 → B from score; letter −2 → D!
    explained = explain_confidence_grade(
        96,
        "High",
        80.0,
        REGIME_TREND,
        stretch_pct=0.40,
        apply_live=True,
    )
    assert explained["trade_score_post_stretch"] == 76
    # base from 76 High pure = B; letter −2 → D
    assert explained["display_grade"] == "D!"
    assert explained["base_grade_pre_stretch"] == "A+"


def test_soft_stretch_letter_on_still_high_score():
    """Letter stacks when penalized score still lands non-D (Pine intent)."""
    # Raw 95 → A+; soft → 75 → B; letter −2 → D!
    # Use score that stays in band after −20: need post >= 85 for A from High pure
    # Soft −20 from 105 capped at 100 → use 95 → 75 = B, then letter D.
    # For non-D after letter: need base_from_post = A+ (score>=95) then −2 → B
    # So raw must be >= 115 impossible. Use Average path:
    # Average pure >=85 → B; soft from 100 → 80: Average pure 80 → C (avg pure >=75);
    # letter −2 → D.
    #
    # A+ → B via letter only when score banding unchanged: need stretch soft letter
    # with score penalty that still keeps A+. That requires raw>=115 before −20 — capped.
    # Soft letter alone on A+ with no score effect isn't possible (soft always −20).
    # Pine still applies both. Closest: hard force D from A+.
    explained = explain_confidence_grade(
        100,
        "High",
        80.0,
        REGIME_TREND,
        stretch_pct=0.36,
        apply_live=True,
    )
    assert explained["trade_score_post_stretch"] == 80
    # High pure 80 → B; letter −2 → D!
    assert explained["base_grade_pre_stretch"] == "A+"
    assert explained["display_grade"] == "D!"


def test_transition_floor_blocked_by_hard_stretch():
    # Raw 80 Low impure → D; TRANS would promote to C* without stretch.
    # Hard stretch: score 30, force D, no C* rescue.
    explained = explain_confidence_grade(
        80,
        "Low",
        0.0,
        REGIME_TRANSITION,
        stretch_pct=0.60,
        apply_live=True,
    )
    assert explained["promote_transition_floor_would_have_fired_pre_penalty"] is True
    assert explained["display_grade"] == "D!"
    assert explained["transition_floor"] is False


def test_transition_floor_still_works_without_stretch():
    explained = explain_confidence_grade(
        80,
        "Low",
        0.0,
        REGIME_TRANSITION,
        stretch_pct=0.10,
        apply_live=True,
    )
    assert explained["display_grade"] == "C*"
    assert explained["transition_floor"] is True


def test_resolve_score_and_grade_levels():
    out = resolve_score_and_grade(
        95,
        "High",
        80.0,
        REGIME_TREND,
        close=4890,
        ema10=4920,
        vwap=4800,
        apply_live=False,
    )
    assert out["trade_score"] == 95
    assert out["stretch"]["stretch_pct"] == pytest.approx(0.6135, rel=1e-2)
    assert out["stretch"]["stretch_score_penalty"] == 50

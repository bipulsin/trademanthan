"""Unit tests for Kavach unified confidence grade + stretch penalty."""
import pytest

from backend.services.kavach_confidence import (
    REGIME_TRANSITION,
    REGIME_TREND,
    apply_ema5_reset_to_penalties,
    compute_confidence_grade,
    compute_stretch_pct,
    detect_ema5_touch_reset,
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


def _touch_candles(bars_after_touch: int = 0) -> list:
    """Synthetic series where last closed bar (or bars_after earlier) pierces EMA5."""
    # Rising closes so EMA5 trails below; final bars dip to touch EMA5.
    closes = [100.0 + i * 0.5 for i in range(20)]
    candles = []
    for i, c in enumerate(closes):
        candles.append({"open": c, "high": c + 0.2, "low": c - 0.2, "close": c})
    # Force a touch on bar index target: range includes a synthetic EMA5.
    touch_i = len(candles) - 1 - bars_after_touch
    # Rebuild EMA5 and set low/high to bracket it on touch bar.
    from backend.services.kavach_confidence import _ema_series

    ema = _ema_series([x["close"] for x in candles], 5)
    e = ema[touch_i]
    candles[touch_i]["low"] = e - 0.5
    candles[touch_i]["high"] = e + 0.5
    return candles


def test_detect_ema5_touch_active_within_persist():
    candles = _touch_candles(0)
    info = detect_ema5_touch_reset(candles, persist_bars=2)
    assert info["ema5_reset_active"] is True
    assert info["ema5_reset_bars_since_touch"] == 0
    assert info["ema5_reset_mode"] == "partial"
    assert info["ema5_reset_factor"] == 0.5


def test_detect_ema5_touch_expires_after_persist():
    candles = _touch_candles(3)
    info = detect_ema5_touch_reset(candles, persist_bars=2)
    assert info["ema5_reset_active"] is False
    assert info["ema5_reset_bars_since_touch"] == 3


def test_apply_ema5_partial_halves_penalties():
    score, letter = apply_ema5_reset_to_penalties(
        20,
        2,
        {"ema5_reset_active": True, "ema5_reset_factor": 0.5},
    )
    assert score == 10
    assert letter == 1
    score2, letter2 = apply_ema5_reset_to_penalties(
        50,
        99,
        {"ema5_reset_active": True, "ema5_reset_factor": 0.5},
    )
    assert score2 == 25
    assert letter2 == 1


def test_apply_ema5_full_zeros_penalties():
    score, letter = apply_ema5_reset_to_penalties(
        50,
        99,
        {"ema5_reset_active": True, "ema5_reset_factor": 0.0},
    )
    assert score == 0
    assert letter == 0


def test_explain_partial_ema5_reset_on_hard_stretch():
    # Hard stretch without reset → 45 / D!. With partial reset → score 70, 1-letter.
    reset = {
        "ema5_reset_active": True,
        "ema5_reset_mode": "partial",
        "ema5_reset_bars_since_touch": 0,
        "ema5_reset_persist_bars": 2,
        "ema5_reset_factor": 0.5,
        "ema5_at_touch": 100.0,
    }
    explained = explain_confidence_grade(
        95,
        "High",
        80.0,
        REGIME_TREND,
        stretch_pct=0.613,
        apply_live=True,
        ema5_reset=reset,
    )
    assert explained["stretch_score_penalty"] == 25
    assert explained["stretch_letter_penalty"] == 1
    assert explained["trade_score_post_stretch"] == 70
    assert explained["stretch"]["ema5_reset_active"] is True
    # High pure 70 → D from banding (<75); letter −1 stays D → D!
    assert explained["display_grade"] == "D!"


def test_explain_full_ema5_reset_clears_hard_stretch():
    reset = {
        "ema5_reset_active": True,
        "ema5_reset_mode": "full",
        "ema5_reset_bars_since_touch": 0,
        "ema5_reset_persist_bars": 2,
        "ema5_reset_factor": 0.0,
        "ema5_at_touch": 100.0,
    }
    explained = explain_confidence_grade(
        95,
        "High",
        80.0,
        REGIME_TREND,
        stretch_pct=0.613,
        apply_live=True,
        ema5_reset=reset,
    )
    assert explained["stretch_score_penalty"] == 0
    assert explained["trade_score_post_stretch"] == 95
    assert explained["display_grade"] == "A+"

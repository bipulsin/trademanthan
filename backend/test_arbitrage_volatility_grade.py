"""Unit tests for volatility grade formula and banding (no live Upstox / DB)."""
from backend.services.arbitrage_volatility_grade import (
    GRADE_HIGH,
    GRADE_LOW,
    GRADE_MOD,
    grade_from_score,
    margin_rupees_from_item,
    margins_from_charges_response,
    volatility_score,
)


def test_score_formula():
    # 100 * 2000 / (100 * 50) = 40
    assert volatility_score(100.0, 50, 2000.0) == 40.0
    # 100 * 10000 / (500 * 100) = 20
    assert volatility_score(500.0, 100, 10000.0) == 20.0


def test_score_zero_or_missing_inputs():
    assert volatility_score(100.0, 50, 0) is None
    assert volatility_score(100.0, 50, None) is None
    assert volatility_score(0, 50, 1000) is None
    assert volatility_score(None, 50, 1000) is None
    assert volatility_score(100.0, 0, 1000) is None
    assert volatility_score(100.0, None, 1000) is None


def test_banding_boundaries():
    assert grade_from_score(20.0) == GRADE_LOW
    assert grade_from_score(0.1) == GRADE_LOW
    assert grade_from_score(20.0001) == GRADE_MOD
    assert grade_from_score(30.0) == GRADE_MOD
    assert grade_from_score(30.0001) == GRADE_HIGH
    assert grade_from_score(None) is None


def test_margin_parse_span_exposure():
    m = margin_rupees_from_item(
        {"span_margin": 8000, "exposure_margin": 2000, "additional_margin": 0}
    )
    assert m == 10000.0
    assert margin_rupees_from_item({"required_margin": 1234.5}) == 1234.5
    assert margin_rupees_from_item({"total_margin": 0}) is None
    assert margin_rupees_from_item(None) is None


def test_margins_from_batch_response():
    resp = {
        "status": "success",
        "data": {
            "margins": [
                {"required_margin": 111},
                {"required_margin": 222},
            ],
            "required_margin": 333,
        },
    }
    out = margins_from_charges_response(resp, 2)
    assert out == [111.0, 222.0]


def test_margins_single_top_level_when_no_list():
    resp = {"status": "success", "data": {"required_margin": 5000}}
    assert margins_from_charges_response(resp, 1) == [5000.0]
    # Batch of 2 without per-leg rows must not assign the summed total to both.
    assert margins_from_charges_response(resp, 2) == [None, None]

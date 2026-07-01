"""Tests for the Daily RS Checklist decision engine (pure evaluator)."""
from backend.services.daily_checklist import (
    D_ELIMINATED,
    D_GO,
    D_NOTRADE,
    D_UNASSESSED,
    D_WATCH,
    SEC_GO,
    SEC_OUT,
    SEC_WATCH,
    adx_935_status,
    evaluate,
)


def _full_long_pass():
    """A LONG row with all 9 gate conditions passing."""
    return {
        "direction": "LONG",
        "news_clean": True,
        "entry_time": "11:30",
        "kavach_score_entry": 87,
        "confidence": "A",
        "trading_state": "BUY",
        "ema_vs_vwap": "Above",
        "supertrend": "Bullish",
        "macd": "Bullish",
        "adx_entry": 34.5,
        "di_alignment": "DI+>DI-",
        "volume": "High",
    }


def test_all_pass_is_go():
    r = evaluate(_full_long_pass())
    assert r["gate_score"] == 9
    assert r["decision"] == D_GO
    assert r["section"] == SEC_GO


def test_adverse_news_eliminates():
    row = _full_long_pass()
    row["news_clean"] = False
    r = evaluate(row)
    assert r["decision"] == D_ELIMINATED
    assert r["section"] == SEC_OUT


def test_time_outside_window_hard_fails():
    for bad in ("09:40", "14:31", "15:00"):
        row = _full_long_pass()
        row["entry_time"] = bad
        r = evaluate(row)
        assert r["time_ok"] is False
        assert r["decision"] == D_NOTRADE
        assert r["section"] == SEC_OUT


def test_time_window_boundaries_pass():
    for ok in ("09:45", "14:30", "12:00"):
        row = _full_long_pass()
        row["entry_time"] = ok
        assert evaluate(row)["time_ok"] is True


def test_misaligned_state_hard_fails_long():
    row = _full_long_pass()
    row["trading_state"] = "SELL"
    r = evaluate(row)
    assert r["state_ok"] is False
    assert r["decision"] == D_NOTRADE


def test_short_direction_rules():
    row = {
        "direction": "SHORT",
        "news_clean": True,
        "entry_time": "11:30",
        "kavach_score_entry": 80,
        "confidence": "B",
        "trading_state": "SELL",
        "ema_vs_vwap": "Below",
        "supertrend": "Bearish",
        "macd": "Bearish",
        "adx_entry": 30,
        "di_alignment": "DI->DI+",
        "volume": "Normal",
    }
    r = evaluate(row)
    assert r["gate_score"] == 9
    assert r["decision"] == D_GO


def test_counter_rs_requires_a_grade():
    row = _full_long_pass()
    row["counter_rs"] = True
    row["confidence"] = "B"  # B is fine same-direction, but counter-RS needs A
    r = evaluate(row)
    assert r["confidence_ok"] is False
    assert r["gate_score"] == 8
    assert r["decision"] == D_WATCH  # 8/9 -> watch
    assert r["eligibility_note"] == "Requires A-grade — counter-RS direction"


def test_extended_maturity_requires_a_grade():
    row = _full_long_pass()
    row["maturity_tag"] = "EXTENDED"
    row["confidence"] = "B"
    r = evaluate(row)
    assert r["confidence_ok"] is False
    assert r["gate_score"] == 8
    assert r["decision"] == D_WATCH
    assert r["eligibility_note"] == "Requires A-grade — EXTENDED move maturity"


def test_stretched_maturity_requires_a_grade():
    row = _full_long_pass()
    row["maturity_tag"] = "STRETCHED"
    row["confidence"] = "B"
    r = evaluate(row)
    assert r["confidence_ok"] is False
    assert "STRETCHED move maturity" in (r["eligibility_note"] or "")


def test_fresh_maturity_allows_b_grade():
    row = _full_long_pass()
    row["maturity_tag"] = "FRESH"
    row["confidence"] = "B"
    r = evaluate(row)
    assert r["confidence_ok"] is True
    assert not r.get("eligibility_note")


def test_confidence_cd_fails():
    for c in ("C", "D"):
        row = _full_long_pass()
        row["confidence"] = c
        assert evaluate(row)["confidence_ok"] is False


def test_macd_crossing_counts_for_long():
    row = _full_long_pass()
    row["macd"] = "Crossing"
    assert evaluate(row)["macd_ok"] is True


def test_adx_below_25_fails_even_with_di():
    row = _full_long_pass()
    row["adx_entry"] = 22
    assert evaluate(row)["adx_ok"] is False


def test_adx_di_misalignment_fails():
    row = _full_long_pass()
    row["di_alignment"] = "DI->DI+"  # wrong for LONG
    assert evaluate(row)["adx_ok"] is False


def test_low_volume_soft_fails_gate():
    row = _full_long_pass()
    row["volume"] = "Low"
    r = evaluate(row)
    assert r["volume_ok"] is False
    assert r["gate_score"] == 8
    assert r["section"] == SEC_WATCH  # 8/9 still WATCH, not hard fail


def test_watch_threshold_six():
    # Exactly 6 of 9 -> WATCH; drop to 5 -> NO TRADE.
    base = _full_long_pass()
    # Remove 3 conditions (leave them unset) -> 6 pass.
    for f in ("supertrend", "macd", "volume"):
        base[f] = None
    r6 = evaluate(base)
    assert r6["gate_score"] == 6 and r6["decision"] == D_WATCH and r6["section"] == SEC_WATCH
    base["adx_entry"] = None  # now 5 pass
    r5 = evaluate(base)
    assert r5["gate_score"] == 5 and r5["decision"] == D_NOTRADE and r5["section"] == SEC_OUT


def test_unassessed_when_nothing_filled():
    r = evaluate({"direction": "LONG"})
    assert r["gate_score"] == 0
    assert r["decision"] == D_UNASSESSED
    assert r["section"] == SEC_WATCH


def test_adx_935_status_buckets():
    assert adx_935_status(25.27) == "immediate"
    assert adx_935_status(22) == "recheck"
    assert adx_935_status(19.9) == "watch"
    assert adx_935_status(None) == ""


def test_confidence_grade_from_score():
    from backend.services.daily_checklist import _confidence_grade

    assert _confidence_grade(95) == "A"
    assert _confidence_grade(85) == "B"
    assert _confidence_grade(75) == "C"
    assert _confidence_grade(60) == "D"


def test_volume_label_buckets():
    from backend.services.daily_checklist import _volume_label

    assert _volume_label(1.5) == "High"
    assert _volume_label(0.8) == "Normal"
    assert _volume_label(0.3) == "Low"

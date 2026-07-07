"""Tests for Fast Watch display ranking, retention, and dedupe."""
from datetime import datetime, timedelta

import pytz

from backend.services.rs_fast_watch import (
    _dedupe_by_symbol,
    _select_featured,
    grade_rank,
    is_degraded,
    momentum_label,
    rank_key,
    state_rank,
)

IST = pytz.timezone("Asia/Kolkata")


def test_grade_rank_order():
    assert grade_rank("A+") > grade_rank("A") > grade_rank("B") > grade_rank("D")


def test_state_rank_buy_above_ready():
    assert state_rank("BUY", "LONG") > state_rank("READY", "LONG")
    assert state_rank("SELL", "SHORT") > state_rank("READY SHORT", "SHORT")


def test_rank_key_a_grade_beats_fresh_d():
    strong = {
        "direction": "LONG",
        "live_grade": "A",
        "live_kavach": "BUY",
        "live_score": 93,
        "confidence_grade": "A",
        "kavach_state": "BUY",
        "trade_score": 93,
    }
    weak = {
        "direction": "LONG",
        "live_grade": "D",
        "live_kavach": "BUY",
        "live_score": 100,
        "confidence_grade": "D",
        "kavach_state": "BUY",
        "trade_score": 100,
    }
    assert rank_key(strong) > rank_key(weak)


def test_dedupe_keeps_stronger_symbol():
    items = [
        {"symbol": "X", "direction": "LONG", "live_grade": "B", "live_kavach": "BUY", "live_score": 80},
        {"symbol": "X", "direction": "LONG", "live_grade": "A", "live_kavach": "BUY", "live_score": 85},
    ]
    out = _dedupe_by_symbol(items)
    assert len(out) == 1
    assert out[0]["live_grade"] == "A"


def test_momentum_labels():
    assert momentum_label(80, 90) == "rising"
    assert momentum_label(90, 80) == "fading"
    assert momentum_label(85, 87) == "flat"


def test_is_degraded_grade_drop():
    item = {
        "direction": "LONG",
        "flip_grade": "A",
        "live_grade": "D",
        "flip_score": 90,
        "live_score": 85,
        "kavach_state": "BUY",
        "live_kavach": "BUY",
    }
    assert is_degraded(item) is True


def test_retention_holds_slot_during_window():
    now = datetime.now(IST)
    session = "2099-01-01"
    candidates = [
        {
            "symbol": "HOLD",
            "direction": "LONG",
            "live_grade": "B",
            "live_kavach": "BUY",
            "live_score": 80,
            "confidence_grade": "B",
            "kavach_state": "BUY",
            "trade_score": 80,
            "first_flip_at": (now - timedelta(minutes=5)).isoformat(),
        },
        {
            "symbol": "NEW",
            "direction": "LONG",
            "live_grade": "A+",
            "live_kavach": "BUY",
            "live_score": 100,
            "confidence_grade": "A+",
            "kavach_state": "BUY",
            "trade_score": 100,
            "first_flip_at": now.isoformat(),
        },
    ]
    from backend.services import rs_fast_watch as fw

    fw._featured_slots.clear()
    fw._featured_slots[session] = {
        "LONG": {"HOLD": now - timedelta(minutes=3)},
    }
    out = _select_featured(candidates, "LONG", session, top_n=1, retention_min=12, now=now)
    assert len(out) == 1
    assert out[0]["symbol"] == "HOLD"
    assert out[0]["retention_hold"] is True

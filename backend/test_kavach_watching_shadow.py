"""Unit tests for Watching Grade A shadow + READY exit+4 helpers (no DB)."""
from __future__ import annotations

from datetime import datetime

import pytz

from backend.services.kavach_watching_shadow import (
    _classify_leave,
    is_grade_a_family,
    is_watching_grade_a,
)
from backend.services.kavach_ready_exit_plus4_shadow import (
    _signed_pnl,
    close_plus_n_10m,
)

IST = pytz.timezone("Asia/Kolkata")


def test_is_grade_a_family():
    assert is_grade_a_family("A+")
    assert is_grade_a_family("A")
    assert is_grade_a_family("A!")
    assert not is_grade_a_family("B")
    assert not is_grade_a_family("C")


def test_is_watching_grade_a_prefers_pine():
    stock = {
        "pine_readiness": "WATCHING",
        "confidence": "A+",
        "trade_state": "WATCH",
    }
    assert is_watching_grade_a(stock, in_lock=True)
    stock["trade_state"] = "READY"
    assert not is_watching_grade_a(stock, in_lock=True)
    stock["trade_state"] = "WATCH"
    stock["pine_readiness"] = "READY TO BUY"
    assert not is_watching_grade_a(stock, in_lock=True)


def test_classify_leave_reasons():
    now = IST.localize(datetime(2026, 7, 24, 11, 0))
    assert (
        _classify_leave(
            prev_dir="LONG",
            stock={"trade_state": "READY", "confidence": "A"},
            still_in_universe=True,
            in_lock=True,
            now=now,
        )
        == "promoted_to_ready"
    )
    assert (
        _classify_leave(
            prev_dir="LONG",
            stock={"trade_state": "WATCH", "confidence": "B"},
            still_in_universe=True,
            in_lock=True,
            now=now,
        )
        == "grade_decay_below_a"
    )
    assert (
        _classify_leave(
            prev_dir="LONG",
            stock={"trade_state": "WATCH", "confidence": "A", "direction": "SHORT"},
            still_in_universe=True,
            in_lock=True,
            now=now,
        )
        == "direction_flip"
    )
    assert (
        _classify_leave(
            prev_dir="LONG",
            stock=None,
            still_in_universe=False,
            in_lock=False,
            now=now,
        )
        == "lock_removed"
    )
    eod = IST.localize(datetime(2026, 7, 24, 15, 20))
    assert (
        _classify_leave(
            prev_dir="LONG",
            stock={"trade_state": "WATCH", "confidence": "A", "pine_readiness": "WATCHING"},
            still_in_universe=True,
            in_lock=True,
            now=eod,
        )
        == "session_eod"
    )
    # Specific reason wins over EOD clock.
    assert (
        _classify_leave(
            prev_dir="LONG",
            stock={"trade_state": "WATCH", "confidence": "B", "pine_readiness": "WATCHING"},
            still_in_universe=True,
            in_lock=True,
            now=eod,
        )
        == "grade_decay_below_a"
    )


def test_signed_pnl():
    assert _signed_pnl("LONG", 100.0, 110.0) == 10.0
    assert _signed_pnl("SHORT", 100.0, 90.0) == 10.0


def test_close_plus_n_10m_basic():
    # Soft check: empty / insufficient candles → None (no crash).
    assert close_plus_n_10m([], exit_at=IST.localize(datetime(2026, 7, 24, 10, 0)), n=4) is None
    # Signed PnL path used by the shadow is covered above; full 10m aggregation
    # needs prior-day bars and is exercised live via enrich.

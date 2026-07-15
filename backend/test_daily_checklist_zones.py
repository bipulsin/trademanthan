"""Tests for Zone 1 session-bar signals and READY downgrades."""
from datetime import datetime, timedelta

import pytz

from backend.services.daily_checklist_zones import (
    STATE_READY,
    STATE_WAIT,
    annotate_regime_context,
    apply_zone_downgrades,
    compromised_lock_banner,
    direction_imbalance,
    regime_research_snapshot,
    removal_counts_last_hour,
    rotation_chip,
)

IST = pytz.timezone("Asia/Kolkata")


def test_rotation_chip_on_thin_overlap():
    chip = rotation_chip(
        {"rotation_day_type": "ROTATION", "bull_overlap": 0, "bear_overlap": 0}
    )
    assert chip and chip["active"]
    assert "ROTATION" in chip["label"]


def test_rotation_chip_hidden_when_overlap_high():
    assert rotation_chip(
        {"rotation_day_type": "ROTATION", "bull_overlap": 3, "bear_overlap": 2}
    ) is None
    assert rotation_chip({"rotation_day_type": "CONTINUATION", "bull_overlap": 0, "bear_overlap": 0}) is None


def test_direction_imbalance_bear_heavy():
    now = IST.localize(datetime(2026, 7, 14, 11, 15))
    removals = []
    for i, sym in enumerate(["A", "B", "C", "D", "E", "F"]):
        removals.append(
            {
                "symbol": sym,
                "rule_tag": "R2",
                "direction": "BEAR",
                "at": (now - timedelta(minutes=10 + i)).isoformat(),
            }
        )
    removals.append(
        {
            "symbol": "X",
            "rule_tag": "R2",
            "direction": "BULL",
            "at": (now - timedelta(minutes=5)).isoformat(),
        }
    )
    imb = direction_imbalance(removals, now=now, threshold=3)
    assert imb and imb["unstable_direction"] == "BEAR"
    assert imb["bear_removals"] == 6
    counts = removal_counts_last_hour(removals, now=now)
    assert counts["BEAR"] == 6 and counts["BULL"] == 1


def test_compromised_manual_lock():
    assert compromised_lock_banner("manual")["active"]
    assert compromised_lock_banner("auto") is None


def test_apply_zone_downgrades_imbalance_and_compromised():
    stocks = [
        {"symbol": "TIINDIA", "direction": "SHORT", "trade_state": STATE_READY, "promoted_at": "10:40"},
        {"symbol": "BIOCON", "direction": "LONG", "trade_state": STATE_READY, "promoted_at": None},
        {"symbol": "MCX", "direction": "LONG", "trade_state": STATE_READY, "promoted_at": "10:05"},
    ]
    apply_zone_downgrades(
        stocks,
        imbalance={"active": True, "unstable_direction": "BEAR"},
        compromised={"active": True},
    )
    assert stocks[0]["trade_state"] == STATE_WAIT
    assert "BEAR" in stocks[0]["trade_state_reason"]
    # morning-lock-only LONG downgraded by compromised
    assert stocks[1]["trade_state"] == STATE_WAIT
    # intraday-promoted LONG stays READY
    assert stocks[2]["trade_state"] == STATE_READY


def test_annotate_regime_context_visibility_only():
    """TRANSITION + BEAR lean: LONG READY gets flags, stays READY (no hard block)."""
    now = IST.localize(datetime(2026, 7, 15, 10, 50))
    removals = [
        {
            "symbol": f"S{i}",
            "rule_tag": "R2",
            "direction": "BEAR",
            "at": (now - timedelta(minutes=5 + i)).isoformat(),
        }
        for i in range(8)
    ]
    imb = direction_imbalance(removals, now=now, threshold=3)
    assert imb and imb["unstable_direction"] == "BEAR"

    stocks = [
        {"symbol": "CHOLAFIN", "direction": "LONG", "trade_state": STATE_READY, "gate_badges": []},
        {"symbol": "SHORT1", "direction": "SHORT", "trade_state": STATE_WAIT, "gate_badges": []},
        {"symbol": "BLOCKED1", "direction": "LONG", "trade_state": "BLOCKED", "gate_badges": []},
    ]
    annotate_regime_context(
        stocks,
        market_regime="TRANSITION",
        market_regime_label="TRANSITION — unconfirmed regime",
        imbalance=imb,
        removals=removals,
        now=now,
    )
    # LONG vs BEAR lean → counter-regime, still READY
    assert stocks[0]["trade_state"] == STATE_READY
    assert "REGIME UNSTABLE" in stocks[0]["gate_badges"]
    assert "COUNTER-REGIME" in stocks[0]["gate_badges"]
    assert any(str(b).startswith("CHURN") for b in stocks[0]["gate_badges"])
    assert stocks[0]["regime_context"]["counter_regime"] is True
    assert stocks[0]["regime_context"]["removals_last_hour"] == 8

    # SHORT aligns with BEAR lean → unstable + churn, not counter
    assert "REGIME UNSTABLE" in stocks[1]["gate_badges"]
    assert "COUNTER-REGIME" not in stocks[1]["gate_badges"]
    assert stocks[1]["regime_context"]["counter_regime"] is False

    # BLOCKED: snapshot only, no flags
    assert stocks[2]["regime_context"]["flags"] == []


def test_regime_research_snapshot_fields():
    snap = regime_research_snapshot(
        market_regime="TRANSITION",
        market_regime_label="TRANSITION — unconfirmed regime",
        imbalance={"active": True, "unstable_direction": "BEAR"},
        removals=[],
        direction="LONG",
    )
    assert snap["regime_unconfirmed"] is True
    assert snap["regime_lean"] == "BEAR"
    assert snap["counter_regime"] is True
    assert snap["removals_last_hour"] == 0

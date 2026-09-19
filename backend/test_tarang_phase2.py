"""Unit tests: Tarang Phase 2 gates, sizing, skew."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from backend.services.tarang.gates import (
    aggregate_status,
    compute_iv_percentile,
    decide_structure_from_skew,
    gate_credit_fraction,
    gate_event_blackout,
    gate_expiry_dte,
    gate_iv_percentile,
    gate_iv_vs_rv,
    gate_liquidity,
    gate_portfolio_limit,
    gate_sizing,
    GateResult,
)
from backend.services.tarang.structures import floor_units, max_loss_per_unit_inr


def test_liquidity_pass_fail():
    ok = gate_liquidity(
        [
            {"right": "PE", "strike": 100, "bid": 9.5, "ask": 10.5, "mid": 10, "oi": 100},
        ],
        max_spread_pct=15,
    )
    assert ok.passed
    bad = gate_liquidity(
        [
            {"right": "PE", "strike": 100, "bid": 5, "ask": 15, "mid": 10, "oi": 100},
        ],
        max_spread_pct=15,
    )
    assert not bad.passed


def test_iv_percentile_warmup():
    g = gate_iv_percentile(40.0, snapshot_count=10, min_percentile=50, min_snapshots=60)
    assert g.passed
    assert g.status_hint == "WARMING_UP"
    g2 = gate_iv_percentile(40.0, snapshot_count=60, min_percentile=50, min_snapshots=60)
    assert not g2.passed
    g3 = gate_iv_percentile(55.0, snapshot_count=60, min_percentile=50, min_snapshots=60)
    assert g3.passed


def test_iv_vs_rv():
    assert gate_iv_vs_rv(0.40, 0.30, 0.10).passed
    assert not gate_iv_vs_rv(0.31, 0.30, 0.10).passed


def test_expiry_dte():
    g = gate_expiry_dte("2099-01-01", None, min_dte=7, max_dte=None)
    assert g.passed and g.actual is not None and g.actual >= 7
    g2 = gate_expiry_dte(None, 3, min_dte=7)
    assert not g2.passed


def test_event_blackout():
    now = datetime(2026, 9, 19, 10, 0, tzinfo=timezone.utc)
    ev = [
        {
            "id": "eia",
            "label": "EIA",
            "applies_to": ["CL", "ENERGY"],
            "starts_at": (now + timedelta(hours=1)).isoformat(),
            "blackout_hours_before": 2.5,
        }
    ]
    g = gate_event_blackout("CL", "ENERGY", ev, now=now)
    assert not g.passed and g.status_hint == "BLOCKED"
    g2 = gate_event_blackout("BTC", "CRYPTO", ev, now=now)
    assert g2.passed


def test_credit_and_sizing():
    assert gate_credit_fraction(1.0, 5.0, 0.20).passed
    assert not gate_credit_fraction(0.5, 5.0, 0.20).passed
    ml = max_loss_per_unit_inr(5.0, 1.5, lot_size=1250, venue="upstox_mcx")
    assert ml == 4375.0
    assert floor_units(5000, ml) == 1
    assert floor_units(3000, ml) == 0
    assert gate_sizing(ml, 5000, 1).passed
    assert not gate_sizing(ml, 3000, 0).passed


def test_portfolio_limit():
    assert gate_portfolio_limit(40000, 25000, 80000).passed
    assert not gate_portfolio_limit(60000, 25000, 80000).passed


def test_skew_structure():
    bal = decide_structure_from_skew(0.40, 0.39, 0.08)
    assert bal["structure"] == "iron_condor"
    put = decide_structure_from_skew(0.50, 0.40, 0.08)
    assert put["structure"] == "put_credit_spread"
    call = decide_structure_from_skew(0.40, 0.50, 0.08)
    assert call["structure"] == "call_credit_spread"


def test_aggregate_status():
    gates = [
        GateResult("a", True),
        GateResult("b", True, status_hint="WARMING_UP"),
    ]
    assert aggregate_status(gates) == "WARMING_UP"
    gates2 = [GateResult("a", True), GateResult("b", False, status_hint="WATCHING")]
    assert aggregate_status(gates2) == "WATCHING"
    gates3 = [GateResult("a", False, status_hint="BLOCKED")]
    assert aggregate_status(gates3) == "BLOCKED"
    assert aggregate_status([GateResult("a", True)]) == "QUALIFIED"


def test_compute_iv_percentile():
    hist = [0.2, 0.3, 0.4, 0.5, 0.6]
    p = compute_iv_percentile(hist, 0.4)
    assert p == 60.0


def test_energy_budget_operator_set():
    from backend.services.tarang.config import energy_budget

    e = energy_budget()
    assert e["per_trade_budget_inr"] == 5000
    assert e["hard_cap_inr"] == 10000
    assert e["portfolio_limit_inr"] == 10000


def test_delta_sizing_usd_to_inr():
    ml = max_loss_per_unit_inr(2000, 300, contract_value=0.001, venue="delta_india", usd_inr=83)
    # (1700) * 0.001 * 83 = 141.1
    assert abs(ml - 141.1) < 0.01
    assert floor_units(3000, ml) >= 1

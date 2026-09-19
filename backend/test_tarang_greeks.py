"""Unit tests: Black-76 / BS and IV unit normalization for Kosmic Tarang."""
from __future__ import annotations

from backend.services.tarang.greeks import (
    black76_delta,
    black76_price,
    bs_delta,
    bs_price,
    implied_vol_black76,
    implied_vol_bs,
)
from backend.services.tarang.iv_normalize import normalize_iv


def test_normalize_iv_decimal():
    dec, raw, unit = normalize_iv(0.43)
    assert unit == "decimal"
    assert abs(dec - 0.43) < 1e-9
    assert raw == 0.43


def test_normalize_iv_percent_heuristic():
    dec, raw, unit = normalize_iv(43.0)
    assert unit == "percent"
    assert abs(dec - 0.43) < 1e-9
    assert raw == 43.0


def test_normalize_iv_percent_hint():
    dec, raw, unit = normalize_iv(50.0, "percent")
    assert unit == "percent"
    assert abs(dec - 0.50) < 1e-9


def test_normalize_iv_rejects_zero():
    dec, raw, unit = normalize_iv(0.0)
    assert dec is None
    assert raw == 0.0


def test_black76_atm_call_delta_near_half():
    F, K, T, sig = 100.0, 100.0, 30 / 365.25, 0.40
    d = black76_delta(F, K, T, sig, "CE")
    assert d is not None
    assert 0.45 < d < 0.55
    p = black76_price(F, K, T, sig, "CE")
    assert p is not None and p > 0
    iv = implied_vol_black76(p, F, K, T, "CE")
    assert iv is not None
    assert abs(iv - sig) < 1e-3


def test_black76_put_delta_negative():
    d = black76_delta(100.0, 100.0, 30 / 365.25, 0.35, "PE")
    assert d is not None and d < 0


def test_bs_roundtrip():
    S, K, T, r, sig = 100.0, 100.0, 7 / 365.25, 0.0, 0.50
    p = bs_price(S, K, T, sig, r, "CE")
    assert p and p > 0
    iv = implied_vol_bs(p, S, K, T, r, "CE")
    assert iv is not None
    assert abs(iv - sig) < 1e-3
    d = bs_delta(S, K, T, sig, r, "CE")
    assert d is not None and 0.4 < d < 0.6


def test_energy_budget_defaults():
    from backend.services.tarang.config import energy_budget

    e = energy_budget()
    assert e["per_trade_budget_inr"] == 40000
    assert e["hard_cap_inr"] == 50000
    assert e["portfolio_limit_inr"] == 80000

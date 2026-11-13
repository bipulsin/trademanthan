"""Unit tests for Fear & Greed normalize / OI / zones / rebalance / fixtures."""
from __future__ import annotations

from typing import Any, Dict, List

from backend.services.fear_greed.compute import (
    combine_scores,
    compute_fear_greed_from_candles,
    interpret_oi,
    map_zone,
    normalize_series,
    percentile_rank,
    rebalance_weights,
)


def test_map_zone_boundaries():
    assert map_zone(0) == "Extreme Fear"
    assert map_zone(24) == "Extreme Fear"
    assert map_zone(25) == "Fear"
    assert map_zone(49) == "Fear"
    assert map_zone(50) == "Greed"
    assert map_zone(74) == "Greed"
    assert map_zone(75) == "Extreme Greed"
    assert map_zone(100) == "Extreme Greed"


def test_percentile_rank_and_normalize_invert():
    hist = [10.0, 20.0, 30.0, 40.0, 50.0]
    # Mid value
    assert abs(percentile_rank(30.0, hist) - 50.0) < 1e-6
    # Highest → near 100
    assert percentile_rank(50.0, hist) > 80
    # Lowest → near 0
    assert percentile_rank(10.0, hist) < 20

    series = [1.0, 2.0, 3.0, 4.0, 5.0]
    high = normalize_series(series, invert=False)
    assert high is not None and high > 80
    inv = normalize_series(series, invert=True)
    assert inv is not None and inv < 20
    assert abs((high + inv) - 100.0) < 1e-6


def test_interpret_oi_four_combinations():
    lb = interpret_oi(1.0, 10.0)
    assert lb["label"] == "long_buildup"
    assert lb["raw"] == 1.0
    assert lb["normalized"] == 100.0

    sb = interpret_oi(-1.0, 10.0)
    assert sb["label"] == "short_buildup"
    assert sb["raw"] == -1.0
    assert sb["normalized"] == 0.0

    sc = interpret_oi(1.0, -10.0)
    assert sc["label"] == "short_covering"
    assert sc["raw"] == 0.4
    assert sc["normalized"] == 70.0

    lu = interpret_oi(-1.0, -10.0)
    assert lu["label"] == "long_unwinding"
    assert lu["raw"] == -0.4
    assert lu["normalized"] == 30.0


def test_rebalance_weights_skips_missing():
    w = rebalance_weights(["momentum", "volatility"], {"momentum": 1.0, "volatility": 1.0, "volume": 1.0})
    assert abs(sum(w.values()) - 1.0) < 1e-9
    assert abs(w["momentum"] - 0.5) < 1e-9
    assert "volume" not in w

    empty = rebalance_weights([], {"momentum": 1.0})
    assert empty == {}

    # Zero weights fall back to equal among available
    eq = rebalance_weights(["a", "b"], {"a": 0.0, "b": 0.0})
    assert abs(eq["a"] - 0.5) < 1e-9


def test_combine_scores_rebalance_and_clamp():
    score, eff = combine_scores({"momentum": 100.0, "volatility": 0.0})
    assert score == 50
    assert abs(sum(eff.values()) - 1.0) < 1e-9

    score2, _ = combine_scores({"momentum": 100.0}, {"momentum": 1.0})
    assert score2 == 100

    score3, _ = combine_scores({})
    assert score3 == 50


def _synth_candles(
    n: int,
    *,
    start: float = 100.0,
    drift: float = 0.0,
    vol_scale: float = 1.0,
    volume: float = 1_000_000.0,
    oi_start: float = 100_000.0,
    oi_drift: float = 0.0,
) -> List[Dict[str, Any]]:
    """Synthetic daily candles with optional OI."""
    out: List[Dict[str, Any]] = []
    px = start
    oi = oi_start
    for i in range(n):
        px = px * (1.0 + drift) + (0.01 * vol_scale if i % 2 == 0 else -0.01 * vol_scale)
        hi = px * (1.0 + 0.005 * vol_scale)
        lo = px * (1.0 - 0.005 * vol_scale)
        oi = max(0.0, oi + oi_drift)
        out.append(
            {
                "timestamp": f"2024-{(i // 28) + 1:02d}-{(i % 28) + 1:02d}T15:30:00+05:30",
                "open": px,
                "high": hi,
                "low": lo,
                "close": px,
                "volume": volume * (1.2 if drift > 0 else 0.8),
                "oi": oi,
            }
        )
    return out


def test_fixture_extreme_greed_near_100():
    # Early choppy / weak, then strong uptrend + long buildup so percentiles + abs lean greed
    early = _synth_candles(100, start=100.0, drift=-0.002, vol_scale=2.5, oi_drift=-200.0)
    late = _synth_candles(
        140,
        start=float(early[-1]["close"]),
        drift=0.006,
        vol_scale=0.25,
        volume=2_500_000.0,
        oi_start=float(early[-1]["oi"]),
        oi_drift=900.0,
    )
    candles = early + late
    bench = _synth_candles(240, start=100.0, drift=0.0, vol_scale=1.0, oi_drift=0.0)
    # Futures rich to spot into the rally
    spot = []
    fut = []
    for i, c in enumerate(candles):
        spot.append({**c, "close": float(c["close"]), "oi": 0})
        prem = 1.0 + (0.0001 * i if i > 100 else -0.0002)
        fut.append({**c, "close": float(c["close"]) * prem})
    result = compute_fear_greed_from_candles(
        candles,
        spot_candles=spot,
        fut_candles=fut,
        benchmark_candles=bench,
    )
    assert result["score"] >= 70, result
    assert result["zone"] in ("Greed", "Extreme Greed")


def test_fixture_extreme_fear_near_0():
    candles = _synth_candles(220, start=200.0, drift=-0.004, vol_scale=3.0, oi_drift=800.0)
    bench = _synth_candles(220, start=100.0, drift=0.001, vol_scale=0.5, oi_drift=0.0)
    result = compute_fear_greed_from_candles(
        candles,
        spot_candles=[dict(c, oi=0) for c in candles],
        fut_candles=candles,
        benchmark_candles=bench,
    )
    assert result["score"] <= 35, result
    assert result["zone"] in ("Fear", "Extreme Fear")


def test_fixture_mid_near_50():
    candles = _synth_candles(220, start=100.0, drift=0.0, vol_scale=1.0, oi_drift=0.0)
    bench = list(candles)
    result = compute_fear_greed_from_candles(
        candles,
        spot_candles=candles,
        fut_candles=candles,
        benchmark_candles=bench,
    )
    # Flat series → percentiles cluster mid; allow a band.
    assert 30 <= result["score"] <= 70, result


def test_missing_data_skips_and_rebalances():
    # Price-only candles (no volume/oi), no spot/fut/benchmark extras
    candles = []
    px = 100.0
    for i in range(120):
        px += 0.1
        candles.append(
            {
                "timestamp": f"2025-01-{(i % 28) + 1:02d}T15:30:00+05:30",
                "open": px,
                "high": px + 1,
                "low": px - 1,
                "close": px,
                "volume": 0,
                "oi": 0,
            }
        )
    result = compute_fear_greed_from_candles(candles)
    assert "volume" in result["skipped"]
    assert "open_interest" in result["skipped"]
    assert "futures_premium" in result["skipped"]
    assert "put_call_ratio" in result["skipped"]
    assert result["weights_used"]
    assert abs(sum(result["weights_used"].values()) - 1.0) < 1e-6
    assert 0 <= result["score"] <= 100


def test_three_instruments_mocked_service(monkeypatch):
    """Run service path for NIFTY / stock / commodity with mocked candles (no live Upstox)."""
    from backend.services.fear_greed import service as fg_service

    fg_service.clear_fear_greed_cache()

    nifty = _synth_candles(260, start=22000.0, drift=0.001, oi_drift=200.0)
    stock = _synth_candles(260, start=2500.0, drift=0.002, oi_drift=100.0)
    cmdty = _synth_candles(260, start=7200.0, drift=-0.001, vol_scale=2.0, oi_drift=50.0)

    def fake_resolve(name, ux=None):
        n = name.upper()
        if "NIFTY" in n:
            return {
                "instrument": "NIFTY",
                "spot_key": "NSE_INDEX|Nifty 50",
                "fut_key": "NSE_FO|NIFTYFUT",
                "futures_contract": "NIFTY24NOVFUT",
                "kind": "index",
            }
        if "RELIANCE" in n:
            return {
                "instrument": "RELIANCE",
                "spot_key": "NSE_EQ|RELIANCE",
                "fut_key": "NSE_FO|RELIANCEFUT",
                "futures_contract": "RELIANCE24NOVFUT",
                "kind": "stock_fut",
            }
        return {
            "instrument": "GOLD",
            "spot_key": "MCX_FO|GOLD",
            "fut_key": "MCX_FO|GOLD",
            "futures_contract": "GOLDDEC24",
            "kind": "commodity",
        }

    def fake_fetch(ux, instrument_key, days_back):
        key = str(instrument_key)
        if "Nifty 50" in key or "NIFTY" in key:
            return list(nifty)
        if "RELIANCE" in key:
            return list(stock)
        if "GOLD" in key or "MCX" in key:
            return list(cmdty)
        if "Bank" in key:
            return list(nifty)
        return list(nifty)

    class DummyUx:
        NIFTY50_KEY = "NSE_INDEX|Nifty 50"
        BANKNIFTY_KEY = "NSE_INDEX|Nifty Bank"

    monkeypatch.setattr(fg_service, "resolve_instrument", fake_resolve)
    monkeypatch.setattr(fg_service, "_fetch_daily", fake_fetch)
    monkeypatch.setattr(fg_service, "_upstox", lambda: DummyUx())

    for sym in ("NIFTY", "RELIANCE", "GOLD"):
        out = fg_service.get_fear_greed(sym, use_cache=False, upstox_service=DummyUx())
        assert out["success"] is True, out
        assert out["score"] is not None
        assert 0 <= int(out["score"]) <= 100
        assert out["zone"] in ("Extreme Fear", "Fear", "Greed", "Extreme Greed")
        assert "disclaimer" in out

"""Rocket Pre-Ignition 10m candle score."""
from backend.services.rocket_pre_ignition import compute_rocket_score, empty_rocket


def _bar(o, h, lo, c, v=1000.0):
    return {"open": o, "high": h, "low": lo, "close": c, "volume": v}


def _coil_base(n=20, start=100.0):
    """Quiet rising coil: slightly higher lows, not at range high."""
    rows = []
    px = start
    for i in range(n):
        o = px
        c = px + 0.15
        rows.append(_bar(o, c + 0.05, o - 0.40, c, v=800 + i * 5))
        px = c
    return rows


def test_insufficient_bars_is_zero():
    assert compute_rocket_score([_bar(1, 2, 0.5, 1.5)] * 5) == empty_rocket()


def test_seller_failure_red_holds_prior_close():
    rows = _coil_base()
    prev_c = rows[-1]["close"]
    # Red bar that still closes >= prior close (sellers failed).
    rows.append(_bar(prev_c + 0.4, prev_c + 0.5, prev_c - 0.2, prev_c + 0.05, v=900))
    out = compute_rocket_score(rows)
    assert "seller_failure" in out["rocket_signals"]
    assert out["rocket_score"] >= 1
    assert out["rocket_label"].startswith("🚀")


def test_seller_failure_lower_wick():
    rows = _coil_base()
    prev_c = rows[-1]["close"]
    # Red bar well below prior close but long lower wick (close in upper half).
    rows.append(_bar(prev_c - 0.1, prev_c, prev_c - 2.0, prev_c - 0.4, v=900))
    out = compute_rocket_score(rows)
    assert "seller_failure" in out["rocket_signals"]


def test_shallower_dips_rising_lows():
    rows = []
    px = 100.0
    for i in range(17):
        rows.append(_bar(px, 102.0, px - 0.5, px + 0.1, v=700))
        px += 0.05
    # Last three lows strictly rising; last high does not break 102.
    rows.append(_bar(101.0, 101.6, 100.20, 101.1, v=700))
    rows.append(_bar(101.1, 101.7, 100.40, 101.2, v=700))
    rows.append(_bar(101.2, 101.8, 100.70, 101.3, v=700))
    out = compute_rocket_score(rows)
    assert "shallower_dips" in out["rocket_signals"]


def test_volume_coil_wakeup():
    rows = _coil_base()
    rows[5]["high"] = 104.0  # range high stays above the wake-up bar
    base = rows[-5]["close"]
    rows[-4] = _bar(base, base + 0.15, base - 0.1, base + 0.05, v=400)
    rows[-3] = _bar(base + 0.05, base + 0.18, base - 0.08, base + 0.08, v=380)
    rows[-2] = _bar(base + 0.08, base + 0.20, base - 0.05, base + 0.10, v=360)
    o = rows[-2]["close"]
    rows[-1] = _bar(o, o + 0.25, o - 0.02, o + 0.22, v=2000)
    out = compute_rocket_score(rows)
    assert "volume_coil_wakeup" in out["rocket_signals"]


def test_cumdelta_lead_price_lags():
    rows = []
    # Green volume builds cum-delta; last bar does not take the 20-bar high.
    high_mark = 120.0
    for i in range(19):
        rows.append(_bar(100 + i * 0.2, high_mark if i == 5 else 104 + i * 0.2, 99.5, 100.5 + i * 0.2, v=2000))
    last_high = max(b["high"] for b in rows)
    rows.append(_bar(108, last_high - 0.8, 107.5, 108.2, v=2500))
    out = compute_rocket_score(rows)
    assert "cumdelta_lead" in out["rocket_signals"]
    assert out["rocket_label"]


def test_anti_chase_drops_late_stage_signals():
    rows = []
    px = 100.0
    for i in range(19):
        rows.append(_bar(px, px + 0.2, px - 0.05, px + 0.15, v=500))
        px += 1.5  # explode higher so last close is far above EMA5
    # Rising lows + volume spike on a fully extended last bar.
    rows.append(_bar(px, px + 8, px - 0.1, px + 7.5, v=5000))
    out = compute_rocket_score(rows)
    assert "shallower_dips" not in out["rocket_signals"]
    assert "volume_coil_wakeup" not in out["rocket_signals"]


def test_zero_score_has_empty_label():
    rows = _coil_base()
    # Force a dull last bar: green but no wick/volume/rising-low edge.
    rows[-1] = _bar(100, 100.1, 99.95, 100.02, v=10)
    out = compute_rocket_score(rows)
    if out["rocket_score"] == 0:
        assert out["rocket_label"] == ""
        assert out["rocket_signals"] == []

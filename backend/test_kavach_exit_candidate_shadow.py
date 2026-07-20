"""Unit tests for high-R full-exit shadow candidates (FEDERALBNK-style)."""
from backend.services.kavach_exit_candidate_fixtures import (
    adanigreen_long_giveback,
    cholafin_round_trip,
    federalbnk_20260720,
    policybzr_giveback,
    tataelxsi_false_stop_20260720,
)
from backend.services.kavach_exit_candidate_shadow import (
    Bar,
    TradeSpec,
    evaluate_bar_snapshot,
    evaluate_baseline_ema_trail,
    evaluate_candidates_on_bars,
    exit_candidate_live_enabled,
    price_r,
)


def test_live_flag_default_off(monkeypatch):
    monkeypatch.delenv("EXIT_CANDIDATE_LIVE", raising=False)
    assert exit_candidate_live_enabled() is False


def test_federalbnk_peak_r():
    trade, bars = federalbnk_20260720()
    # True peak 351.95 → (351.95-350.10)/0.5 = 3.7R
    assert abs(price_r(351.95, entry=350.10, risk_pts=0.5, is_long=True) - 3.7) < 1e-9
    result = evaluate_candidates_on_bars(bars, trade)
    assert abs(result.peak_r - 3.7) < 0.01


def test_federalbnk_baseline_exits_late_near_scratch():
    trade, bars = federalbnk_20260720()
    baseline = evaluate_baseline_ema_trail(bars, trade)
    assert baseline is not None
    assert baseline.reason.startswith("EMA5")
    assert baseline.bar_index == 4  # exit candle, not spike
    assert abs(baseline.exit_r - 0.6) < 0.05  # (350.40-350.10)/0.5


def test_federalbnk_c2_fires_on_spike_candle():
    """Spike touches 3.7R and same candle closes at 1.4R ≤ 1.5 retain → C2."""
    trade, bars = federalbnk_20260720()
    result = evaluate_candidates_on_bars(bars, trade)
    c2 = result.candidates["C2_spike_reverse"]
    assert c2 is not None
    assert c2.bar_index == 3  # spike candle
    assert abs(c2.exit_price - 350.80) < 1e-9
    assert c2.exit_r <= 1.5
    # Retains ~1.4R vs baseline ~0.6R
    assert c2.exit_r > (result.baseline.exit_r if result.baseline else -99)


def test_federalbnk_c1_fires_on_spike_candle():
    trade, bars = federalbnk_20260720()
    result = evaluate_candidates_on_bars(bars, trade)
    c1 = result.candidates["C1_faster_ratchet"]
    assert c1 is not None
    assert c1.bar_index == 3
    assert c1.exit_price == 350.80


def test_federalbnk_c3_fires_on_spike_weaker_close():
    """Spike pierces EMA5 intrabar and closes weaker than prior → C3 on spike."""
    trade, bars = federalbnk_20260720()
    result = evaluate_candidates_on_bars(bars, trade)
    c3 = result.candidates["C3_intrabar_ema5_fast"]
    assert c3 is not None
    assert c3.bar_index == 3
    assert c3.detail.get("pierced") is True
    assert c3.detail.get("weaker_than_prior") is True


def test_policybzr_candidates_reduce_giveback():
    trade, bars = policybzr_giveback()
    result = evaluate_candidates_on_bars(bars, trade)
    assert result.peak_r >= 3.0
    assert result.baseline is not None
    for cid in ("C1_faster_ratchet", "C2_spike_reverse", "C3_intrabar_ema5_fast"):
        ev = result.candidates[cid]
        assert ev is not None, cid
        assert ev.bar_index <= result.baseline.bar_index
        assert ev.exit_r >= result.baseline.exit_r - 0.05


def test_adanigreen_c1_c2_protect_before_ema10_scratch():
    trade, bars = adanigreen_long_giveback()
    result = evaluate_candidates_on_bars(bars, trade)
    assert result.peak_r >= 3.0
    assert result.baseline is not None
    c1 = result.candidates["C1_faster_ratchet"]
    c2 = result.candidates["C2_spike_reverse"]
    assert c1 is not None and c2 is not None
    assert c1.exit_r > result.baseline.exit_r
    assert c2.exit_r > result.baseline.exit_r


def test_tataelxsi_control_candidates_silent_before_baseline():
    """Peak < 2R → candidates must not arm/fire on the pullback stop-out bars."""
    trade, bars = tataelxsi_false_stop_20260720()
    result = evaluate_candidates_on_bars(bars, trade)
    assert result.peak_r < 2.0
    for cid, ev in result.candidates.items():
        assert ev is None, f"{cid} must stay silent when peak < 2R, got {ev}"


def test_cholafin_extreme_round_trip_shadowed():
    trade, bars = cholafin_round_trip()
    result = evaluate_candidates_on_bars(bars, trade)
    assert result.peak_r >= 5.0
    assert any(result.candidates[c] is not None for c in result.candidates)


def test_bar_snapshot_matches_c2_on_spike():
    trade, bars = federalbnk_20260720()
    # Replay through spike with running peak
    peak = 0.0
    peak_ext = None
    touch = None
    prev = None
    snap = None
    for i, bar in enumerate(bars[:4]):
        is_long = True
        snap = evaluate_bar_snapshot(
            is_long=is_long,
            entry=trade.entry,
            risk_pts=trade.risk_pts,
            peak_r_so_far=peak,
            bar=bar,
            prev_close=prev,
            touch_bar_index=touch,
            current_bar_index=i,
            peak_extreme=peak_ext,
        )
        peak = snap["peak_r"]
        peak_ext = snap["peak_extreme"]
        touch = snap["touch_bar_index"]
        prev = bar.close
    assert snap is not None
    assert snap["armed"] is True
    assert "C2_spike_reverse" in snap["would_exit"]
    assert snap["shadow_mode"] is True


def test_replay_result_serializable():
    from backend.services.kavach_exit_candidate_shadow import replay_result_to_dict

    trade, bars = federalbnk_20260720()
    d = replay_result_to_dict(evaluate_candidates_on_bars(bars, trade))
    assert d["trade"]["symbol"] == "FEDERALBNK"
    assert d["candidates"]["C2_spike_reverse"]["bar_index"] == 3

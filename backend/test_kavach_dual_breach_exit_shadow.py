"""Unit tests for Hypothesis D dual-breach shadow (no live gating)."""
from backend.services.kavach_dual_breach_exit_shadow import evaluate_dual_breach


def test_long_dual_breach_when_low_below_both():
    snap = evaluate_dual_breach(
        is_long=True,
        entry=100.0,
        risk_pts=2.0,
        qty=50,
        bar_high=101.0,
        bar_low=97.0,
        bar_close=97.5,
        ema10=98.5,
        vwap=98.0,
        peak_r=2.5,
    )
    assert snap["dual_breach"] is True
    assert snap["hyp_d_would_exit"] is True
    assert snap["ema10_breached"] is True
    assert snap["vwap_breached"] is True
    assert snap["hyp_d_sim_exit_price"] == 97.5
    assert snap["hyp_d_sim_exit_r"] == -1.25  # (97.5-100)/2
    assert snap["hyp_d_sim_exit_pnl_inr"] == -125.0


def test_long_ema10_only_not_dual():
    """UNITDSPR-style: dip below EMA10 but still above VWAP."""
    snap = evaluate_dual_breach(
        is_long=True,
        entry=1424.0,
        risk_pts=4.02,
        qty=400,
        bar_high=1434.8,
        bar_low=1428.8,
        bar_close=1428.8,
        ema10=1430.8902,
        vwap=1427.9774,
        peak_r=4.25,
    )
    assert snap["ema10_breached"] is True
    assert snap["vwap_breached"] is False
    assert snap["dual_breach"] is False
    assert snap["hyp_d_would_exit"] is False
    assert snap["hyp_d_sim_exit_price"] is None


def test_short_ema10_only_not_dual():
    """KALYANKJIL-style: spike above EMA10 but still below VWAP."""
    snap = evaluate_dual_breach(
        is_long=False,
        entry=578.3,
        risk_pts=0.35,
        qty=1350,
        bar_high=578.9,
        bar_low=574.95,
        bar_close=578.0,
        ema10=578.4299,
        vwap=582.6588,
        peak_r=11.86,
    )
    assert snap["ema10_breached"] is True
    assert snap["vwap_breached"] is False
    assert snap["dual_breach"] is False


def test_muthoot_dip_neither_level():
    snap = evaluate_dual_breach(
        is_long=True,
        entry=3076.0,
        risk_pts=7.0,
        qty=275,
        bar_high=3090.0,
        bar_low=3084.2,
        bar_close=3087.3,
        ema10=3080.4035,
        vwap=3060.5357,
        peak_r=2.0,
    )
    assert snap["ema10_breached"] is False
    assert snap["vwap_breached"] is False
    assert snap["dual_breach"] is False


def test_short_dual_breach():
    snap = evaluate_dual_breach(
        is_long=False,
        entry=200.0,
        risk_pts=1.0,
        qty=100,
        bar_high=203.0,
        bar_low=199.0,
        bar_close=202.5,
        ema10=201.0,
        vwap=201.5,
        peak_r=3.0,
    )
    assert snap["dual_breach"] is True
    assert snap["hyp_d_sim_exit_r"] == -2.5  # (200-202.5)/1
    assert snap["hyp_d_sim_exit_pnl_inr"] == -250.0

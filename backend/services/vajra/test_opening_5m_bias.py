"""Tests for Vajra opening 5m bull bias."""
from backend.services.vajra.transition import (
    apply_opening_5m_bias_to_tps,
    opening_session_5m_bull_bias,
    TransitionScores,
)


def _bullish_5m_candles(n=20):
    candles = []
    price = 100.0
    for i in range(n):
        o = price
        c = price + 0.4
        h = c + 0.2
        l = o - 0.1
        candles.append(
            {
                "open": o,
                "high": h,
                "low": l,
                "close": c,
                "volume": 10_000 + i * 500,
            }
        )
        price = c
    return candles


def test_opening_5m_bull_bias_detects_rising_above_vwap():
    assert opening_session_5m_bull_bias(_bullish_5m_candles()) is True


def test_apply_opening_bias_flips_bull_dir():
    tps = TransitionScores(
        tps_bull=40.0,
        tps_bear=45.0,
        pullback_quality=50.0,
        extension_risk=30.0,
        transition_state="SCANNING",
        vwap_reclaim_status="BELOW VWAP",
        ema_reclaim_status="BELOW EMA5",
        rsi_transition_status="RSI 48",
        bull_dir=False,
        trend_pass=False,
        momentum_improving=False,
        market_phase="",
    )
    out = apply_opening_5m_bias_to_tps(tps, bull_bias=True)
    assert out.bull_dir is True
    assert out.tps_bull >= 58.0

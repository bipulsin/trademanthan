"""Unit tests for the Kavach engine and Relative Strength scoring/ranking."""
from backend.services.kavach_engine import (
    RANKING_BEARISH,
    RANKING_BULLISH,
    STATE_BUY,
    STATE_NEUTRAL,
    STATE_SELL,
    STATE_WATCH,
    KavachInput,
    adx_score,
    compute_trade_score,
    evaluate_kavach,
    kavach_score,
    relative_strength_score,
    volume_ratio_score,
    vwap_score,
)


def _bull_input():
    return KavachInput(
        price=105, ema5=104, ema9=103, ema9_slope=0.5, vwap=100,
        supertrend_bullish=True, macd=1.2, macd_signal=0.8, macd_histogram=0.4,
        adx=28, volume_ratio=1.6,
    )


def _bear_input():
    return KavachInput(
        price=95, ema5=96, ema9=97, ema9_slope=-0.5, vwap=100,
        supertrend_bullish=False, macd=-1.2, macd_signal=-0.8, macd_histogram=-0.4,
        adx=28, volume_ratio=1.6,
    )


def test_full_bullish_is_buy():
    res = evaluate_kavach(_bull_input())
    assert res.bullish_count == 10
    assert res.state == STATE_BUY
    assert res.strength == 10


def test_full_bearish_is_sell():
    res = evaluate_kavach(_bear_input())
    assert res.bearish_count == 10
    assert res.state == STATE_SELL


def test_neutral_when_few_conditions():
    inp = KavachInput(
        price=100, ema5=100, ema9=100, ema9_slope=0.0, vwap=100,
        supertrend_bullish=None, macd=0.0, macd_signal=0.0, macd_histogram=0.0,
        adx=10, volume_ratio=0.5,
    )
    res = evaluate_kavach(inp)
    assert res.state == STATE_NEUTRAL


def test_ready_band_five_conditions():
    # 5 bullish conditions (price/EMA/VWAP aligned), no ST/MACD/ADX/volume -> READY.
    inp = KavachInput(
        price=105, ema5=104, ema9=103, ema9_slope=0.5, vwap=100,
        supertrend_bullish=False, macd=-1.0, macd_signal=0.0, macd_histogram=-0.1,
        adx=10, volume_ratio=0.5,
    )
    res = evaluate_kavach(inp)
    assert res.bullish_count == 5
    assert res.state == "READY"


def test_relative_strength_score_bands():
    assert relative_strength_score(1.5, RANKING_BULLISH) == 40
    assert relative_strength_score(0.8, RANKING_BULLISH) == 35
    assert relative_strength_score(0.6, RANKING_BULLISH) == 30
    assert relative_strength_score(0.3, RANKING_BULLISH) == 20
    assert relative_strength_score(0.1, RANKING_BULLISH) == 10
    assert relative_strength_score(-0.5, RANKING_BULLISH) == 0
    # Bearish uses magnitude: very negative RS scores high.
    assert relative_strength_score(-1.5, RANKING_BEARISH) == 40
    assert relative_strength_score(0.5, RANKING_BEARISH) == 0


def test_component_scores():
    assert kavach_score(STATE_BUY) == 30
    assert kavach_score(STATE_WATCH) == 12
    assert kavach_score(STATE_NEUTRAL) == 0
    assert volume_ratio_score(2.5) == 15
    assert volume_ratio_score(1.7) == 12
    assert volume_ratio_score(1.2) == 8
    assert volume_ratio_score(0.9) == 0
    assert adx_score(35) == 10
    assert adx_score(27) == 8
    assert adx_score(22) == 5
    assert adx_score(15) == 0
    assert vwap_score(105, 100, RANKING_BULLISH) == 5
    assert vwap_score(95, 100, RANKING_BULLISH) == 0
    assert vwap_score(95, 100, RANKING_BEARISH) == 5


def test_trade_score_caps_at_100():
    score = compute_trade_score(
        rs=2.0, state=STATE_BUY, volume_ratio=3.0, adx=40, price=105, vwap=100,
        ranking_type=RANKING_BULLISH,
    )
    # 40 + 30 + 15 + 10 + 5 = 100
    assert score == 100


def test_bearish_trade_score_is_meaningful():
    score = compute_trade_score(
        rs=-2.0, state=STATE_SELL, volume_ratio=3.0, adx=40, price=95, vwap=100,
        ranking_type=RANKING_BEARISH,
    )
    assert score == 100

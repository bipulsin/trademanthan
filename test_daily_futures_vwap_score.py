from backend.services.daily_futures_service import _vwap_proximity_score_0_50


def test_vwap_score_ofss_scenario_all_trend_true():
    score = _vwap_proximity_score_0_50(
        1.21, candle_is_green=True, candle_higher_high=True, candle_higher_low=True
    )
    assert score >= 42.0


def test_vwap_score_adanigreen_scenario_all_trend_false():
    score = _vwap_proximity_score_0_50(
        0.9, candle_is_green=False, candle_higher_high=False, candle_higher_low=False
    )
    assert score <= 15.0


def test_vwap_score_sweet_spot():
    score = _vwap_proximity_score_0_50(
        0.5, candle_is_green=False, candle_higher_high=False, candle_higher_low=False
    )
    assert 38.0 <= score <= 50.0


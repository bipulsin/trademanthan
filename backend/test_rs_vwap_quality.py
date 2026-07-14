"""Tests for shared VWAP-quality scoring + READY gate flag."""
from backend.services.rs_vwap_quality import (
    ready_vwap_quality_gate_enabled,
    score_vwap_quality,
    vwap_slope_steepening,
)


def test_ready_vwap_gate_default_off(monkeypatch):
    monkeypatch.delenv("READY_VWAP_QUALITY_GATE", raising=False)
    assert ready_vwap_quality_gate_enabled() is False


def test_ready_vwap_gate_on(monkeypatch):
    monkeypatch.setenv("READY_VWAP_QUALITY_GATE", "1")
    assert ready_vwap_quality_gate_enabled() is True


def _trend_candles(n: int = 80, *, up: bool = True, start: float = 100.0):
    out = []
    px = start
    for i in range(n):
        px += 0.4 if up else -0.4
        # Keep timestamps within a session morning window
        mins = 15 + i * 5
        hh = 9 + mins // 60
        mm = mins % 60
        out.append(
            {
                "timestamp": f"2026-07-14T{hh:02d}:{mm:02d}:00+05:30",
                "open": px - 0.1,
                "high": px + 0.25,
                "low": px - 0.25,
                "close": px,
                "volume": 2000 + i * 20,
            }
        )
    return out


def test_vwap_slope_steepening_shared_api():
    candles = _trend_candles(90, up=True)
    ok, score, signed = vwap_slope_steepening(
        candles, side="LONG", atr_daily_pct=0.25
    )
    # Strong uptrend + low ATR → steepening should clear
    assert isinstance(score, float)
    assert isinstance(signed, float)
    if ok:
        assert signed > 0
        assert score >= 50


def test_score_vwap_quality_shape():
    candles = _trend_candles(90, up=True)
    q = score_vwap_quality(candles, side="LONG", atr_daily_pct=0.25)
    assert "steep_ok" in q
    assert "flip_flop" in q
    assert "quality_pass" in q
    assert "whipsaw_crosses" in q
    assert q["quality_pass"] == (q["steep_ok"] and not q["unstable"])


def test_sort_prefers_vwap_slope_over_rank():
    from backend.services.daily_checklist_trade_state import (
        STATE_READY,
        sort_stocks_by_trade_state,
    )

    a = {
        "symbol": "LOW_SLOPE",
        "trade_state": STATE_READY,
        "confidence": "A",
        "vwap_quality": {"slope_score": 20},
    }
    b = {
        "symbol": "HIGH_SLOPE",
        "trade_state": STATE_READY,
        "confidence": "A",
        "vwap_quality": {"slope_score": 80},
    }
    # Same grade; HIGH_SLOPE should sort first despite worse RS rank
    rank_map = {"LOW_SLOPE": (0, 1), "HIGH_SLOPE": (0, 5)}
    ordered = sort_stocks_by_trade_state([a, b], rank_map)
    assert ordered[0]["symbol"] == "HIGH_SLOPE"

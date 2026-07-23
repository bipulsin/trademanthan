"""Tests for shared VWAP-quality scoring + READY gate flag."""
from backend.services.rs_vwap_quality import (
    ready_vwap_quality_gate_enabled,
    score_vwap_quality,
    vwap_extension_pct,
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


def test_vwap_extension_pct_signed():
    candles = _trend_candles(90, up=True)
    ext = vwap_extension_pct(candles)
    assert ext is not None
    # Strong uptrend → close typically above session VWAP
    assert isinstance(ext, float)

    down = _trend_candles(90, up=False)
    ext_d = vwap_extension_pct(down)
    assert ext_d is not None
    assert ext_d < ext


def test_vwap_extension_abs_pct_is_percent_points():
    from backend.services.rs_vwap_quality import vwap_extension_abs_pct

    candles = _trend_candles(90, up=True)
    signed = vwap_extension_pct(candles)
    abs_pct = vwap_extension_abs_pct(candles)
    assert abs_pct is not None and signed is not None
    assert abs_pct == round(abs(signed) * 100.0, 4)


def test_build_raw_row_minimal():
    from backend.services.kavach_vwap_raw_log import build_raw_row, lock_direction_to_side

    assert lock_direction_to_side("BEAR") == "SHORT"
    assert lock_direction_to_side("BULL") == "LONG"
    row = build_raw_row(
        session_date="2026-07-16",
        symbol="bankindia",
        direction="LONG",
        lock_rank=2,
        lock_direction="BULL",
        slope_score=55.0,
        steep_ok=True,
        vwap_extension_pct=0.012345,
    )
    assert row["symbol"] == "BANKINDIA"
    assert row["steep_ok"] is True
    assert row["vwap_extension_pct"] == 0.012345
    assert "rendered_state" not in row
    assert "quality_pass" not in row


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

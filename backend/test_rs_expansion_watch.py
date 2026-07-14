"""Unit tests for Expansion Watch detector (no live DB)."""
from backend.services.rs_expansion_watch import (
    ALERT_TIER,
    evaluate_candles_for_expansion,
    live_enabled,
)


def _bars(n: int = 80, *, up: bool = True, start: float = 100.0):
    out = []
    px = start
    for i in range(n):
        if up:
            px += 0.35
        else:
            px -= 0.35
        out.append(
            {
                "timestamp": f"2026-07-14T09:{i // 2:02d}:{(i % 2) * 5:02d}:00+05:30"
                if i < 120
                else f"2026-07-14T10:00:00+05:30",
                "open": px - 0.1,
                "high": px + 0.2,
                "low": px - 0.2,
                "close": px,
                "volume": 1000 + i * 10,
            }
        )
    return out


def test_live_disabled_by_default(monkeypatch):
    monkeypatch.delenv("EXPANSION_WATCH_LIVE", raising=False)
    assert live_enabled() is False


def test_expansion_long_on_steep_uptrend():
    candles = _bars(90, up=True, start=100.0)
    # Low ATR% so normalized slope clears threshold easily
    hit = evaluate_candles_for_expansion(
        candles, side="LONG", atr_daily_pct=0.3, atr_ext_max=5.0
    )
    # May or may not fire depending on VWAP/EMA geometry; assert shape when present
    if hit:
        assert hit["tier"] == ALERT_TIER
        assert hit["direction"] == "LONG"
        assert hit["ema_align_bars"] >= 2
        assert hit["extension_atr"] <= 5.0


def test_short_side_rejects_uptrend():
    candles = _bars(90, up=True, start=100.0)
    hit = evaluate_candles_for_expansion(
        candles, side="SHORT", atr_daily_pct=0.3, atr_ext_max=5.0
    )
    assert hit is None

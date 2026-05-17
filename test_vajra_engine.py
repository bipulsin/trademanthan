"""Unit tests for Vajra rating engine."""
from backend.services.vajra.engine import compute_vajra_rating, sort_vajra_rows, TRADE_TYPE_SORT_ORDER


def _synthetic_candles(n: int = 120, trend: float = 1.0) -> list:
    candles = []
    price = 100.0
    for i in range(n):
        o = price
        c = price + trend * 0.5
        h = max(o, c) + 0.3
        l = min(o, c) - 0.3
        v = 10000 + i * 100
        candles.append(
            {
                "timestamp": f"2026-05-01T{10 + i // 4:02d}:{(i % 4) * 15:02d}:00+05:30",
                "open": o,
                "high": h,
                "low": l,
                "close": c,
                "volume": v,
            }
        )
        price = c
    return candles


def test_compute_vajra_rating_returns_rating():
    m15 = _synthetic_candles(120, trend=0.8)
    m60 = _synthetic_candles(80, trend=0.5)
    rating = compute_vajra_rating(m15, m60)
    assert rating is not None
    assert 0 <= rating.confidence <= 100
    assert rating.trade_type in TRADE_TYPE_SORT_ORDER


def test_sort_vajra_rows_by_trade_type_then_confidence():
    rows = [
        {"trade_type": "REJECT", "confidence": 90, "security": "A"},
        {"trade_type": "LONG", "confidence": 80, "security": "B"},
        {"trade_type": "LONG  [A+]", "confidence": 70, "security": "C"},
    ]
    sorted_rows = sort_vajra_rows(rows)
    assert sorted_rows[0]["trade_type"] == "LONG  [A+]"
    assert sorted_rows[1]["trade_type"] == "LONG"

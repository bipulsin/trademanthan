"""Unit tests for checklist trade-state columns (READY / WAIT / EXPIRED / BLOCKED)."""
from backend.services.daily_checklist_trade_state import (
    STATE_BLOCKED,
    STATE_EXPIRED,
    STATE_READY,
    STATE_READY_RECHECK,
    STATE_WAIT,
    compute_trade_state_for_stock,
    sort_stocks_by_trade_state,
)

CFG = {"convergence_atr": 0.35, "expiry_atr": 1.5}


def _compute(levels=None, stock=None, atr_pct=2.0, lot=50, session_hi=106.0, session_lo=94.0,
             open_pos=None, promo=None):
    base_stock = {"symbol": "TEST", "direction": "LONG", "confidence": "B"}
    if stock:
        base_stock.update(stock)
    base_levels = {
        "price": 100.0,
        "ema5": 100.0,
        "ema10": 98.0,
        "vwap": 99.5,
        "adx": 28.0,
        "confidence_grade": "B",
        "market_regime": "TREND",
        "source": "test",
    }
    if levels:
        base_levels.update(levels)
    return compute_trade_state_for_stock(
        base_stock,
        levels=base_levels,
        atr_pct=atr_pct,
        lot=lot,
        session_hi=session_hi,
        session_lo=session_lo,
        open_pos=open_pos,
        promo=promo,
        cfg=CFG,
    )


def test_ready_at_ema5_defined_risk():
    out = _compute()
    assert out["trade_state"] == STATE_READY
    assert out["trade_entry"] == 100.0
    assert out["trade_sl"] == 98.0
    assert out["trade_risk_inr"] == 100
    assert out["trade_rr_label"] == "1:3.0"
    assert out["trade_rr_low"] is False


def test_ready_recheck_adx_band():
    out = _compute(levels={"adx": 22.0})
    assert out["trade_state"] == STATE_READY_RECHECK
    assert out["trade_adx"] == 22.0
    assert out["trade_entry"] == 100.0


def test_wait_extended_not_expired():
    # ATR=2 → near=0.7, expiry=3. price 101.5 → dist 0.75 ATR → WAIT
    out = _compute(levels={"price": 101.5, "ema5": 100.0})
    assert out["trade_state"] == STATE_WAIT
    assert out["trade_entry"] is not None


def test_expired_past_1_5_atr():
    # ATR=2 → expiry=3. price 104 → dist 2 ATR → EXPIRED
    out = _compute(levels={"price": 104.0, "ema5": 100.0})
    assert out["trade_state"] == STATE_EXPIRED
    assert out["trade_entry"] is None


def test_blocked_d_grade():
    out = _compute(levels={"confidence_grade": "D", "adx": 30.0})
    assert out["trade_state"] == STATE_BLOCKED
    assert "conf D" in (out["trade_state_reason"] or "")
    assert out["trade_entry"] is None


def test_blocked_flat_regime():
    out = _compute(levels={"market_regime": "FLAT"})
    assert out["trade_state"] == STATE_BLOCKED
    assert "regime" in (out["trade_state_reason"] or "").lower()


def test_blocked_risk_over_3k():
    out = _compute(levels={"ema10": 60.0}, lot=100)
    assert out["trade_state"] == STATE_BLOCKED
    assert "risk" in (out["trade_state_reason"] or "").lower()


def test_rr_low_badge_not_block():
    out = _compute(session_hi=101.0)
    assert out["trade_state"] == STATE_READY
    assert out["trade_rr_low"] is True
    assert out["trade_rr_label"] == "1:0.5"


def test_short_symmetric():
    out = _compute(
        stock={"symbol": "SHORT1", "direction": "SHORT", "confidence": "A"},
        levels={
            "price": 100.0,
            "ema5": 100.0,
            "ema10": 102.0,
            "vwap": 100.5,
            "adx": 30.0,
            "confidence_grade": "A",
            "market_regime": "TREND",
        },
    )
    assert out["trade_state"] == STATE_READY
    assert out["trade_entry"] == 100.0
    assert out["trade_sl"] == 102.0
    assert out["trade_risk_inr"] == 100
    assert out["trade_rr"] == 3.0


def test_sort_order():
    stocks = [
        {"symbol": "B", "trade_state": STATE_BLOCKED, "confidence": "A", "rs_pct": 5},
        {"symbol": "R", "trade_state": STATE_READY, "confidence": "B", "rs_pct": 1},
        {"symbol": "W", "trade_state": STATE_WAIT, "confidence": "A", "rs_pct": 3},
        {"symbol": "C", "trade_state": STATE_READY_RECHECK, "confidence": "A", "rs_pct": 2},
    ]
    ordered = sort_stocks_by_trade_state(stocks)
    assert [s["symbol"] for s in ordered] == ["R", "C", "W", "B"]


def test_position_book_now_beyond_ema10():
    out = _compute(
        levels={"price": 97.0},
        open_pos={"direction": "LONG", "entry_price": 100.0, "lot_size": 50},
    )
    assert out["position"]["trail_state"] == "BOOK-NOW"
    assert out["position"]["trail_reason"] == "EMA10 close"

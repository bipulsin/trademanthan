"""Tests for chop-day / whipsaw / re-entry downgrade gates."""
from backend.services.daily_checklist_chop_gates import apply_state_downgrades
from backend.services.daily_checklist_trade_state import (
    STATE_BLOCKED,
    STATE_EXPIRED,
    STATE_READY,
    STATE_READY_RECHECK,
    STATE_WAIT,
    compute_trade_state_for_stock,
)

CFG = {"convergence_atr": 0.35, "expiry_atr": 1.5}


def test_chop_tiers_ready_to_wait():
    st, reason, badges = apply_state_downgrades(
        state=STATE_READY,
        market_regime="CHOP",
        direction_unstable=False,
        unstable_reason=None,
        whipsaw_count=0,
        pullback_count=0,
        stopped=None,
    )
    assert st == STATE_WAIT
    assert "CHOP" in (reason or "")
    assert "CHOP DAY" in badges


def test_chop_tiers_wait_to_expired():
    st, _, _ = apply_state_downgrades(
        state=STATE_WAIT,
        market_regime="CHOP",
        direction_unstable=False,
        unstable_reason=None,
        whipsaw_count=0,
        pullback_count=0,
        stopped=None,
    )
    assert st == STATE_EXPIRED


def test_direction_unstable_blocks():
    st, reason, badges = apply_state_downgrades(
        state=STATE_READY,
        market_regime="TREND",
        direction_unstable=True,
        unstable_reason="lock BULL↔BEAR same day",
        whipsaw_count=0,
        pullback_count=0,
        stopped=None,
    )
    assert st == STATE_BLOCKED
    assert "DIRECTION UNSTABLE" in (reason or "")
    assert "DIRECTION UNSTABLE" in badges


def test_stopped_out_hard_blocks_even_opposite_side():
    st, reason, badges = apply_state_downgrades(
        state=STATE_READY,
        market_regime="TREND",
        direction_unstable=False,
        unstable_reason=None,
        whipsaw_count=0,
        pullback_count=0,
        stopped={
            "blocked": True,
            "label": "SL hit earlier today · no re-entry regardless of direction",
        },
    )
    assert st == STATE_BLOCKED
    assert "SL hit earlier today" in (reason or "")
    assert "RE-ENTRY BLOCKED" in badges


def test_whipsaw_caps_at_wait():
    st, reason, badges = apply_state_downgrades(
        state=STATE_READY,
        market_regime="TREND",
        direction_unstable=False,
        unstable_reason=None,
        whipsaw_count=2,
        pullback_count=0,
        stopped=None,
    )
    assert st == STATE_WAIT
    assert any("WHIPSAW" in b for b in badges)


def test_third_pullback_downgrades_ready():
    st, reason, badges = apply_state_downgrades(
        state=STATE_READY_RECHECK,
        market_regime="TREND",
        direction_unstable=False,
        unstable_reason=None,
        whipsaw_count=0,
        pullback_count=3,
        stopped=None,
    )
    assert st == STATE_WAIT
    assert "EXTENDED" in (reason or "")


def test_adanigreen_style_flip_plus_stop():
    """After SL, opposite-side READY must hard-block (Part 4 + Part 2)."""
    out = compute_trade_state_for_stock(
        {"symbol": "ADANIGREEN", "direction": "SHORT", "confidence": "A"},
        levels={
            "price": 100.0,
            "ema5": 100.0,
            "ema10": 102.0,
            "vwap": 100.2,
            "adx": 30.0,
            "confidence_grade": "A",
            "market_regime": "TREND",
        },
        atr_pct=2.0,
        lot=50,
        session_hi=110.0,
        session_lo=90.0,
        open_pos=None,
        promo=None,
        cfg=CFG,
        market_regime_idx="CHOP",
        direction_unstable=True,
        unstable_reason="flipped from morning BULL to BEAR",
        whipsaw_count=1,
        pullback_count=1,
        stopped={
            "blocked": True,
            "label": "SL hit earlier today · no re-entry regardless of direction",
        },
    )
    assert out["trade_state"] == STATE_BLOCKED
    assert "SL hit" in (out["trade_state_reason"] or "")
    assert out["stopped_out_today"] is True


def test_persistent_style_whipsaw_under_chop():
    out = compute_trade_state_for_stock(
        {"symbol": "PERSISTENT", "direction": "LONG", "confidence": "B"},
        levels={
            "price": 100.0,
            "ema5": 100.0,
            "ema10": 98.0,
            "vwap": 99.5,
            "adx": 28.0,
            "confidence_grade": "B",
            "market_regime": "TREND",
        },
        atr_pct=2.0,
        lot=50,
        session_hi=106.0,
        session_lo=94.0,
        open_pos=None,
        promo=None,
        cfg=CFG,
        market_regime_idx="CHOP",
        direction_unstable=False,
        unstable_reason=None,
        whipsaw_count=3,
        pullback_count=1,
        stopped=None,
    )
    # READY → WAIT (whip) → EXPIRED (chop tier-down of WAIT)
    assert out["trade_state"] == STATE_EXPIRED
    assert any("WHIPSAW" in b for b in (out.get("gate_badges") or []))


def test_profit_locked_shows_ema5_alt():
    out = compute_trade_state_for_stock(
        {"symbol": "X", "direction": "LONG", "confidence": "A"},
        levels={
            "price": 110.0,
            "ema5": 109.0,
            "ema10": 98.0,
            "vwap": 105.0,
            "adx": 30.0,
            "confidence_grade": "A",
            "market_regime": "TREND",
        },
        atr_pct=2.0,
        lot=50,
        session_hi=120.0,
        session_lo=90.0,
        open_pos={
            "direction": "LONG",
            "entry_price": 100.0,
            "lot_size": 50,
            "peak_unrealized_pnl_rupees": 6000,
        },
        promo=None,
        cfg=CFG,
        market_regime_idx="TREND",
    )
    assert out["position"]["profit_locked"] is True
    assert out["position"]["alt_exit_ema5"] == 109.0
    assert out["position"]["trail_state"] in ("PROFIT LOCKED", "BOOK-NOW")

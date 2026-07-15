"""Unit tests for checklist trade-state columns (READY / WAIT / EXPIRED / BLOCKED)."""
from datetime import datetime

import pytz

from backend.services.daily_checklist_trade_state import (
    STATE_BLOCKED,
    STATE_EXPIRED,
    STATE_READY,
    STATE_READY_RECHECK,
    STATE_SCANNING,
    STATE_WAIT,
    compute_atr_consumed_metrics,
    compute_trade_state_for_stock,
    direction_live_conflict,
    entry_off_live_ema5,
    entry_outside_session_range,
    sort_stocks_by_trade_state,
)

CFG = {"convergence_atr": 0.35, "expiry_atr": 1.5}
IST = pytz.timezone("Asia/Kolkata")
# Pin after 09:45 so READY tests are not forced into SCANNING.
AFTER_ENTRY = IST.localize(datetime(2026, 7, 15, 10, 0))
BEFORE_ENTRY = IST.localize(datetime(2026, 7, 15, 9, 33))


def _compute(levels=None, stock=None, atr_pct=2.0, lot=50, session_hi=106.0, session_lo=94.0,
             open_pos=None, promo=None, now=None,
             session_open=None, opening_candle_high=None, opening_candle_low=None):
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
        now=now or AFTER_ENTRY,
        session_open=session_open,
        opening_candle_high=opening_candle_high,
        opening_candle_low=opening_candle_low,
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
    assert out["trade_expiry_crossed"] is True
    assert out["trade_expiry_price"] == 103.12  # 100 + 1.5 * (104 * 0.02)
    assert out["trade_entry"] == 100.0  # intended entry kept for display


def test_blocked_d_grade():
    out = _compute(levels={"confidence_grade": "D", "adx": 30.0})
    assert out["trade_state"] == STATE_BLOCKED
    assert "conf D" in (out["trade_state_reason"] or "")
    assert out["trade_entry"] is None


def test_blocked_flat_regime():
    out = _compute(levels={"market_regime": "FLAT"})
    assert out["trade_state"] == STATE_BLOCKED
    assert "regime" in (out["trade_state_reason"] or "").lower()


def test_blocked_risk_over_3k_hard_gate():
    """Risk > ₹3k with R:R < 1:2 → BLOCKED (hard), Take Trade off."""
    out = _compute(levels={"ema10": 60.0}, lot=100)
    assert out["trade_state"] == STATE_BLOCKED
    assert "risk" in (out["trade_state_reason"] or "").lower()
    assert out["trade_take_enabled"] is False
    assert out["trade_risk_inr"] and out["trade_risk_inr"] > 3000


def test_ready_includes_expiry_price():
    out = _compute()
    assert out["trade_state"] == STATE_READY
    # atr_pct=2 → ATR=2 at price 100; expiry = 100 + 1.5*2 = 103
    assert out["trade_expiry_price"] == 103.0


def test_rr_low_badge_not_block():
    out = _compute(session_hi=101.0)
    assert out["trade_state"] == STATE_READY
    assert out["trade_rr_low"] is True
    assert out["trade_rr_label"] == "1:0.5"


def test_risk_cap_waived_when_rr_high():
    # Large session high → high RR; risk over but R:R waiver keeps READY
    out = _compute(levels={"ema10": 60.0}, lot=100, session_hi=200.0)
    assert out["trade_state"] == STATE_READY
    assert out["trade_risk_over"] is True
    assert out["trade_rr"] is not None and out["trade_rr"] >= 2
    assert out["trade_risk_cap_waived"] is True
    assert out["trade_risk_cap_flag"] is False
    assert out["trade_risk_cap_waiver_label"]
    assert "cap waived" in out["trade_risk_cap_waiver_label"]
    assert out["trade_take_enabled"] is True


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


def test_entry_outside_session_range_helper():
    assert entry_outside_session_range(
        is_long=True, entry=7175.0, session_hi=7300.0, session_lo=7229.0
    )
    assert not entry_outside_session_range(
        is_long=True, entry=7250.0, session_hi=7300.0, session_lo=7229.0
    )
    assert entry_outside_session_range(
        is_long=False, entry=120.0, session_hi=115.0, session_lo=110.0
    )


def test_divislab_style_stale_entry_expired():
    """LONG READY with entry below today's low → EXPIRED (untouchable)."""
    out = _compute(
        levels={"price": 7250.0, "ema5": 7175.0, "ema10": 7160.0, "vwap": 7200.0, "adx": 28.0},
        session_hi=7300.0,
        session_lo=7229.0,
        atr_pct=1.0,  # near EMA5: |7250-7175|/(7250*0.01)=10.3 ATR → WAIT/EXPIRED by distance
    )
    # Force near-EMA5 READY path: price near ema5 but session_lo still above entry
    out = _compute(
        levels={"price": 7176.0, "ema5": 7175.98, "ema10": 7160.0, "vwap": 7200.0, "adx": 28.0},
        session_hi=7300.0,
        session_lo=7229.0,
        atr_pct=2.0,
    )
    assert out["trade_state"] == STATE_EXPIRED
    assert "below today's low" in (out["trade_state_reason"] or "")
    assert out["trade_take_enabled"] is False


def test_no_ready_before_0945_scanning():
    out = _compute(now=BEFORE_ENTRY)
    assert out["trade_state"] == STATE_SCANNING
    assert "09:45" in (out["trade_state_reason"] or "")
    assert out["trade_take_enabled"] is False
    assert out["trade_entry_window_open"] is False


def test_ready_after_0945_take_enabled():
    out = _compute(now=AFTER_ENTRY)
    assert out["trade_state"] == STATE_READY
    assert out["trade_take_enabled"] is True
    assert out["trade_entry_window_open"] is True


def test_wait_entry_is_ema5_not_vwap():
    """Pullback entry must be EMA5, never a VWAP blend."""
    out = _compute(levels={"price": 101.5, "ema5": 100.0, "vwap": 101.2})
    assert out["trade_state"] == STATE_WAIT
    assert out["trade_entry"] == 100.0


def test_entry_off_live_ema5_helper():
    # CHOLAFIN-style: entry well below live EMA5 under a tight band
    assert entry_off_live_ema5(1818.55, 1820.78, tol_pct=0.05)
    assert not entry_off_live_ema5(1820.78, 1820.78)
    assert entry_off_live_ema5(None, 100.0)
    assert entry_off_live_ema5(100.0, None)


def test_no_ready_without_sl_or_risk():
    out = _compute(levels={"ema10": None})
    assert out["trade_state"] == STATE_WAIT
    assert "SL/Risk" in (out["trade_state_reason"] or "")
    assert out["trade_sl"] is None
    assert out["trade_take_enabled"] is False


def test_ready_ema5_anchored_within_cap():
    out = _compute()
    assert out["trade_state"] == STATE_READY
    assert out["trade_entry"] == 100.0
    assert out["trade_sl"] == 98.0
    assert out["trade_risk_inr"] == 100
    assert out["trade_take_enabled"] is True
    assert out["trade_risk_cap_waived"] is False


def test_direction_live_conflict_helper():
    # HCLTECH-style: SHORT lock vs live bullish Trend+ST+MACD
    c = direction_live_conflict(
        direction="SHORT",
        ema_vs_vwap="Above",
        supertrend="Bullish",
        macd="Bullish",
    )
    assert c["conflict_count"] == 3
    assert c["suppress_ready"] is True
    assert c["live_lean"] == "Bullish"
    assert "checklist SHORT" in (c["reason"] or "")

    # MANKIND-style: LONG lock vs live bearish
    c2 = direction_live_conflict(
        direction="LONG",
        ema_vs_vwap="Below",
        supertrend="Bearish",
        macd="Bearish",
    )
    assert c2["conflict_count"] == 3
    assert c2["suppress_ready"] is True
    assert c2["live_lean"] == "Bearish"

    # Clean LONG aligned
    c3 = direction_live_conflict(
        direction="LONG",
        ema_vs_vwap="Above",
        supertrend="Bullish",
        macd="Bullish",
    )
    assert c3["conflict_count"] == 0
    assert c3["suppress_ready"] is False


def test_hcltech_style_short_vs_live_bullish_suppresses_ready():
    out = _compute(
        stock={
            "symbol": "HCLTECH",
            "direction": "SHORT",
            "confidence": "A",
            "ema_vs_vwap": "Above",
            "supertrend": "Bullish",
            "macd": "Bullish",
        },
        levels={
            "price": 1148.0,
            "ema5": 1147.91,
            "ema10": 1155.0,
            "vwap": 1146.0,
            "adx": 28.0,
            "confidence_grade": "A",
            "market_regime": "TREND",
        },
        session_hi=1160.0,
        session_lo=1140.0,
    )
    assert out["trade_state"] == STATE_WAIT
    assert out["trade_take_enabled"] is False
    assert "DIR CONFLICT" in (out["gate_badges"] or [])
    assert out["dir_conflict"]["suppress_ready"] is True
    assert "direction conflict" in (out["trade_state_reason"] or "")


def test_mankind_style_long_vs_live_bearish_suppresses_ready():
    out = _compute(
        stock={
            "symbol": "MANKIND",
            "direction": "LONG",
            "confidence": "A",
            "ema_vs_vwap": "Below",
            "supertrend": "Bearish",
            "macd": "Bearish",
        },
        levels={
            "price": 2615.5,
            "ema5": 2624.77,
            "ema10": 2630.0,
            "vwap": 2620.0,
            "adx": 28.0,
            "confidence_grade": "A",
            "market_regime": "TREND",
        },
        atr_pct=2.0,
        session_hi=2650.0,
        session_lo=2600.0,
    )
    # price near ema5 → would have been READY without conflict gate
    assert out["trade_state"] == STATE_WAIT
    assert "DIR CONFLICT" in (out["gate_badges"] or [])
    assert out["dir_conflict"]["live_lean"] == "Bearish"
    assert out["trade_take_enabled"] is False


def test_aligned_live_momentum_still_ready():
    out = _compute(
        stock={
            "ema_vs_vwap": "Above",
            "supertrend": "Bullish",
            "macd": "Bullish",
        }
    )
    assert out["trade_state"] == STATE_READY
    assert out["trade_take_enabled"] is True
    assert "DIR CONFLICT" not in (out["gate_badges"] or [])
    assert out["dir_conflict"]["conflict_count"] == 0


def test_one_of_three_conflict_flags_but_stays_ready():
    """Soft visibility: single opposing field badges but does not suppress READY."""
    out = _compute(
        stock={
            "ema_vs_vwap": "Above",
            "supertrend": "Bullish",
            "macd": "Bearish",  # 1 opposing
        }
    )
    assert out["trade_state"] == STATE_READY
    assert out["dir_conflict"]["conflict_count"] == 1
    assert out["dir_conflict"]["suppress_ready"] is False
    assert "DIR CONFLICT" in (out["gate_badges"] or [])
    assert out["trade_take_enabled"] is True


def test_overlay_live_momentum_empty_candles_keeps_prior():
    from backend.services.daily_checklist_trade_state import overlay_live_momentum_from_candles

    stock = {
        "symbol": "IEX",
        "direction": "LONG",
        "ema_vs_vwap": "Above",
        "supertrend": "Bullish",
        "macd": "Bearish",
    }
    out = overlay_live_momentum_from_candles(stock, [], nifty_pct=0.0)
    assert out["ema_vs_vwap"] == "Above"
    assert stock["macd"] == "Bearish"


def test_atr_consumed_metrics_from_open_and_opening_range():
    # daily ATR = 20; open 100 → price 114 = 70% from open
    m = compute_atr_consumed_metrics(
        price=114.0,
        atr=20.0,
        atr_pct=2.0,
        session_open=100.0,
        opening_candle_high=102.0,
        opening_candle_low=99.0,
        is_long=True,
    )
    assert m["atr_consumed_pct_from_open"] == 70.0
    assert m["move_from_open"] == 14.0
    # LONG uses opening high 102 → |114-102|/20 = 60%
    assert m["atr_consumed_pct_from_opening_range"] == 60.0
    assert m["opening_range_ref"] == "opening_high"

    m_short = compute_atr_consumed_metrics(
        price=90.0,
        atr=20.0,
        session_open=100.0,
        opening_candle_high=102.0,
        opening_candle_low=98.0,
        is_long=False,
    )
    assert m_short["atr_consumed_pct_from_open"] == 50.0
    assert m_short["atr_consumed_pct_from_opening_range"] == 40.0  # |90-98|/20
    assert m_short["opening_range_ref"] == "opening_low"


def test_atr_consumed_logged_on_ready_no_gating():
    """ATR chip is informational — does not change READY / Take Trade."""
    out = _compute(
        session_open=98.0,
        opening_candle_high=99.0,
        opening_candle_low=97.5,
    )
    assert out["trade_state"] == STATE_READY
    assert out["trade_take_enabled"] is True
    ac = out["atr_consumed"]
    assert ac["atr_consumed_pct_from_open"] is not None
    assert any(str(b).startswith("ATR ") for b in (out["gate_badges"] or []))

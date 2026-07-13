"""State-machine unit tests for Kavach open-trade transitions (ADANIGREEN-style)."""
from backend.services.kavach_open_trades import (
    STATE_EXIT_NOW,
    STATE_PROFIT_LOCKED,
    STATE_TRAILING,
)
from backend.services.daily_checklist_trade_state import MAX_INR_RISK, RR_LOW


def _next_state(*, state, is_long, close, ema5, ema10, rr, qty, risk_cap=MAX_INR_RISK):
    """Mirror evaluate_open_trades transition rules (pure)."""
    new_state = state
    trigger = None
    if state == STATE_EXIT_NOW:
        return state, None
    if state == STATE_TRAILING:
        if ema10 is not None and rr < RR_LOW:
            if abs(close - ema10) * qty > risk_cap:
                return STATE_EXIT_NOW, "Risk cap exceeded before 1:2"
        if ema10 is not None:
            beyond = (close < ema10) if is_long else (close > ema10)
            if beyond:
                return STATE_EXIT_NOW, "EMA10 reverse close"
        if rr >= RR_LOW:
            return STATE_PROFIT_LOCKED, None
    elif state == STATE_PROFIT_LOCKED:
        if ema5 is not None:
            beyond = (close < ema5) if is_long else (close > ema5)
            if beyond:
                return STATE_EXIT_NOW, "EMA5 reverse close after profit protection"
    return new_state, trigger


def test_adanigreen_trail_to_profit_lock():
    # entry 1564.5, risk ~ few pts; strong move → rr >= 2
    st, reason = _next_state(
        state=STATE_TRAILING, is_long=True, close=1580.0, ema5=1575.0, ema10=1568.0,
        rr=2.1, qty=650,
    )
    assert st == STATE_PROFIT_LOCKED
    assert reason is None


def test_adanigreen_profit_lock_to_exit_ema5():
    st, reason = _next_state(
        state=STATE_PROFIT_LOCKED, is_long=True, close=1560.0, ema5=1565.0, ema10=1558.0,
        rr=0.5, qty=650,
    )
    assert st == STATE_EXIT_NOW
    assert "EMA5" in reason


def test_trailing_ema10_exit():
    st, reason = _next_state(
        state=STATE_TRAILING, is_long=True, close=1550.0, ema5=1555.0, ema10=1555.0,
        rr=0.2, qty=50,  # 5 pts * 50 = 250 < ₹3k risk cap
    )
    assert st == STATE_EXIT_NOW
    assert "EMA10" in reason


def test_risk_cap_before_12():
    # |close-ema10|*qty > 3000, rr < 2
    st, reason = _next_state(
        state=STATE_TRAILING, is_long=True, close=1570.0, ema5=1568.0, ema10=1560.0,
        rr=0.5, qty=400,  # 10 pts * 400 = 4000
    )
    assert st == STATE_EXIT_NOW
    assert "Risk cap" in reason


def test_no_reentry_after_exit_state_sticky():
    st, reason = _next_state(
        state=STATE_EXIT_NOW, is_long=True, close=1600.0, ema5=1590.0, ema10=1580.0,
        rr=3.0, qty=650,
    )
    assert st == STATE_EXIT_NOW
    assert reason is None

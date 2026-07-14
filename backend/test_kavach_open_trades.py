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


def test_lock_removal_reason_format():
    from datetime import datetime
    import pytz
    from backend.services.kavach_open_trades import format_lock_removal_exit_reason

    ist = pytz.timezone("Asia/Kolkata")
    at = ist.localize(datetime(2026, 7, 14, 10, 25))
    reason = format_lock_removal_exit_reason("R2", at)
    assert reason == "Lock removed via R2 at 10:25 — setup no longer qualified"
    assert "R1" in format_lock_removal_exit_reason("R1", at)


def test_canonical_lock_removal_exit_reason():
    from backend.services.kavach_open_trades import canonical_exit_reason

    raw = "Lock removed via R2 at 11:15 — setup no longer qualified"
    assert canonical_exit_reason(raw) == "Lock removed via R2"
    assert canonical_exit_reason("Lock removed via R1 at 10:15 — setup no longer qualified") == (
        "Lock removed via R1"
    )


def test_mark_open_trades_exit_on_lock_removal():
    """MANAPPURAM-style: open SHORT → R2 removal → EXIT_NOW + alarm."""
    from datetime import datetime
    from types import SimpleNamespace
    from unittest.mock import MagicMock, patch
    import pytz
    from backend.services import kavach_open_trades as ot

    ist = pytz.timezone("Asia/Kolkata")
    removed_at = ist.localize(datetime(2026, 7, 14, 10, 25))
    row = SimpleNamespace(
        id="t-mana",
        state=ot.STATE_TRAILING,
        direction="SHORT",
        provenance=None,
        state_context_snapshot=None,
    )
    db = MagicMock()
    db.execute.side_effect = [
        # SELECT open trades
        MagicMock(fetchall=lambda: [row]),
        # UPDATE
        MagicMock(),
    ]
    ctx = {
        "rule": "R2",
        "label": "R2 rank-based removal — price has NOT closed beyond EMA10",
        "rank_trail": "4→8→out",
        "entry_rank": 4,
        "removal_rank": None,
        "last_3_ranks": [],
        "price_closed_beyond_ema10": False,
        "price_closed_beyond_vwap": False,
    }
    trade_dict = {
        "id": "t-mana",
        "state": ot.STATE_TRAILING,
        "direction": "SHORT",
        "provenance": None,
        "state_context_snapshot": {},
    }
    with patch.object(ot, "ensure_tables"), patch.object(
        ot, "_row_to_dict", return_value=trade_dict
    ), patch.object(ot, "build_lock_removal_context", return_value=ctx), patch.object(
        ot, "log_r2_exit_now"
    ) as log_mock:
        ids = ot.mark_open_trades_exit_on_lock_removal(
            db, "2026-07-14", "MANAPPURAM", "R2", removed_at=removed_at
        )
    assert ids == ["t-mana"]
    update_call = db.execute.call_args_list[1]
    params = update_call[0][1]
    assert params["st"] == ot.STATE_EXIT_NOW
    assert "Lock removed via R2 at 10:25" in params["tr"]
    assert "R2 rank-based removal" in params["tr"]
    assert params["alarm"] == removed_at
    log_mock.assert_called_once()


def test_evaluate_prioritizes_lock_removal_over_ema():
    """Even with a healthy EMA10 trail, R2 removal forces EXIT_NOW."""
    from datetime import datetime
    from types import SimpleNamespace
    from unittest.mock import MagicMock, patch
    import pytz
    from backend.services import kavach_open_trades as ot

    ist = pytz.timezone("Asia/Kolkata")
    entry = ist.localize(datetime(2026, 7, 14, 10, 15))
    rem_at = ist.localize(datetime(2026, 7, 14, 10, 25))
    trade = {
        "id": "t1",
        "symbol": "TIINDIA",
        "direction": "SHORT",
        "state": ot.STATE_TRAILING,
        "entry_price": 2839.0,
        "entry_qty": 200,
        "initial_sl_inr": 1000,
        "current_sl_price": 2845.0,
        "highest_rr_reached": 0,
        "exit_trigger_reason": None,
        "exit_trigger_price": None,
        "alarm_fired_at": None,
        "last_eval_bar_at": None,
        "entry_time": entry.isoformat(),
        "created_at": entry.isoformat(),
        "state_context_snapshot": {},
    }
    db = MagicMock()
    db.execute.side_effect = [
        MagicMock(fetchall=lambda: [SimpleNamespace(**trade)]),  # open rows
        MagicMock(),  # UPDATE
    ]
    ctx = {
        "rule": "R2",
        "label": "R2 rank-based removal — price has NOT closed beyond EMA10",
        "rank_trail": "3→9→out",
        "entry_rank": 3,
        "removal_rank": None,
        "last_3_ranks": [],
        "price_closed_beyond_ema10": False,
        "price_closed_beyond_vwap": False,
        "removed_at": rem_at.isoformat(),
    }

    with patch.object(ot, "_row_to_dict", return_value=trade), patch.object(
        ot, "latest_r_lock_removal", return_value={"rule": "R2", "at": rem_at}
    ), patch.object(ot, "_symbol_on_lock", return_value=False), patch.object(
        ot,
        "_confirmed_10m_levels",
        return_value={"close": 2820.0, "ema5": 2830.0, "ema10": 2840.0, "bar_at": "2026-07-14T10:25:00+05:30"},
    ), patch.object(ot, "build_lock_removal_context", return_value=ctx), patch.object(
        ot, "log_r2_exit_now"
    ):
        newly = ot.evaluate_open_trades(db, "2026-07-14")

    assert newly == ["t1"]
    params = db.execute.call_args_list[-1][0][1]
    assert params["st"] == ot.STATE_EXIT_NOW
    assert "Lock removed via R2 at 10:25" in params["tr"]
    assert "R2 rank-based removal" in params["tr"]


def test_click_revalidation_blocks_confirmed_ema10_break():
    from backend.services.kavach_open_trades import classify_click_revalidation

    v = classify_click_revalidation(
        is_long=True,
        live=100.0,
        confirmed_close=98.0,
        ema10=99.0,
        vwap=99.5,
        bar_hm="10:40",
    )
    assert v["blocked"] is True
    assert "beyond EMA10 at 10:40" in v["message"]
    assert "Re-scan required" in v["message"]


def test_click_revalidation_warns_borderline_live():
    from backend.services.kavach_open_trades import classify_click_revalidation

    v = classify_click_revalidation(
        is_long=True,
        live=98.5,
        confirmed_close=100.5,
        ema10=99.0,
        vwap=99.2,
        bar_hm="10:40",
    )
    assert v["blocked"] is False
    assert v["warning"]
    assert "Borderline" in v["warning"]


def test_lock_removal_structure_labels():
    from backend.services.kavach_open_trades import lock_removal_structure_label

    assert "R1 structure" in lock_removal_structure_label("R1", price_closed_beyond_ema10=True)
    assert "NOT closed" in lock_removal_structure_label("R2", price_closed_beyond_ema10=False)
    assert "HAS closed" in lock_removal_structure_label("R2", price_closed_beyond_ema10=True)

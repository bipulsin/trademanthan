"""Unit tests: Tarang Phase 3 state machine, exit triggers, paper lifecycle."""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from backend.services.tarang.exit_engine import evaluate_exits
from backend.services.tarang.paper_broker import (
    mark_to_market,
    simulate_entry_fills,
    simulate_exit_fills,
    simulated_fill_price,
)
from backend.services.tarang.report import compute_metrics
from backend.services.tarang.state_machine import (
    CLOSED,
    ENTRY_PENDING,
    EXIT_PENDING,
    IN_TRADE,
    InvalidTransition,
    QUALIFIED,
    REPORTED,
    assert_transition,
    can_transition,
)


def test_state_machine_happy_path():
    assert can_transition(QUALIFIED, ENTRY_PENDING)
    assert can_transition(ENTRY_PENDING, IN_TRADE)
    assert can_transition(IN_TRADE, EXIT_PENDING)
    assert can_transition(EXIT_PENDING, CLOSED)
    assert can_transition(CLOSED, REPORTED)
    assert not can_transition(REPORTED, IN_TRADE)
    with pytest.raises(InvalidTransition):
        assert_transition(CLOSED, IN_TRADE)


def test_paper_fill_price_adverse():
    # mid 10, spread 2 → sell gets 10 - 0.4*2 = 9.2; buy pays 10.8
    assert abs(simulated_fill_price("SELL", 9, 11, 10, spread_fraction=0.4) - 9.2) < 1e-9
    assert abs(simulated_fill_price("BUY", 9, 11, 10, spread_fraction=0.4) - 10.8) < 1e-9


def _sample_legs():
    return [
        {"side": "SELL", "right": "PE", "strike": 270, "bid": 0.9, "ask": 1.1, "mid": 1.0, "symbol": "S1"},
        {"side": "BUY", "right": "PE", "strike": 265, "bid": 0.4, "ask": 0.6, "mid": 0.5, "symbol": "L1"},
    ]


def test_paper_entry_buy_longs_first_and_credit():
    legs = _sample_legs()
    res = simulate_entry_fills(legs, units=1, venue="upstox_mcx", lot_size=1250, spread_fraction=0.4)
    assert res["ok"]
    # BUY first in fill order
    assert res["fills"][0]["side"] == "BUY"
    assert res["fills"][1]["side"] == "SELL"
    assert res["net_credit_pts"] > 0
    assert res["fees_inr"] > 0


def test_paper_roundtrip_pnl_and_mtm():
    legs = _sample_legs()
    entry = simulate_entry_fills(legs, units=1, venue="upstox_mcx", lot_size=1250, spread_fraction=0.4)
    assert entry["ok"]
    # Marks equal to entry → near-zero unrealized before fees
    mtm = mark_to_market(
        legs,
        entry["fills"],
        units=1,
        venue="upstox_mcx",
        lot_size=1250,
        live_quotes=legs,
    )
    assert abs(mtm["unrealized_pnl_pts"]) < 0.5  # within slip noise of mids vs fills
    exit_ = simulate_exit_fills(
        legs,
        entry["fills"],
        units=1,
        venue="upstox_mcx",
        lot_size=1250,
        live_quotes=legs,
        spread_fraction=0.4,
    )
    assert exit_["ok"]
    # Round-trip with adverse fills both ways should lose a bit vs mid credit
    assert exit_["gross_pnl_inr"] is not None


def test_exit_profit_target():
    # credit 1.0, debit 0.5 → captured 50% ≥ 40%
    ev = evaluate_exits(
        entry_credit_pts=1.0,
        debit_to_close_pts=0.5,
        unrealized_pnl_inr=500,
        max_profit_inr=1000,
        max_loss_inr=4000,
        budget_inr=4000,
        short_deltas=[0.12],
        venue="upstox_mcx",
        profile_id="CL",
        holding_mode="INTRADAY",
        exit_levels={"profit_take_frac": 0.40, "credit_stop_multiple": 2.0},
        now=datetime(2026, 9, 19, 10, 0, tzinfo=timezone.utc),
    )
    assert ev.should_exit
    assert ev.reason == "PROFIT_TARGET"


def test_exit_credit_stop():
    ev = evaluate_exits(
        entry_credit_pts=1.0,
        debit_to_close_pts=2.1,
        unrealized_pnl_inr=-1500,
        max_profit_inr=1000,
        max_loss_inr=4000,
        budget_inr=4000,
        short_deltas=[0.12],
        venue="upstox_mcx",
        profile_id="CL",
        exit_levels={"profit_take_frac": 0.40, "credit_stop_multiple": 2.0},
        now=datetime(2026, 9, 19, 10, 0, tzinfo=timezone.utc),
    )
    assert ev.should_exit
    assert ev.reason == "CREDIT_STOP"


def test_exit_delta_stop():
    ev = evaluate_exits(
        entry_credit_pts=1.0,
        debit_to_close_pts=1.0,
        unrealized_pnl_inr=0,
        max_profit_inr=1000,
        max_loss_inr=4000,
        budget_inr=4000,
        short_deltas=[0.32],
        venue="upstox_mcx",
        profile_id="CL",
        exit_levels={"profit_take_frac": 0.40, "credit_stop_multiple": 2.0, "delta_stop_min": 0.30},
        now=datetime(2026, 9, 19, 10, 0, tzinfo=timezone.utc),
    )
    assert ev.should_exit
    assert ev.reason == "DELTA_STOP"


def test_exit_budget_stop():
    ev = evaluate_exits(
        entry_credit_pts=1.0,
        debit_to_close_pts=1.5,
        unrealized_pnl_inr=-5000,
        max_profit_inr=1000,
        max_loss_inr=4000,
        budget_inr=4000,
        short_deltas=[0.12],
        venue="upstox_mcx",
        profile_id="CL",
        now=datetime(2026, 9, 19, 10, 0, tzinfo=timezone.utc),
    )
    assert ev.should_exit
    assert ev.reason == "BUDGET_STOP"


def test_exit_hard_exit_mcx():
    # 18:00 UTC = 23:30 IST — past 23:15
    now = datetime(2026, 9, 19, 18, 0, tzinfo=timezone.utc)
    ev = evaluate_exits(
        entry_credit_pts=1.0,
        debit_to_close_pts=0.9,
        unrealized_pnl_inr=100,
        max_profit_inr=1000,
        max_loss_inr=4000,
        budget_inr=4000,
        short_deltas=[0.12],
        venue="upstox_mcx",
        profile_id="CL",
        holding_mode="INTRADAY",
        now=now,
    )
    assert ev.should_exit
    assert ev.reason == "HARD_EXIT"


def test_report_metrics():
    trades = [
        {"net_pnl": 500, "gross_pnl": 550, "fees_total": 50, "exit_reason": "PROFIT_TARGET", "profile_id": "CL"},
        {"net_pnl": -800, "gross_pnl": -750, "fees_total": 50, "exit_reason": "CREDIT_STOP", "profile_id": "CL"},
        {"net_pnl": 300, "gross_pnl": 340, "fees_total": 40, "exit_reason": "MANUAL", "profile_id": "NG"},
    ]
    m = compute_metrics(trades)
    assert m["count"] == 3
    assert abs(m["win_rate"] - 2 / 3) < 1e-9
    assert m["avg_win"] == 400.0
    assert m["avg_loss"] == -800.0
    assert m["profit_factor"] is not None
    assert m["expectancy"] == pytest.approx((500 - 800 + 300) / 3)


def test_paper_lifecycle_with_db_fixtures():
    """Screener→take→exit→report using DB mocks (no live venue)."""
    from backend.services.tarang import lifecycle as lc

    legs = _sample_legs()
    candidate = {
        "id": 99,
        "profile_id": "CL",
        "structure": "put_credit_spread",
        "status": "qualified",
        "payload": {
            "status": "QUALIFIED",
            "legs": legs,
            "lots_or_contracts": 1,
            "net_credit": 0.5,
            "width": 5,
            "max_loss_per_unit_inr": 4375,
            "venue": "upstox_mcx",
            "lot_size": 1250,
            "exit_levels": {"profit_take_frac": 0.4, "credit_stop_multiple": 2.0},
            "budget_inr": 40000,
            "max_profit_approx_inr": 625,
            "iv_percentile": 60,
            "atm_iv": 0.35,
            "expiry": "2026-10-01",
            "underlying": "CRUDEOIL",
        },
    }

    class FakeResult:
        def __init__(self, mapping=None):
            self._m = mapping

        def mappings(self):
            return self

        def first(self):
            return self._m

        def all(self):
            return []

    trade_id_box = {"id": 501}
    state = {"status": None, "events": []}

    def execute(sql, params=None):
        q = str(sql)
        params = params or {}
        if "INSERT INTO tarang_trades" in q:
            state["status"] = params.get("status")
            return FakeResult({"id": trade_id_box["id"]})
        if "INSERT INTO tarang_trade_events" in q:
            state["events"].append(params)
            return FakeResult({"id": len(state["events"])})
        if "INSERT INTO tarang_fills" in q or "INSERT INTO tarang_orders" in q:
            return FakeResult({"id": 1})
        if "INSERT INTO tarang_alerts" in q:
            return FakeResult({"id": 1})
        if "UPDATE tarang_trades" in q and "status" in params:
            state["status"] = params.get("st") or params.get("status") or state["status"]
            return FakeResult({"id": trade_id_box["id"]})
        if "UPDATE tarang_candidates" in q:
            return FakeResult({"id": 99})
        if "FROM tarang_fills" in q:
            return FakeResult(None)
        if "SELECT value FROM tarang_settings" in q:
            return FakeResult({"value": "PAPER"})
        if "SELECT 1 FROM tarang_orders" in q:
            return FakeResult(None)
        return FakeResult({"id": 1})

    db = MagicMock()
    db.execute.side_effect = execute

    with patch.object(lc, "SessionLocal", return_value=db), patch.object(
        lc, "get_candidate", return_value=candidate
    ), patch.object(lc, "ensure_tarang_tables"):
        out = lc.take_trade_paper(99, note="test")
        assert out["ok"] is True
        assert out["status"] == IN_TRADE
        assert out["trade_id"] == 501

    # Exit path with get_trade mocked
    trade_row = {
        "id": 501,
        "status": IN_TRADE,
        "mode": "PAPER",
        "profile_id": "CL",
        "risk_bucket": "ENERGY",
        "holding_mode": "INTRADAY",
        "venue": "upstox_mcx",
        "lots_or_contracts": 1,
        "entry_credit": out["entry_credit_pts"],
        "max_loss": 4375,
        "fees_total": 40,
        "legs": legs,
        "meta": {
            "entry_fills": simulate_entry_fills(
                legs, units=1, venue="upstox_mcx", lot_size=1250
            )["fills"],
            "lot_size": 1250,
            "exit_levels": {"profit_take_frac": 0.4},
            "budget_inr": 40000,
            "expiry": "2026-10-01",
        },
    }

    def execute2(sql, params=None):
        q = str(sql)
        params = params or {}
        if "FROM tarang_fills" in q and "entry" in q:
            # return empty so meta entry_fills used
            class R:
                def mappings(self):
                    return self

                def all(self):
                    return []

            return R()
        if "INSERT INTO tarang_trade_events" in q:
            return FakeResult({"id": 1})
        if "INSERT INTO tarang_fills" in q or "INSERT INTO tarang_orders" in q:
            return FakeResult({"id": 1})
        if "INSERT INTO tarang_alerts" in q:
            return FakeResult({"id": 1})
        if "SELECT 1 FROM tarang_orders" in q:
            return FakeResult(None)
        if "UPDATE tarang_trades" in q:
            return FakeResult({"id": 501})
        return FakeResult({"id": 1})

    db2 = MagicMock()
    db2.execute.side_effect = execute2

    with patch.object(lc, "SessionLocal", return_value=db2), patch.object(
        lc, "get_trade", return_value=trade_row
    ), patch.object(lc, "ensure_tarang_tables"), patch.object(
        lc, "_refresh_quotes_for_trade", return_value=legs
    ):
        closed = lc.exit_trade(501, reason="MANUAL", actor="USER")
        assert closed["ok"] is True
        assert closed["status"] == REPORTED
        assert closed["exit_reason"] == "MANUAL"
        assert "net_pnl" in closed

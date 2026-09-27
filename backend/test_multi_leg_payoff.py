"""At-expiry payoff: short straddle is capped; a naked long call is not."""
import pytest

from backend.services.multi_leg_payoff import PAD, payoff_summary


def _straddle():
    return [
        {"side": "SELL", "option_type": "CE", "strike_price": 20000, "entry_price": 100, "lot_size": 50},
        {"side": "SELL", "option_type": "PE", "strike_price": 20000, "entry_price": 80, "lot_size": 50},
    ]


def test_short_straddle_has_two_breakevens_and_capped_max_profit():
    # Credit is (100 + 80) * 50. Breakevens sit one total premium either side of 20,000.
    summary = payoff_summary(_straddle(), current_spot=20050)
    assert summary["max_profit_unlimited"] is False
    assert summary["max_profit"] == pytest.approx(9000)
    assert summary["max_loss_unlimited"] is True
    assert summary["net_credit"] == pytest.approx(9000)
    assert summary["breakevens"] == pytest.approx([19820, 20180])
    assert max(summary["pnl"]) == pytest.approx(9000)


def test_naked_long_call_has_unlimited_max_profit():
    legs = [
        {"side": "BUY", "option_type": "CE", "strike_price": 20000, "entry_price": 100, "lot_size": 50},
    ]
    summary = payoff_summary(legs, current_spot=19900)
    assert summary["max_profit_unlimited"] is True
    assert summary["max_profit"] is None
    assert summary["max_loss_unlimited"] is False
    assert summary["max_loss"] == pytest.approx(-5000)
    assert summary["breakevens"] == pytest.approx([20100])
    assert summary["net_credit"] == pytest.approx(-5000)


def test_spot_range_pads_outer_strikes_and_includes_current_spot():
    legs = [
        {"side": "BUY", "option_type": "CE", "strike_price": 22000, "entry_price": 10, "lot_size": 50},
        {"side": "BUY", "option_type": "PE", "strike_price": 18000, "entry_price": 10, "lot_size": 50},
    ]
    inside = payoff_summary(legs, current_spot=20000)
    assert inside["spot_min"] == pytest.approx(18000 * (1 - PAD))
    assert inside["spot_max"] == pytest.approx(22000 * (1 + PAD))
    assert inside["spot_min"] <= 20000 <= inside["spot_max"]

    outside = payoff_summary(legs, current_spot=30000)
    assert outside["spot_min"] == pytest.approx(18000 * (1 - PAD))
    assert outside["spot_max"] >= 30000
    assert outside["spot_min"] <= 18000
    assert outside["spot_max"] >= 22000


def test_defined_risk_wings_cap_both_sides():
    legs = [
        {"side": "SELL", "option_type": "CE", "strike_price": 20000, "entry_price": 150, "lot_size": 50},
        {"side": "SELL", "option_type": "PE", "strike_price": 20000, "entry_price": 150, "lot_size": 50},
        {"side": "BUY", "option_type": "CE", "strike_price": 21000, "entry_price": 40, "lot_size": 50},
        {"side": "BUY", "option_type": "PE", "strike_price": 19000, "entry_price": 40, "lot_size": 50},
    ]
    summary = payoff_summary(legs, current_spot=20000)
    assert summary["max_profit_unlimited"] is False
    assert summary["max_loss_unlimited"] is False
    assert summary["max_profit"] == pytest.approx(11000)
    assert summary["max_loss"] == pytest.approx(-39000)
    assert len(summary["breakevens"]) == 2


def test_exited_leg_is_flat_and_exit_price_alone_stays_open():
    closed = payoff_summary([
        {
            "side": "BUY",
            "option_type": "CE",
            "strike_price": 20000,
            "entry_price": 100,
            "lot_size": 50,
            "exit_price": 130,
            "exit_time": "2026-09-27T15:30:00+05:30",
        }
    ])
    assert closed["max_profit_unlimited"] is False
    assert closed["max_loss_unlimited"] is False
    assert closed["max_profit"] == pytest.approx(1500)
    assert closed["max_loss"] == pytest.approx(1500)
    assert closed["breakevens"] == []
    assert closed["net_credit"] == pytest.approx(-5000)

    still_open = payoff_summary([
        {
            "side": "BUY",
            "option_type": "CE",
            "strike_price": 20000,
            "entry_price": 100,
            "lot_size": 50,
            "exit_price": 130,
        }
    ], current_spot=20000)
    assert still_open["max_profit_unlimited"] is True
    assert still_open["max_loss"] == pytest.approx(-5000)
    assert still_open["net_credit"] < 0

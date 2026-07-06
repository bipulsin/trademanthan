"""3:00–3:15 PM option premium gate evaluation."""
from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional, Tuple

from backend.services.btst_backtest.atm import resolve_atm_option
from backend.services.btst_backtest.data_access import BtstDataAccess
from backend.services.btst_backtest.gates import check_hull_gate, check_supertrend_gate
from backend.services.btst_backtest.timing import close_at_or_before, ist_dt


def evaluate_entry(
    data: BtstDataAccess,
    candidate: Dict[str, Any],
    cfg: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Resolve ATM option and run Supertrend + Hull on premium candles.
    Returns fields to merge into the result row.
    """
    trade_date: date = candidate["trade_date"]
    sym = candidate["stock_symbol"]
    direction = candidate["direction"]
    opt_type = "CE" if direction == "bullish" else "PE"
    spot_1500 = data.spot_at(candidate["instrument_key"], trade_date, cfg["atm_hhmm"])
    if spot_1500 is None:
        return {
            "atm_strike": None,
            "option_symbol": None,
            "data_mode": "manual_fill",
            "premium_at_1500": None,
            "supertrend_pass": None,
            "hull_pass": None,
            "eligible_final": False,
            "no_eligible_reason": "atm_spot_unavailable",
            "lot_size": None,
            "direction": direction,
        }
    atm_strike, option_symbol, option_key, lot_size = resolve_atm_option(
        sym, spot_1500, trade_date, opt_type
    )
    base = {
        "atm_strike": atm_strike,
        "option_symbol": option_symbol,
        "direction": direction,
        "lot_size": lot_size,
        "premium_at_1500": None,
        "supertrend_pass": None,
        "hull_pass": None,
    }
    if not option_key:
        return {
            **base,
            "data_mode": "manual_fill",
            "eligible_final": False,
            "no_eligible_reason": "atm_option_unresolved",
        }
    premium_m5 = data.get_candles_5m(option_key, trade_date, days_back=5)
    prem_1500 = close_at_or_before(premium_m5, trade_date, cfg["atm_hhmm"])
    base["premium_at_1500"] = prem_1500
    full = data.option_premium_history_usable(option_key, trade_date)
    if not full:
        return {
            **base,
            "data_mode": "manual_fill",
            "eligible_final": False,
            "no_eligible_reason": "manual_fill_premium_history",
        }
    st_pass, _st = check_supertrend_gate(
        premium_m5,
        trade_date,
        cfg["premium_gate_hhmm"],
        period=int(cfg["supertrend_period"]),
        multiplier=float(cfg["supertrend_multiplier"]),
    )
    hull_pass, _h, _rising = check_hull_gate(
        premium_m5,
        trade_date,
        cfg["premium_gate_hhmm"],
        length=int(cfg["hull_length"]),
    )
    base["supertrend_pass"] = st_pass
    base["hull_pass"] = hull_pass
    if not (st_pass and hull_pass):
        return {
            **base,
            "data_mode": "full",
            "eligible_final": False,
            "no_eligible_reason": "premium_indicators_failed",
        }
    entry_premium = close_at_or_before(premium_m5, trade_date, cfg["entry_hhmm"])
    entry_time = ist_dt(trade_date, cfg["entry_hhmm"])
    buy_cost = None
    if entry_premium is not None and lot_size:
        buy_cost = float(entry_premium) * int(lot_size)
    return {
        **base,
        "data_mode": "full",
        "eligible_final": True,
        "no_eligible_reason": None,
        "entry_time": entry_time,
        "entry_premium": entry_premium,
        "buy_cost": buy_cost,
        "option_instrument_key": option_key,
        "premium_candles": premium_m5,
    }

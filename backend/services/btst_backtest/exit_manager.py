"""Fixed-time exit scenarios A (3:25 same day) and B (9:20 next trading day)."""
from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional

from backend.services.btst_backtest.data_access import BtstDataAccess
from backend.services.btst_backtest.timing import close_at_or_before, ist_dt, next_trading_day


def compute_exits(
    data: BtstDataAccess,
    trade_date: date,
    option_key: str,
    premium_candles: List[dict],
    entry_premium: Optional[float],
    lot_size: Optional[int],
    cfg: Dict[str, Any],
) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if entry_premium is None or not lot_size:
        return out
    lot = int(lot_size)
    # Scenario A — same day 3:25 PM
    exit_a_time = ist_dt(trade_date, cfg["exit_a_hhmm"])
    exit_a_prem = close_at_or_before(premium_candles, trade_date, cfg["exit_a_hhmm"])
    out["exit_a_time"] = exit_a_time
    out["exit_a_premium"] = exit_a_prem
    if exit_a_prem is not None:
        out["exit_a_pnl"] = (float(exit_a_prem) - float(entry_premium)) * lot

    # Scenario B — next trading day 9:20 AM
    nd = next_trading_day(trade_date)
    nd_candles = data.get_candles_5m(option_key, nd, days_back=3)
    exit_b_time = ist_dt(nd, cfg["exit_b_hhmm"])
    exit_b_prem = close_at_or_before(nd_candles, nd, cfg["exit_b_hhmm"])
    out["exit_b_time"] = exit_b_time
    out["exit_b_premium"] = exit_b_prem
    if exit_b_prem is not None:
        out["exit_b_pnl"] = (float(exit_b_prem) - float(entry_premium)) * lot
    return out


def recalc_pnls(
    entry_premium: Optional[float],
    exit_a_premium: Optional[float],
    exit_b_premium: Optional[float],
    lot_size: Optional[int],
) -> Dict[str, Optional[float]]:
    if entry_premium is None or not lot_size:
        return {"exit_a_pnl": None, "exit_b_pnl": None, "buy_cost": None}
    lot = int(lot_size)
    buy_cost = float(entry_premium) * lot
    exit_a_pnl = (
        (float(exit_a_premium) - float(entry_premium)) * lot if exit_a_premium is not None else None
    )
    exit_b_pnl = (
        (float(exit_b_premium) - float(entry_premium)) * lot if exit_b_premium is not None else None
    )
    return {"exit_a_pnl": exit_a_pnl, "exit_b_pnl": exit_b_pnl, "buy_cost": buy_cost}

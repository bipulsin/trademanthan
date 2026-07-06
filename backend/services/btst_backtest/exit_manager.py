"""Fixed-time exit PnL helpers (shared with manual-fill PATCH)."""
from __future__ import annotations

from typing import Dict, Optional


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

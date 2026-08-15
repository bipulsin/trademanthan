"""Risk/return analytics for Rocket tear sheets."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence

import numpy as np
import pandas as pd

from rocket.engine.costs import CostAccumulator
from rocket.engine.portfolio import ClosedTrade


def _equity_series(equity_curve: Sequence[dict]) -> pd.Series:
    if not equity_curve:
        return pd.Series(dtype=float)
    idx = pd.to_datetime([e["timestamp"] for e in equity_curve], utc=True)
    vals = [float(e["equity"]) for e in equity_curve]
    s = pd.Series(vals, index=idx).sort_index()
    return s[~s.index.duplicated(keep="last")]


def max_drawdown(equity: pd.Series) -> tuple[float, Optional[pd.Timedelta]]:
    if equity.empty:
        return 0.0, None
    peak = equity.cummax()
    dd = (equity - peak) / peak.replace(0, np.nan)
    mdd = float(dd.min()) if len(dd) else 0.0
    duration = None
    if mdd < 0:
        trough_i = int(dd.idxmin().value) if hasattr(dd.idxmin(), "value") else None
        # duration: bars underwater until recovery — approximate via time span of worst episode
        under = dd < 0
        if under.any():
            # longest contiguous underwater period
            groups = (~under).cumsum()
            lengths = under.groupby(groups).sum()
            # map to time
            worst_group = lengths.idxmax() if not lengths.empty else None
            if worst_group is not None:
                ep = dd[groups == worst_group]
                if len(ep) >= 2:
                    duration = ep.index[-1] - ep.index[0]
    return abs(mdd), duration


def compute_performance(
    *,
    initial_capital: float,
    equity_curve: Sequence[dict],
    trades: Sequence[ClosedTrade],
    costs: CostAccumulator,
    periods_per_year: float = 252 * 75,  # ~5m bars in Indian session
) -> Dict[str, Any]:
    equity = _equity_series(list(equity_curve))
    final_eq = float(equity.iloc[-1]) if not equity.empty else float(initial_capital)
    net_return = (final_eq / initial_capital - 1.0) if initial_capital else 0.0

    pnls = [t.pnl for t in trades]
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p <= 0]
    gross_profit = float(sum(wins)) if wins else 0.0
    gross_loss = float(abs(sum(losses))) if losses else 0.0
    profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else (float("inf") if gross_profit > 0 else 0.0)
    win_rate = (len(wins) / len(pnls)) if pnls else 0.0
    expectancy = float(np.mean(pnls)) if pnls else 0.0

    mdd, mdd_dur = max_drawdown(equity)
    rets = equity.pct_change().dropna()
    if len(rets) > 2 and rets.std() > 0:
        sharpe = float(np.sqrt(periods_per_year) * rets.mean() / rets.std())
    else:
        sharpe = 0.0
    downside = rets[rets < 0]
    if len(downside) > 2 and downside.std() > 0:
        sortino = float(np.sqrt(periods_per_year) * rets.mean() / downside.std())
    else:
        sortino = 0.0
    calmar = (net_return / mdd) if mdd > 0 else 0.0

    return {
        "initial_capital": round(initial_capital, 2),
        "final_equity": round(final_eq, 2),
        "net_return_pct": round(net_return * 100.0, 4),
        "total_trades": len(pnls),
        "win_rate_pct": round(win_rate * 100.0, 2),
        "profit_factor": round(profit_factor, 4) if profit_factor != float("inf") else None,
        "profit_factor_raw": profit_factor,
        "expectancy": round(expectancy, 2),
        "max_drawdown_pct": round(mdd * 100.0, 4),
        "max_drawdown_duration": str(mdd_dur) if mdd_dur is not None else None,
        "sharpe": round(sharpe, 4),
        "sortino": round(sortino, 4),
        "calmar": round(calmar, 4),
        "costs": costs.as_dict(),
        "equity_curve": list(equity_curve),
        "trades": [
            {
                "symbol": t.symbol,
                "side": t.side,
                "qty": t.quantity,
                "entry": t.entry_price,
                "exit": t.exit_price,
                "entry_time": t.entry_time.isoformat() if isinstance(t.entry_time, datetime) else str(t.entry_time),
                "exit_time": t.exit_time.isoformat() if isinstance(t.exit_time, datetime) else str(t.exit_time),
                "pnl": round(t.pnl, 2),
                "costs": round(t.costs, 2),
                "reason": t.reason,
            }
            for t in trades
        ],
    }

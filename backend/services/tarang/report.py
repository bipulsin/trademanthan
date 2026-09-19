"""Trade Report metrics for Kosmic Tarang Phase 3."""
from __future__ import annotations

import csv
import io
import math
from typing import Any, Dict, List, Optional

from backend.services.tarang.lifecycle import list_closed_trades


def _safe(n: Optional[float], default: float = 0.0) -> float:
    try:
        if n is None or (isinstance(n, float) and math.isnan(n)):
            return default
        return float(n)
    except (TypeError, ValueError):
        return default


def compute_metrics(trades: List[Dict[str, Any]]) -> Dict[str, Any]:
    pnls = [_safe(t.get("net_pnl")) for t in trades]
    if not pnls:
        return {
            "count": 0,
            "win_rate": None,
            "avg_win": None,
            "avg_loss": None,
            "profit_factor": None,
            "expectancy": None,
            "gross_total": 0.0,
            "net_total": 0.0,
            "max_drawdown": None,
            "worst_trade": None,
            "by_exit_reason": {},
            "by_profile": {},
        }
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p < 0]
    gross_wins = sum(wins)
    gross_losses = abs(sum(losses))
    pf = (gross_wins / gross_losses) if gross_losses > 0 else (None if not wins else float("inf"))
    win_rate = len(wins) / len(pnls) if pnls else None
    avg_win = (sum(wins) / len(wins)) if wins else None
    avg_loss = (sum(losses) / len(losses)) if losses else None
    expectancy = (sum(pnls) / len(pnls)) if pnls else None

    # Max drawdown on equity curve (ordered as given — prefer exit time order)
    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    for p in pnls:
        equity += p
        peak = max(peak, equity)
        max_dd = min(max_dd, equity - peak)
    worst = min(pnls) if pnls else None

    by_reason: Dict[str, int] = {}
    by_profile: Dict[str, Dict[str, Any]] = {}
    for t, p in zip(trades, pnls):
        er = t.get("exit_reason") or "UNKNOWN"
        by_reason[er] = by_reason.get(er, 0) + 1
        pid = t.get("profile_id") or "?"
        bucket = by_profile.setdefault(pid, {"count": 0, "net": 0.0})
        bucket["count"] += 1
        bucket["net"] += p

    return {
        "count": len(pnls),
        "win_rate": win_rate,
        "avg_win": avg_win,
        "avg_loss": avg_loss,
        "profit_factor": pf if pf != float("inf") else None,
        "profit_factor_infinite": pf == float("inf"),
        "expectancy": expectancy,
        "gross_total": sum(_safe(t.get("gross_pnl")) for t in trades),
        "net_total": sum(pnls),
        "fees_total": sum(_safe(t.get("fees_total")) for t in trades),
        "max_drawdown": max_dd,
        "worst_trade": worst,
        "by_exit_reason": by_reason,
        "by_profile": by_profile,
    }


def build_report(
    *,
    mode: str = "PAPER",
    profile_id: Optional[str] = None,
    limit: int = 200,
) -> Dict[str, Any]:
    trades = list_closed_trades(mode=mode, limit=limit)
    if profile_id:
        trades = [t for t in trades if t.get("profile_id") == profile_id.upper()]
    metrics = compute_metrics(trades)
    live_empty = list_closed_trades(mode="LIVE", limit=5) if mode == "PAPER" else []
    return {
        "product": "Kosmic Tarang",
        "phase": 3,
        "mode": mode,
        "trades": trades,
        "metrics": metrics,
        "live_trades": live_empty if mode == "PAPER" else trades,
        "note": "LIVE book empty until Phase 4." if mode == "PAPER" else None,
    }


def report_csv(mode: str = "PAPER", limit: int = 500) -> str:
    trades = list_closed_trades(mode=mode, limit=limit)
    buf = io.StringIO()
    fields = [
        "id",
        "profile_id",
        "structure",
        "mode",
        "status",
        "entry_at",
        "exit_at",
        "exit_reason",
        "entry_credit",
        "gross_pnl",
        "net_pnl",
        "fees_total",
        "lots_or_contracts",
        "venue",
    ]
    w = csv.DictWriter(buf, fieldnames=fields, extrasaction="ignore")
    w.writeheader()
    for t in trades:
        w.writerow({k: t.get(k) for k in fields})
    return buf.getvalue()

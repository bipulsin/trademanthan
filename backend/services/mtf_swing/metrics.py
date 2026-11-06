"""Closed-trade dashboard math. OPEN rows are visible in the blotter and excluded here."""
from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Sequence

from backend.services.mtf_swing.config import month_windows


def _f(raw: Any) -> Optional[float]:
    if raw is None:
        return None
    s = str(raw).strip()
    if s == "":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def is_closed(row: Dict[str, Any]) -> bool:
    return str(row.get("exit_reason") or "").upper() != "OPEN"


def _max_drawdown(nets: Sequence[float]) -> float:
    peak = 0.0
    equity = 0.0
    worst = 0.0
    for n in nets:
        equity += n
        if equity > peak:
            peak = equity
        dd = peak - equity
        if dd > worst:
            worst = dd
    return worst


def _block(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    closed = [r for r in rows if is_closed(r)]
    nets: List[float] = []
    for r in closed:
        n = _f(r.get("net_pnl"))
        nets.append(0.0 if n is None else n)
    wins = [n for n in nets if n > 0]
    losses = [n for n in nets if n < 0]
    total = len(nets)
    net = sum(nets)
    gross_win = sum(wins)
    gross_loss = abs(sum(losses))
    if total == 0:
        pf = None
    elif gross_loss == 0:
        pf = None if gross_win == 0 else float("inf")
    else:
        pf = gross_win / gross_loss
    return {
        "trades": total,
        "net_pnl": net,
        "win_rate": (len(wins) / total) if total else None,
        "avg_pnl": (net / total) if total else None,
        "profit_factor": pf,
        "max_profit": max(nets) if nets else None,
        "max_loss": min(nets) if nets else None,
        "max_drawdown": _max_drawdown(nets) if nets else 0.0,
        "profitable": len(wins),
        "losing": len(losses),
    }


def _sort_closed(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    closed = [r for r in rows if is_closed(r)]
    closed.sort(key=lambda r: (str(r.get("exit_time") or ""), str(r.get("trade_id") or "")))
    return closed


def build_dashboard(rows: Sequence[Dict[str, Any]], timeframe: str = "all") -> Dict[str, Any]:
    chosen = list(rows)
    if timeframe and timeframe != "all":
        chosen = [r for r in chosen if str(r.get("timeframe") or "") == timeframe]
    closed = _sort_closed(chosen)
    summary = _block(closed)
    months_meta = month_windows()
    by_month: Dict[str, List[Dict[str, Any]]] = {label: [] for _, _, label in months_meta}
    for r in closed:
        key = str(r.get("month") or "")
        by_month.setdefault(key, []).append(r)
    months = []
    for _, _, label in months_meta:
        block = _block(by_month.get(label) or [])
        block["month"] = label
        months.append(block)
    equity = []
    peak = 0.0
    curve = 0.0
    for r in closed:
        n = _f(r.get("net_pnl")) or 0.0
        curve += n
        if curve > peak:
            peak = curve
        equity.append(
            {
                "exit_time": r.get("exit_time") or "",
                "symbol": r.get("symbol") or "",
                "equity": curve,
                "drawdown": peak - curve,
            }
        )
    reasons: Dict[str, int] = {"SL": 0, "T1": 0, "TIME": 0, "OPEN": 0}
    sides: Dict[str, int] = {"LONG": 0, "SHORT": 0}
    for r in chosen:
        er = str(r.get("exit_reason") or "").upper()
        if er in reasons:
            reasons[er] += 1
        elif er:
            reasons[er] = reasons.get(er, 0) + 1
        if is_closed(r):
            side = str(r.get("side") or "").upper()
            if side in sides:
                sides[side] += 1
    return {
        "timeframe": timeframe or "all",
        "summary": summary,
        "months": months,
        "charts": {
            "month_pnl": [{"month": m["month"], "net_pnl": m["net_pnl"]} for m in months],
            "equity": equity,
            "exit_reasons": reasons,
            "sides": sides,
        },
        "open_trades": sum(1 for r in chosen if not is_closed(r)),
    }

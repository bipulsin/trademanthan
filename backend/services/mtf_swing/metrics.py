"""Closed-trade dashboard math. OPEN rows are visible in the blotter and excluded here."""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

from backend.services.mtf_swing.config import V2_MONTHS, month_windows


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


def _open_block(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    opens = [r for r in rows if not is_closed(r)]
    mtms = []
    for r in opens:
        m = _f(r.get("mtm_pnl"))
        if m is not None:
            mtms.append(m)
    return {
        "open_trades": len(opens),
        "mtm_pnl_sum": sum(mtms) if mtms else 0.0,
        "mtm_available": len(mtms),
    }


def build_dashboard(rows: Sequence[Dict[str, Any]], timeframe: str = "all") -> Dict[str, Any]:
    chosen = list(rows)
    if timeframe and timeframe != "all":
        chosen = [r for r in chosen if str(r.get("timeframe") or "") == timeframe]
    closed = _sort_closed(chosen)
    summary = _block(closed)
    open_info = _open_block(chosen)
    summary["open_trades"] = open_info["open_trades"]
    summary["mtm_pnl_sum"] = open_info["mtm_pnl_sum"]
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
    reasons: Dict[str, int] = {"SL": 0, "T1": 0, "TP": 0, "TIME": 0, "OPEN": 0}
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
        "open_trades": open_info["open_trades"],
        "open_mtm": open_info,
    }


def filter_rows(
    rows: Sequence[Dict[str, Any]],
    *,
    timeframe: str = "all",
    month: str = "all",
    variant: str = "all",
) -> List[Dict[str, Any]]:
    out = list(rows)
    if timeframe and timeframe != "all":
        out = [r for r in out if str(r.get("timeframe") or "") == timeframe]
    if month and month != "all":
        out = [r for r in out if str(r.get("month") or "") == month]
    if variant and variant != "all":
        out = [r for r in out if str(r.get("variant") or "") == variant]
    return out


def build_funnel(discards: Sequence[Dict[str, Any]], trades: Sequence[Dict[str, Any]]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for d in discards:
        reason = str(d.get("reason") or "UNKNOWN")
        counts[reason] = counts.get(reason, 0) + 1
    counts["ENTERED"] = sum(1 for t in trades if is_closed(t) or str(t.get("exit_reason") or "").upper() == "OPEN")
    counts["CLOSED"] = sum(1 for t in trades if is_closed(t))
    counts["OPEN"] = sum(1 for t in trades if not is_closed(t))
    return counts


def compare_summaries(v1: Dict[str, Any], v2: Dict[str, Any]) -> Dict[str, Any]:
    """Headline v1 vs v2 comparison with deltas on shared numeric fields."""
    keys = ("trades", "net_pnl", "win_rate", "avg_pnl", "profit_factor", "max_drawdown", "open_trades")
    out = {"v1": {}, "v2": {}, "delta": {}}
    for k in keys:
        a = v1.get(k)
        b = v2.get(k)
        out["v1"][k] = a
        out["v2"][k] = b
        if isinstance(a, (int, float)) and isinstance(b, (int, float)) and a is not None and b is not None:
            if a == float("inf") or b == float("inf"):
                out["delta"][k] = None
            else:
                out["delta"][k] = b - a
        else:
            out["delta"][k] = None
    return out


def build_v2_dashboard(
    trades: Sequence[Dict[str, Any]],
    discards: Optional[Sequence[Dict[str, Any]]] = None,
    *,
    timeframe: str = "all",
    month: str = "all",
    variant: str = "all",
    v1_trades: Optional[Sequence[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    view = filter_rows(trades, timeframe=timeframe, month=month, variant=variant)
    dash = build_dashboard(view, timeframe="all")  # already filtered
    # Restrict month table to v2 experiment months when useful.
    if month and month != "all":
        dash["months"] = [m for m in dash["months"] if m.get("month") == month]
    else:
        dash["months"] = [m for m in dash["months"] if m.get("month") in V2_MONTHS] or dash["months"]
    disc = filter_rows(discards or [], timeframe=timeframe, month=month, variant=variant)
    funnel = build_funnel(disc, view)
    comparison = None
    if v1_trades is not None:
        # Fair apples-to-apples: when month=all, still restrict v1 to the v2
        # experiment months rather than the full Nov'25–Oct'26 book.
        if month and month != "all":
            v1_view = filter_rows(v1_trades, timeframe=timeframe, month=month)
        else:
            v1_view = [
                r
                for r in filter_rows(v1_trades, timeframe=timeframe, month="all")
                if str(r.get("month") or "") in V2_MONTHS
            ]
        v1_dash = build_dashboard(v1_view, timeframe="all")
        comparison = compare_summaries(v1_dash["summary"], dash["summary"])
    return {
        "timeframe": timeframe,
        "month": month,
        "variant": variant,
        "summary": dash["summary"],
        "months": dash["months"],
        "charts": dash["charts"],
        "open_trades": dash["open_trades"],
        "open_mtm": dash.get("open_mtm") or {},
        "funnel": funnel,
        "comparison": comparison,
        "trades": view,
    }


def ablation_table(trades: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Roll up by variant × timeframe × month for the ablation summary table."""
    groups: Dict[tuple, List[Dict[str, Any]]] = {}
    for r in trades:
        key = (
            str(r.get("variant") or "all"),
            str(r.get("timeframe") or ""),
            str(r.get("month") or ""),
        )
        groups.setdefault(key, []).append(r)
    rows = []
    for (variant, tf, month), group in sorted(groups.items()):
        block = _block(group)
        open_info = _open_block(group)
        rows.append(
            {
                "variant": variant,
                "timeframe": tf,
                "month": month,
                "trades": block["trades"],
                "net_pnl": block["net_pnl"],
                "win_rate": block["win_rate"],
                "profit_factor": block["profit_factor"],
                "avg_pnl": block["avg_pnl"],
                "open_trades": open_info["open_trades"],
                "mtm_pnl_sum": open_info["mtm_pnl_sum"],
            }
        )
    return rows

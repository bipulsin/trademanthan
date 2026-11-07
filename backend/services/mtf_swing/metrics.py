"""Closed-trade dashboard math. OPEN rows are visible in the blotter and excluded here."""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Tuple

from backend.services.mtf_swing.config import ABLATION_VARIANTS, month_windows


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
    rs: List[float] = []
    for r in closed:
        n = _f(r.get("net_pnl"))
        nets.append(0.0 if n is None else n)
        rr = _f(r.get("r_multiple"))
        if rr is not None:
            rs.append(rr)
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
        "avg_r": (sum(rs) / len(rs)) if rs else None,
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
        "open_rows": opens,
    }


def _equity_curve(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    closed = _sort_closed(rows)
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
    return equity


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
            "equity": _equity_curve(chosen),
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
    variant: str = "any",
) -> List[Dict[str, Any]]:
    """Filter trades. ``variant=any`` (or empty) means no variant filter.

    ``variant=all`` selects the full-feature combo rows tagged ``all``.
    """
    out = list(rows)
    if timeframe and timeframe != "all":
        out = [r for r in out if str(r.get("timeframe") or "") == timeframe]
    if month and month not in ("all", "full", ""):
        out = [r for r in out if str(r.get("month") or "") == month]
    v = (variant or "any").strip().lower()
    if v and v not in ("any", "*", "all_variants"):
        out = [r for r in out if str(r.get("variant") or "") == v]
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
    """Headline v1 vs v2 comparison. Win-rate delta is in percentage points."""
    keys = ("trades", "net_pnl", "win_rate", "avg_pnl", "avg_r", "profit_factor", "max_drawdown", "open_trades")
    out = {"v1": {}, "v2": {}, "delta": {}, "delta_pp": {}}
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
        if k == "win_rate" and out["delta"][k] is not None:
            out["delta_pp"][k] = out["delta"][k] * 100.0
        else:
            out["delta_pp"][k] = None
    return out


def _month_compare(
    v1_months: Sequence[Dict[str, Any]],
    v2_months: Sequence[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    by_v1 = {m["month"]: m for m in v1_months}
    by_v2 = {m["month"]: m for m in v2_months}
    labels = [lab for _, _, lab in month_windows()]
    rows = []
    for lab in labels:
        a = by_v1.get(lab) or _block([])
        b = by_v2.get(lab) or _block([])
        a_net = float(a.get("net_pnl") or 0)
        b_net = float(b.get("net_pnl") or 0)
        a_wr = a.get("win_rate")
        b_wr = b.get("win_rate")
        wr_pp = None
        if isinstance(a_wr, (int, float)) and isinstance(b_wr, (int, float)):
            wr_pp = (b_wr - a_wr) * 100.0
        rows.append(
            {
                "month": lab,
                "v1_trades": a.get("trades") or 0,
                "v2_trades": b.get("trades") or 0,
                "v1_net_pnl": a_net,
                "v2_net_pnl": b_net,
                "delta_net_pnl": b_net - a_net,
                "v1_win_rate": a_wr,
                "v2_win_rate": b_wr,
                "delta_wr_pp": wr_pp,
                "v1_pf": a.get("profit_factor"),
                "v2_pf": b.get("profit_factor"),
                "v2_worse": b_net < a_net,
            }
        )
    return rows


def robustness_block(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    """Concentration, exposure, side/regime breakdowns for the selected view."""
    closed = [r for r in rows if is_closed(r)]
    by_symbol: Dict[str, float] = {}
    by_side: Dict[str, Dict[str, Any]] = {}
    by_regime: Dict[str, Dict[str, Any]] = {}
    for r in closed:
        sym = str(r.get("symbol") or "")
        net = _f(r.get("net_pnl")) or 0.0
        by_symbol[sym] = by_symbol.get(sym, 0.0) + net
        side = str(r.get("side") or "UNKNOWN").upper()
        by_side.setdefault(side, []).append(r)
        regime = str(r.get("regime") or "n/a")
        by_regime.setdefault(regime, []).append(r)
    total_abs = sum(abs(v) for v in by_symbol.values()) or 1.0
    ranked = sorted(by_symbol.items(), key=lambda kv: abs(kv[1]), reverse=True)
    top5 = ranked[:5]
    top5_share = (sum(abs(v) for _, v in top5) / total_abs) if ranked else 0.0
    # Rough exposure: concurrent open count is not reconstructed here; report
    # closed-trade count and unique symbols as a proxy, plus open rows if present.
    opens = [r for r in rows if not is_closed(r)]
    return {
        "unique_symbols": len(by_symbol),
        "closed_trades": len(closed),
        "open_trades": len(opens),
        "top5_symbols": [{"symbol": s, "net_pnl": n} for s, n in top5],
        "top5_abs_pnl_share": top5_share,
        "side_breakdown": {k: _block(v) for k, v in sorted(by_side.items())},
        "regime_breakdown": {k: _block(v) for k, v in sorted(by_regime.items())},
    }


def ablation_vs_baseline(
    trades: Sequence[Dict[str, Any]],
    *,
    baseline: str = "v1_equivalent",
    timeframe: str = "all",
) -> List[Dict[str, Any]]:
    """Per-variant roll-up vs v1_equivalent for the selected timeframe (all months)."""
    view = filter_rows(trades, timeframe=timeframe, month="all", variant="any")
    by_variant: Dict[str, List[Dict[str, Any]]] = {}
    for r in view:
        by_variant.setdefault(str(r.get("variant") or ""), []).append(r)
    base_rows = by_variant.get(baseline) or []
    base = _block(base_rows)
    order = list(ABLATION_VARIANTS)
    extra = sorted(k for k in by_variant if k and k not in order)
    rows = []
    for tag in order + extra:
        group = by_variant.get(tag) or []
        if not group and tag != baseline:
            continue
        block = _block(group)
        open_info = _open_block(group)
        d_pf = None
        if (
            isinstance(base.get("profit_factor"), (int, float))
            and isinstance(block.get("profit_factor"), (int, float))
            and base["profit_factor"] != float("inf")
            and block["profit_factor"] != float("inf")
        ):
            d_pf = block["profit_factor"] - base["profit_factor"]
        d_r = None
        if isinstance(base.get("avg_r"), (int, float)) and isinstance(block.get("avg_r"), (int, float)):
            d_r = block["avg_r"] - base["avg_r"]
        d_wr_pp = None
        if isinstance(base.get("win_rate"), (int, float)) and isinstance(block.get("win_rate"), (int, float)):
            d_wr_pp = (block["win_rate"] - base["win_rate"]) * 100.0
        rows.append(
            {
                "variant": tag,
                "timeframe": timeframe,
                "trades": block["trades"],
                "net_pnl": block["net_pnl"],
                "win_rate": block["win_rate"],
                "profit_factor": block["profit_factor"],
                "avg_pnl": block["avg_pnl"],
                "avg_r": block["avg_r"],
                "open_trades": open_info["open_trades"],
                "mtm_pnl_sum": open_info["mtm_pnl_sum"],
                "delta_pf_vs_baseline": d_pf,
                "delta_avg_r_vs_baseline": d_r,
                "delta_wr_pp_vs_baseline": d_wr_pp,
                "delta_net_vs_baseline": (
                    block["net_pnl"] - base["net_pnl"]
                    if base["trades"] or block["trades"]
                    else None
                ),
                "baseline": baseline,
            }
        )
    return rows


def build_v2_dashboard(
    trades: Sequence[Dict[str, Any]],
    discards: Optional[Sequence[Dict[str, Any]]] = None,
    *,
    timeframe: str = "all",
    month: str = "all",
    variant: str = "all",
    v1_trades: Optional[Sequence[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """v2 Experiment dashboard with like-for-like v1 comparison.

    Like-for-like: same timeframe, same month scope, closed-trade metrics.
    OPEN counts / MTM are reported separately and do not enter closed P&L.
    """
    view = filter_rows(trades, timeframe=timeframe, month=month, variant=variant)
    dash = build_dashboard(view, timeframe="all")  # already filtered
    if month and month not in ("all", "full", ""):
        dash["months"] = [m for m in dash["months"] if m.get("month") == month]
    # else keep full 12-month table
    disc = filter_rows(discards or [], timeframe=timeframe, month=month, variant=variant)
    funnel = build_funnel(disc, view)
    scope_months = (
        [month]
        if month not in ("all", "full", "")
        else [lab for _, _, lab in month_windows()]
    )
    scope_label = (
        f"TF={timeframe}; months={month if month not in ('all', 'full', '') else 'Full (Nov25–Oct26)'}; "
        f"variant={variant}; closed vs closed (OPEN/MTM separate)"
    )
    comparison = None
    month_compare: List[Dict[str, Any]] = []
    if v1_trades is not None:
        v1_view = filter_rows(v1_trades, timeframe=timeframe, month=month, variant="any")
        # v1 has no variant column — month/tf filter only.
        if month not in ("all", "full", ""):
            v1_view = [r for r in v1_trades if str(r.get("month") or "") == month]
            if timeframe and timeframe != "all":
                v1_view = [r for r in v1_view if str(r.get("timeframe") or "") == timeframe]
        else:
            v1_view = filter_rows(v1_trades, timeframe=timeframe, month="all", variant="any")
        v1_dash = build_dashboard(v1_view, timeframe="all")
        comparison = compare_summaries(v1_dash["summary"], dash["summary"])
        comparison["scope"] = scope_label
        comparison["scope_months"] = scope_months
        month_compare = _month_compare(v1_dash["months"], dash["months"])
    baseline_equity = []
    if variant != "v1_equivalent":
        base_view = filter_rows(trades, timeframe=timeframe, month=month, variant="v1_equivalent")
        baseline_equity = _equity_curve(base_view)
    selected_equity = dash["charts"]["equity"]
    return {
        "timeframe": timeframe,
        "month": month,
        "variant": variant,
        "scope": scope_label,
        "summary": dash["summary"],
        "months": dash["months"],
        "month_compare": month_compare,
        "charts": {
            **dash["charts"],
            "equity_selected": selected_equity,
            "equity_v1_equivalent": baseline_equity,
        },
        "open_trades": dash["open_trades"],
        "open_mtm": dash.get("open_mtm") or {},
        "funnel": funnel,
        "comparison": comparison,
        "robustness": robustness_block(view),
        "ablation_vs_baseline": ablation_vs_baseline(trades, timeframe=timeframe),
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
                "avg_r": block["avg_r"],
                "open_trades": open_info["open_trades"],
                "mtm_pnl_sum": open_info["mtm_pnl_sum"],
            }
        )
    return rows


def core_trade_signature(row: Dict[str, Any]) -> Tuple:
    """Comparable identity for v1 vs v1_equivalent (ignores v2-only columns / id suffix)."""
    return (
        str(row.get("timeframe") or ""),
        str(row.get("symbol") or ""),
        str(row.get("side") or ""),
        str(row.get("entry_time") or ""),
        str(row.get("exit_time") or ""),
        str(row.get("exit_reason") or ""),
        str(row.get("entry_price") or ""),
        str(row.get("exit_price") or ""),
        str(row.get("net_pnl") or ""),
        str(row.get("month") or ""),
    )


def compare_v1_equivalent(
    v1_rows: Sequence[Dict[str, Any]],
    v2_rows: Sequence[Dict[str, Any]],
    *,
    timeframe: str = "all",
) -> Dict[str, Any]:
    """Validate v1_equivalent against v1 for a timeframe (or all)."""
    v1 = filter_rows(v1_rows, timeframe=timeframe)
    v2 = [
        r
        for r in filter_rows(v2_rows, timeframe=timeframe, variant="v1_equivalent")
    ]
    sig_v1 = sorted(core_trade_signature(r) for r in v1)
    sig_v2 = sorted(core_trade_signature(r) for r in v2)
    only_v1 = [s for s in sig_v1 if s not in sig_v2]
    only_v2 = [s for s in sig_v2 if s not in sig_v1]
    b1 = _block(v1)
    b2 = _block(v2)
    return {
        "timeframe": timeframe,
        "v1_trades": len(v1),
        "v2_trades": len(v2),
        "v1_closed": b1["trades"],
        "v2_closed": b2["trades"],
        "v1_net_pnl": b1["net_pnl"],
        "v2_net_pnl": b2["net_pnl"],
        "net_pnl_match": abs((b1["net_pnl"] or 0) - (b2["net_pnl"] or 0)) < 0.01,
        "count_match": len(v1) == len(v2),
        "signature_match": sig_v1 == sig_v2,
        "only_v1_count": len(only_v1),
        "only_v2_count": len(only_v2),
        "only_v1_sample": only_v1[:10],
        "only_v2_sample": only_v2[:10],
        "ok": sig_v1 == sig_v2 and abs((b1["net_pnl"] or 0) - (b2["net_pnl"] or 0)) < 0.01,
    }

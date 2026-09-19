"""Trade Report metrics — Forward test, Live, and All (never silently blended)."""
from __future__ import annotations

import csv
import io
import math
from typing import Any, Dict, List, Optional

from backend.services.tarang.labels import (
    CSV_FILL_SOURCE,
    CSV_MODE,
    CSV_RECORD_TYPE,
    fill_source_label,
    record_type_label,
)
from backend.services.tarang.lifecycle import list_closed_trades


def _safe(n: Optional[float], default: float = 0.0) -> float:
    try:
        if n is None or (isinstance(n, float) and math.isnan(n)):
            return default
        return float(n)
    except (TypeError, ValueError):
        return default


def _record_type(t: Dict[str, Any]) -> str:
    rt = str(t.get("record_type") or "").upper()
    if rt:
        return rt
    return "LIVE" if str(t.get("mode") or "").upper() == "LIVE" else "FORWARD_TEST"


def compute_metrics(trades: List[Dict[str, Any]]) -> Dict[str, Any]:
    pnls = [_safe(t.get("net_pnl")) for t in trades]
    empty = {
        "count": 0,
        "independent_cycles": 0,
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
        "by_underlying": {},
        "by_holding_mode": {"INTRADAY": {"count": 0}, "POSITIONAL": {"count": 0}},
        "by_origin": {"AUTO": {"count": 0}, "USER": {"count": 0}},
    }
    if not pnls:
        return empty
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p < 0]
    gross_wins = sum(wins)
    gross_losses = abs(sum(losses))
    pf = (gross_wins / gross_losses) if gross_losses > 0 else (None if not wins else float("inf"))
    win_rate = len(wins) / len(pnls) if pnls else None
    avg_win = (sum(wins) / len(wins)) if wins else None
    avg_loss = (sum(losses) / len(losses)) if losses else None
    expectancy = (sum(pnls) / len(pnls)) if pnls else None

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
    by_und: Dict[str, Dict[str, Any]] = {}
    by_holding: Dict[str, List[Dict[str, Any]]] = {"INTRADAY": [], "POSITIONAL": []}
    keys = set()
    for t, p in zip(trades, pnls):
        er = t.get("exit_reason") or "UNKNOWN"
        by_reason[er] = by_reason.get(er, 0) + 1
        pid = t.get("profile_id") or "?"
        bucket = by_profile.setdefault(pid, {"count": 0, "net": 0.0})
        bucket["count"] += 1
        bucket["net"] += p
        und = (t.get("meta") or {}).get("underlying") or t.get("profile_id") or "?"
        ub = by_und.setdefault(str(und), {"count": 0, "net": 0.0})
        ub["count"] += 1
        ub["net"] += p
        hm = str(t.get("holding_mode") or "INTRADAY").upper()
        by_holding.setdefault(hm, []).append(t)
        if t.get("signal_key"):
            keys.add(t["signal_key"])
        else:
            keys.add(f"{t.get('id')}")

    def _slice_metrics(subset: List[Dict[str, Any]]) -> Dict[str, Any]:
        sp = [_safe(t.get("net_pnl")) for t in subset]
        if not sp:
            return {"count": 0, "net_total": 0.0, "win_rate": None}
        wins_s = [x for x in sp if x > 0]
        return {
            "count": len(sp),
            "net_total": sum(sp),
            "win_rate": (len(wins_s) / len(sp)) if sp else None,
            "avg_win": (sum(wins_s) / len(wins_s)) if wins_s else None,
            "avg_loss": (sum([x for x in sp if x < 0]) / max(len([x for x in sp if x < 0]), 1))
            if any(x < 0 for x in sp)
            else None,
        }

    holding_metrics = {k: _slice_metrics(v) for k, v in by_holding.items()}
    by_origin: Dict[str, List[Dict[str, Any]]] = {"AUTO": [], "USER": [], "BACKTEST": []}
    for t in trades:
        orig = str(t.get("origin") or ("AUTO" if t.get("auto_managed") else "USER")).upper()
        by_origin.setdefault(orig, []).append(t)
    origin_metrics = {k: _slice_metrics(v) for k, v in by_origin.items()}

    return {
        "count": len(pnls),
        "independent_cycles": len(keys),
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
        "by_underlying": by_und,
        "by_holding_mode": holding_metrics,
        "by_origin": origin_metrics,
    }


def _pair_metric(name: str, ft: Any, live: Any) -> Dict[str, Any]:
    return {"metric": name, "forward_test": ft, "live": live}


def build_report(
    *,
    mode: str = "PAPER",
    profile_id: Optional[str] = None,
    limit: int = 200,
    book: Optional[str] = None,
) -> Dict[str, Any]:
    all_closed = list_closed_trades(mode=None, limit=max(limit, 500))
    if profile_id:
        all_closed = [t for t in all_closed if t.get("profile_id") == profile_id.upper()]
    ft = [t for t in all_closed if _record_type(t) == "FORWARD_TEST"]
    live = [t for t in all_closed if _record_type(t) == "LIVE"]
    book_u = str(book or mode or "FORWARD_TEST").upper()
    if book_u in ("PAPER", "FORWARD_TEST", "FT"):
        book_u = "FORWARD_TEST"
        trades = ft
        metrics = compute_metrics(ft)
        blended = False
    elif book_u == "LIVE":
        trades = live
        metrics = compute_metrics(live)
        blended = False
    else:
        book_u = "ALL"
        trades = all_closed
        metrics = None
        blended = False

    ft_m = compute_metrics(ft)
    live_m = compute_metrics(live)
    all_view = None
    if book_u == "ALL":
        keys = [
            "count",
            "independent_cycles",
            "win_rate",
            "profit_factor",
            "expectancy",
            "max_drawdown",
            "worst_trade",
            "net_total",
        ]
        all_view = [_pair_metric(k, ft_m.get(k), live_m.get(k)) for k in keys]
        metrics = {
            "blended": False,
            "note": "All view lists Forward test and Live counts beside every metric — never a single blended number.",
            "forward_test": ft_m,
            "live": live_m,
            "paired": all_view,
        }

    return {
        "product": "Kosmic Tarang",
        "phase": 3,
        "mode": mode,
        "book": book_u,
        "display_book": "Forward test" if book_u == "FORWARD_TEST" else ("Live" if book_u == "LIVE" else "All"),
        "trades": trades,
        "metrics": metrics,
        "metrics_forward_test": ft_m,
        "metrics_live": live_m,
        "all_paired": all_view,
        "blended": blended,
        "display_auto": "Auto orders: locked",
        "note": "Live book is user-entered broker fills only. The system never places orders.",
    }


def report_csv(mode: str = "PAPER", limit: int = 500, book: Optional[str] = None) -> str:
    payload = build_report(mode=mode, limit=limit, book=book)
    trades = payload.get("trades") or []
    buf = io.StringIO()
    fields = [
        "id",
        CSV_RECORD_TYPE,
        CSV_FILL_SOURCE,
        CSV_MODE,
        "profile_id",
        "structure",
        "holding_mode",
        "status",
        "origin",
        "auto_managed",
        "entry_at",
        "exit_at",
        "exit_reason",
        "entry_credit",
        "gross_pnl",
        "net_pnl",
        "fees_total",
        "lots_or_contracts",
        "venue",
        "signal_key",
    ]
    w = csv.DictWriter(buf, fieldnames=fields, extrasaction="ignore")
    w.writeheader()
    for t in trades:
        w.writerow(
            {
                "id": t.get("id"),
                CSV_RECORD_TYPE: record_type_label(_record_type(t)),
                CSV_FILL_SOURCE: fill_source_label(t.get("fill_source")),
                CSV_MODE: record_type_label(_record_type(t)),
                "profile_id": t.get("profile_id"),
                "structure": t.get("structure"),
                "holding_mode": t.get("holding_mode"),
                "status": t.get("status"),
                "origin": t.get("origin"),
                "auto_managed": t.get("auto_managed"),
                "entry_at": t.get("entry_at"),
                "exit_at": t.get("exit_at"),
                "exit_reason": t.get("exit_reason"),
                "entry_credit": t.get("entry_credit"),
                "gross_pnl": t.get("gross_pnl"),
                "net_pnl": t.get("net_pnl"),
                "fees_total": t.get("fees_total"),
                "lots_or_contracts": t.get("lots_or_contracts"),
                "venue": t.get("venue"),
                "signal_key": t.get("signal_key"),
            }
        )
    return buf.getvalue()

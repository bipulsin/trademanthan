"""Pure helpers for CommDiv History LIVE Overall PnL and month grouping.

Mirrors frontend/public/commDiv.js (execMonthKey / sumPnl / groupRowsByMonth).
Timestamps are IST strings like YYYY-MM-DD HH:MM:SS.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional


_MONTHS = (
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
)


def exec_month_key(row: Dict[str, Any]) -> str:
    """YYYY-MM from exit (completed) time; fall back to entry. Unknown → '—'."""
    dt = row.get("exit_at") or row.get("exit_submitted_at") or row.get("trade_taken_at") or ""
    s = str(dt).strip().replace("T", " ")
    if len(s) >= 7 and s[4] == "-" and s[0:4].isdigit() and s[5:7].isdigit():
        return s[:7]
    return "—"


def format_month_label(ym: str) -> str:
    if not ym or ym == "—":
        return "Date unknown"
    parts = str(ym).split("-")
    if len(parts) < 2:
        return ym
    try:
        m = int(parts[1])
    except ValueError:
        return ym
    if m < 1 or m > 12:
        return ym
    return f"{_MONTHS[m - 1]} {parts[0]}"


def sum_pnl(rows: List[Dict[str, Any]]) -> Optional[float]:
    total = 0.0
    any_val = False
    for r in rows or []:
        pnl = r.get("pnl") if isinstance(r, dict) else None
        if pnl is None or pnl == "":
            continue
        try:
            n = float(pnl)
        except (TypeError, ValueError):
            continue
        total += n
        any_val = True
    if not any_val:
        return None
    return round(total, 2)


def group_rows_by_month(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    groups: Dict[str, List[Dict[str, Any]]] = {}
    order: List[str] = []
    for r in rows or []:
        k = exec_month_key(r)
        if k not in groups:
            groups[k] = []
            order.append(k)
        groups[k].append(r)

    known = [k for k in order if k != "—"]
    known.sort(reverse=True)
    if "—" in groups:
        known.append("—")
    return [
        {
            "key": k,
            "label": format_month_label(k),
            "rows": groups[k],
            "pnl": sum_pnl(groups[k]),
        }
        for k in known
    ]


def overall_live_pnl(history_rows: List[Dict[str, Any]]) -> Optional[float]:
    """Sum PnL for LIVE History rows only."""
    live = [
        r
        for r in (history_rows or [])
        if str((r or {}).get("trade_mode") or "PAPER").strip().upper() == "LIVE"
    ]
    return sum_pnl(live)

"""Pure helpers for CommDiv History LIVE Overall PnL and month grouping.

Mirrors frontend/public/commDiv.js (execMonthKey / rowPnl / sumPnl / groupRowsByMonth).
Timestamps are IST strings like YYYY-MM-DD HH:MM:SS.

PnL single source of truth: recompute from entry/exit × lot when prices+lot
are known; otherwise fall back to the stored ``pnl`` field.
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


def _lot_for_pnl(row: Dict[str, Any]) -> Optional[int]:
    """Prefer lot_size / lot_qty when a positive lot is known."""
    for key in ("lot_size", "lot_qty", "qty"):
        raw = row.get(key)
        if raw is None or raw == "":
            continue
        try:
            lot = int(raw)
        except (TypeError, ValueError):
            continue
        if lot > 0:
            return lot
    return None


def row_pnl(row: Optional[Dict[str, Any]]) -> Optional[float]:
    """
    Per-trade ₹ PnL.

    Prefer (exit − entry) × lot for BULL / (entry − exit) × lot for BEAR when
    entry, exit (or mark), and lot are known. Else stored ``pnl``.
    """
    if not isinstance(row, dict):
        return None

    entry_raw = row.get("entry_price")
    mark_raw = row.get("exit_price")
    if mark_raw is None or mark_raw == "":
        mark_raw = row.get("ltp")
    lot = _lot_for_pnl(row)

    try:
        entry = float(entry_raw) if entry_raw is not None and entry_raw != "" else None
        mark = float(mark_raw) if mark_raw is not None and mark_raw != "" else None
    except (TypeError, ValueError):
        entry = None
        mark = None

    if entry is not None and mark is not None and lot is not None:
        dir_u = str(row.get("direction") or "").strip().upper()
        if dir_u in ("BEAR", "SHORT", "SELL"):
            return round((entry - mark) * lot, 2)
        return round((mark - entry) * lot, 2)

    pnl = row.get("pnl")
    if pnl is None or pnl == "":
        return None
    try:
        return round(float(pnl), 2)
    except (TypeError, ValueError):
        return None


def sum_pnl(rows: List[Dict[str, Any]]) -> Optional[float]:
    total = 0.0
    any_val = False
    for r in rows or []:
        n = row_pnl(r if isinstance(r, dict) else None)
        if n is None:
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
    """Sum PnL for LIVE History rows only (PAPER excluded)."""
    live = [
        r
        for r in (history_rows or [])
        if str((r or {}).get("trade_mode") or "PAPER").strip().upper() == "LIVE"
    ]
    return sum_pnl(live)

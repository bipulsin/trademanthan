"""Ranking layer — sort by trade quality state then execution score."""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from backend.services.vajra.trade_quality import STATE_EXECUTABLE, STATE_REJECT, STATE_WATCHLIST

_STATE_RANK = {
    STATE_EXECUTABLE: 3,
    STATE_WATCHLIST: 2,
    STATE_REJECT: 1,
}


def entry_state_sort_rank(entry_state: Optional[str]) -> int:
    s = (entry_state or "").strip().upper()
    if s in _STATE_RANK:
        return _STATE_RANK[s]
    if "EXECUTABLE" in s:
        return 3
    if "WATCH" in s:
        return 2
    if "REJECT" in s or "AVOID" in s:
        return 1
    return 0


def sort_vajra_rows_for_display(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """EXECUTABLE → WATCHLIST → REJECT; within band by trade_quality_score desc."""

    def _f(r: Dict[str, Any], key: str) -> float:
        v = r.get(key)
        if v is None:
            return 0.0
        try:
            return float(v)
        except (TypeError, ValueError):
            return 0.0

    def _key(r: Dict[str, Any]) -> tuple:
        tq = _f(r, "trade_quality_score") or _f(r, "confidence")
        sym = str(r.get("security") or r.get("stock") or "")
        return (-entry_state_sort_rank(r.get("entry_state")), -tq, sym)

    return sorted(rows, key=_key)


def shortlist_by_trade_quality(
    candidates: List[Dict[str, Any]],
    *,
    min_count: int = 5,
    max_count: int = 15,
) -> List[Dict[str, Any]]:
    """Shortlist strongest setups for 5m validation (not TPS-only)."""
    if not candidates:
        return []
    ranked = sort_vajra_rows_for_display(candidates)
    non_reject = [r for r in ranked if entry_state_sort_rank(r.get("entry_state")) >= 2]
    pool = non_reject if len(non_reject) >= min_count else ranked
    alert_rows = [r for r in pool if r.get("_early") or r.get("alertable")]
    core = [r for r in pool if r not in alert_rows]
    n_core = max(0, min(max_count - len(alert_rows), len(core)))
    picked = alert_rows + core[:n_core]
    seen: set = set()
    out: List[Dict[str, Any]] = []
    for r in picked:
        key = r.get("stock") or r.get("security")
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
        if len(out) >= min_count:
            break
    return out[: max_count + len(alert_rows)]

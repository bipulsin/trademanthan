"""Ranking layer — execution rank + Top 8 tiers from trade_state."""
from __future__ import annotations

from typing import Any, Dict, List

from backend.services.vajra.market_phase_scoring import enrich_execution_scores, select_top_picks
from backend.services.vajra.trade_state import STATE_EXECUTABLE, STATE_REJECT, STATE_WATCHLIST
from backend.services.vajra.ui_mapping import finalize_screener_rows

_STATE_RANK = {
    STATE_EXECUTABLE: 3,
    STATE_WATCHLIST: 2,
    STATE_REJECT: 1,
}


def entry_state_sort_rank(entry_state: Any) -> int:
    s = (entry_state or "").strip().upper() if entry_state else ""
    if s in _STATE_RANK:
        return _STATE_RANK[s]
    if "EXECUTABLE" in s:
        return 3
    if "WATCH" in s:
        return 2
    if "REJECT" in s or "AVOID" in s:
        return 1
    return 0


def _qualification(row: Dict[str, Any]) -> str:
    return str(row.get("qualification_state") or row.get("qualification") or "").strip().upper()


def sort_vajra_rows_for_display(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    def _key(r: Dict[str, Any]) -> tuple:
        sym = str(r.get("security") or r.get("stock") or "")
        return (
            -float(r.get("execution_rank_score") or 0),
            -entry_state_sort_rank(_qualification(r)),
            -float(r.get("market_phase_score") or 0),
            sym,
        )

    return sorted(rows, key=_key)


def group_by_qualification(rows: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    ranked = sort_vajra_rows_for_display(rows)
    groups: Dict[str, List[Dict[str, Any]]] = {
        STATE_EXECUTABLE: [],
        STATE_WATCHLIST: [],
        STATE_REJECT: [],
    }
    for r in ranked:
        q = _qualification(r)
        if q == STATE_EXECUTABLE:
            groups[STATE_EXECUTABLE].append(r)
        elif q == STATE_REJECT:
            groups[STATE_REJECT].append(r)
        else:
            groups[STATE_WATCHLIST].append(r)
    return groups


def build_screener_display(rows: List[Dict[str, Any]], top_n: int = 8) -> Dict[str, Any]:
    enriched = [enrich_execution_scores(dict(r)) for r in rows]
    finalized = finalize_screener_rows(enriched)
    sorted_rows = sort_vajra_rows_for_display(finalized)
    groups = group_by_qualification(sorted_rows)
    top_picks, top_sections = select_top_picks(sorted_rows, n=top_n)
    top_keys = {(r.get("stock") or r.get("security")) for r in top_picks}
    remainder = [
        r for r in sorted_rows if (r.get("stock") or r.get("security")) not in top_keys
    ]
    return {
        "rows": sorted_rows,
        "groups": groups,
        "top_picks": top_picks,
        "top_sections": top_sections,
        "remainder": remainder,
    }


def shortlist_by_trade_quality(
    candidates: List[Dict[str, Any]],
    *,
    min_count: int = 5,
    max_count: int = 15,
) -> List[Dict[str, Any]]:
    if not candidates:
        return []
    ranked = sort_vajra_rows_for_display(candidates)
    non_reject = [r for r in ranked if entry_state_sort_rank(_qualification(r)) >= 2]
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

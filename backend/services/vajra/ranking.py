"""Ranking layer — sectional screener + tier-aware sort."""
from __future__ import annotations

from typing import Any, Dict, List

from backend.services.vajra.market_phase_scoring import enrich_execution_scores, select_top_picks
from backend.services.vajra.qualification_config import (
    STATE_ARMED,
    STATE_DISCOVERY,
    STATE_EXECUTABLE,
    STATE_REJECT,
    STATE_WATCHLIST,
)
from backend.services.vajra.screener_sections import build_screener_sections
from backend.services.vajra.ui_mapping import finalize_screener_rows

_STATE_RANK = {
    STATE_EXECUTABLE: 4,
    STATE_ARMED: 3,
    STATE_DISCOVERY: 2,
    STATE_WATCHLIST: 2,
    STATE_REJECT: 1,
}


def entry_state_sort_rank(entry_state: Any) -> int:
    s = (entry_state or "").strip().upper() if entry_state else ""
    if s in _STATE_RANK:
        return _STATE_RANK[s]
    if "EXECUTABLE" in s:
        return 4
    if "ARMED" in s:
        return 3
    if "DISCOVERY" in s or "WATCH" in s or "MONITOR" in s:
        return 2
    if "REJECT" in s or "AVOID" in s:
        return 1
    return 0


def _qualification(row: Dict[str, Any]) -> str:
    return str(
        row.get("qualification_state") or row.get("qualification") or row.get("entry_state") or ""
    ).strip().upper()


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
        STATE_ARMED: [],
        STATE_DISCOVERY: [],
        STATE_WATCHLIST: [],
        STATE_REJECT: [],
    }
    for r in ranked:
        q = _qualification(r)
        if q == STATE_EXECUTABLE:
            groups[STATE_EXECUTABLE].append(r)
        elif q == STATE_ARMED:
            groups[STATE_ARMED].append(r)
        elif q == STATE_DISCOVERY:
            groups[STATE_DISCOVERY].append(r)
        elif q == STATE_REJECT:
            groups[STATE_REJECT].append(r)
        elif q == STATE_WATCHLIST:
            groups[STATE_WATCHLIST].append(r)
            groups[STATE_ARMED].append(r)
        else:
            groups[STATE_DISCOVERY].append(r)
    return groups


def _filter_screener_grades(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    from backend.services.vajra.setup_classifier import quality_grade, screener_grade_allowed

    out: List[Dict[str, Any]] = []
    for r in rows:
        row = dict(r)
        if not row.get("quality_grade"):
            row["quality_grade"] = quality_grade(row)
        if screener_grade_allowed(row):
            out.append(row)
    return out


_GRADE_SORT_RANK = {"A+": 6, "A": 5, "B+": 4, "B": 3, "C": 2, "—": 0, "": 0}


def grade_sort_rank(grade: Any) -> int:
    g = str(grade or "").strip().upper()
    return _GRADE_SORT_RANK.get(g, 1)


def _stock_key(row: Dict[str, Any]) -> str:
    return str(row.get("stock") or row.get("security") or "").strip().upper()


def _placeholder_universe_row(item: Dict[str, str]) -> Dict[str, Any]:
    stock = str(item.get("stock") or "").strip()
    fut_sym = str(item.get("future_symbol") or "").strip()
    return {
        "stock": stock,
        "security": fut_sym or stock,
        "future_symbol": fut_sym,
        "instrument_key": item.get("instrument_key"),
        "qualification_state": STATE_REJECT,
        "entry_state": STATE_REJECT,
        "qualification": STATE_REJECT,
        "trade_type": "—",
        "confidence": 0,
        "quality_grade": "C",
        "pipeline_stage": "unrated",
        "execution_validated": False,
        "enter_action": "",
        "enter_enabled": False,
    }


def _minimal_modal_placeholder_row(item: Dict[str, str]) -> Dict[str, Any]:
    """Light row for symbols without a scan — avoids heavy sticky enrich on 200+ names."""
    row = _placeholder_universe_row(item)
    row.update(
        {
            "executable_score": 0.0,
            "freshness_score": 0.0,
            "extension_risk_display": 50.0,
            "momentum_velocity": 0.0,
            "score_trend": "→",
            "chase_risk": "MEDIUM",
            "setup_trend": "stable",
            "execution_bias": "LONG",
            "sticky_rank_score": 0.0,
        }
    )
    return row


def build_universe_modal_rows(
    rated_rows: List[Dict[str, Any]],
    universe: List[Dict[str, str]],
) -> List[Dict[str, Any]]:
    """
    Full arbitrage_master curr-month universe for the More modal — all symbols,
    rated rows merged with placeholders for symbols without a scan row.
    Rated rows are reused as-is (already enriched by stable_execution overlay).
    """
    from backend.services.vajra.setup_classifier import quality_grade

    by_stock: Dict[str, Dict[str, Any]] = {}
    for r in rated_rows:
        key = _stock_key(r)
        if key:
            by_stock[key] = dict(r)

    out: List[Dict[str, Any]] = []
    for item in universe:
        key = str(item.get("stock") or "").strip().upper()
        if not key:
            continue
        if key in by_stock:
            row = dict(by_stock[key])
            if not row.get("quality_grade"):
                row["quality_grade"] = quality_grade(row)
        else:
            row = _minimal_modal_placeholder_row(item)
        out.append(row)

    out.sort(
        key=lambda r: (
            -grade_sort_rank(r.get("quality_grade")),
            -entry_state_sort_rank(_qualification(r)),
            -float(r.get("executable_score") or 0),
            _stock_key(r),
        )
    )
    return out


def build_screener_display(rows: List[Dict[str, Any]], top_n: int = 8) -> Dict[str, Any]:
    enriched = [enrich_execution_scores(dict(r)) for r in rows]
    finalized = finalize_screener_rows(enriched)
    graded = _filter_screener_grades(finalized)
    sorted_rows = sort_vajra_rows_for_display(graded)
    groups = group_by_qualification(sorted_rows)
    section_out = build_screener_sections(sorted_rows, limits={
        STATE_EXECUTABLE: top_n,
        STATE_ARMED: top_n,
        STATE_DISCOVERY: top_n,
    })
    top_picks = section_out["top_picks"]
    top_sections = section_out["top_sections"]
    top_keys = {(r.get("stock") or r.get("security")) for r in top_picks}
    top_keys.update(
        (r.get("stock") or r.get("security"))
        for tier in (STATE_ARMED, STATE_DISCOVERY)
        for r in top_sections.get(tier, [])
    )
    remainder = [
        r for r in sorted_rows if (r.get("stock") or r.get("security")) not in top_keys
    ]
    return {
        "rows": sorted_rows,
        "groups": groups,
        "top_picks": top_picks,
        "top_sections": top_sections,
        "sections": section_out["sections"],
        "banner": section_out["banner"],
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

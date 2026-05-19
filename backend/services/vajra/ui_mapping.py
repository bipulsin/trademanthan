"""UI mapping — thin layer; all logic lives in trade_state."""
from __future__ import annotations

from typing import Any, Dict, List

from backend.services.vajra.market_phase_scoring import enrich_execution_scores
from backend.services.vajra.trade_state import (
    STATE_EXECUTABLE,
    STATE_REJECT,
    STATE_WATCHLIST,
    derive_structural_bias,
    resolve_market_phase,
)


def normalize_qualification(entry_state: str | None) -> str:
    s = (entry_state or STATE_WATCHLIST).strip().upper()
    if s == STATE_EXECUTABLE or "EXECUTABLE" in s:
        return STATE_EXECUTABLE
    if s == STATE_REJECT or "REJECT" in s or "AVOID" in s:
        return STATE_REJECT
    return STATE_WATCHLIST


def finalize_screener_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Ensure unified trade-state fields exist for API/UI consumption."""
    enrich_execution_scores(row)
    if row.get("ees_score") is not None:
        row["setup_potential_score"] = row["ees_score"]
    qual = normalize_qualification(
        row.get("qualification_state") or row.get("qualification") or row.get("entry_state")
    )
    row["qualification"] = qual
    row["qualification_state"] = qual
    row["entry_state"] = qual
    mp = row.get("market_phase") or row.get("market_context")
    if mp:
        row["market_context"] = mp
        row["market_phase"] = mp
    eb = row.get("execution_bias") or row.get("direction")
    if eb:
        row["execution_bias"] = eb
        row["direction"] = eb
    tags = row.get("reason_tags") or row.get("qualification_tags")
    if tags:
        row["qualification_tags"] = tags
        row["reason_tags"] = tags
    if qual == STATE_EXECUTABLE:
        row["enter_action"] = "ENTER"
        row["enter_enabled"] = True
        row["action"] = "ENTER"
    elif qual == STATE_WATCHLIST:
        row["enter_action"] = "WATCH"
        row["enter_enabled"] = False
        row["action"] = "WATCH"
    else:
        row["enter_action"] = ""
        row["enter_enabled"] = False
        row["action"] = ""
    mp = row.get("market_phase") or resolve_market_phase(row)
    from backend.services.vajra.trade_state import (
        compute_directional_scores,
        directional_confidence_label,
        has_directional_conviction,
        resolve_execution_direction,
    )

    row["structural_bias"] = row.get("structural_bias") or derive_structural_bias(row)
    qual_pre = normalize_qualification(
        row.get("qualification_state") or row.get("qualification") or row.get("entry_state")
    )
    allow_neutral = qual_pre == STATE_REJECT
    row["execution_bias"] = resolve_execution_direction(row, mp, allow_neutral=allow_neutral)
    if row["execution_bias"] == "NEUTRAL" and not allow_neutral:
        row["execution_bias"] = "LONG"
    row["direction"] = row["execution_bias"]
    ls, ss = compute_directional_scores(row, mp)
    row["directional_confidence"] = directional_confidence_label(row["execution_bias"], ls, ss)
    row["directional_conviction"] = has_directional_conviction(row, mp)
    return row


def finalize_screener_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [finalize_screener_row(dict(r)) for r in rows]

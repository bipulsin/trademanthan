"""UI mapping — v2 DISCOVERY / ARMED / EXECUTABLE / REJECT."""
from __future__ import annotations

from typing import Any, Dict, List

from backend.services.vajra.market_phase_scoring import enrich_execution_scores
from backend.services.vajra.qualification_config import (
    STATE_ARMED,
    STATE_DISCOVERY,
    STATE_EXECUTABLE,
    STATE_REJECT,
    STATE_WATCHLIST,
)
from backend.services.vajra.session_window import entry_disabled_message, is_vajra_entry_disabled_ist
from backend.services.vajra.trade_state import derive_structural_bias, resolve_market_phase


def normalize_qualification(entry_state: str | None) -> str:
    s = (entry_state or STATE_DISCOVERY).strip().upper()
    if s in (STATE_EXECUTABLE, STATE_ARMED, STATE_DISCOVERY, STATE_REJECT):
        return s
    if "EXECUTABLE" in s:
        return STATE_EXECUTABLE
    if "ARMED" in s:
        return STATE_ARMED
    if "DISCOVERY" in s or "MONITOR" in s:
        return STATE_DISCOVERY
    if s == STATE_REJECT or "REJECT" in s or "AVOID" in s:
        return STATE_REJECT
    if s == STATE_WATCHLIST or "WATCH" in s or "PULLBACK" in s:
        return STATE_ARMED
    return STATE_DISCOVERY


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
    row["qualification_stage"] = qual.lower()
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
    elif qual == STATE_ARMED:
        row["enter_action"] = row.get("enter_action") or "ARMED"
        row["enter_enabled"] = False
        row["action"] = "ARMED"
    elif qual == STATE_DISCOVERY:
        row["enter_action"] = row.get("enter_action") or "MONITOR"
        row["enter_enabled"] = False
        row["action"] = "MONITOR"
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
    if row["execution_bias"] == "NEUTRAL":
        from backend.services.vajra.trade_state import _f

        row["execution_bias"] = "LONG" if _f(row.get("bull_score")) >= _f(row.get("bear_score")) else "SHORT"
    row["direction"] = row["execution_bias"]
    ls, ss = compute_directional_scores(row, mp)
    row["directional_confidence"] = directional_confidence_label(row["execution_bias"], ls, ss)
    row["directional_conviction"] = has_directional_conviction(row, mp)
    if row.get("conviction_score") is None and row.get("confidence") is not None:
        row["conviction_score"] = row["confidence"]
    if row.get("enter_enabled") and is_vajra_entry_disabled_ist():
        row["enter_enabled"] = False
        row["enter_action"] = "CLOSED"
        row["enter_reason"] = entry_disabled_message()
        row["action"] = "CLOSED"
    return row


def finalize_screener_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [finalize_screener_row(dict(r)) for r in rows]

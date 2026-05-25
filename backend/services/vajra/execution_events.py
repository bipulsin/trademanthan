"""Event-driven execution alerts for Vajra Co-Pilot."""
from __future__ import annotations

from typing import Any, Dict, List

from backend.services.vajra.setup_classifier import (
    WF_EXECUTABLE,
    WF_EXIT_RISK,
    WF_PREPARE,
    WF_WAIT,
    classify_setup_type,
)


def _stock(row: Dict[str, Any]) -> str:
    return str(row.get("stock") or row.get("security") or "").strip().upper()


def build_row_execution_events(row: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Meaningful alerts for this symbol (not raw scanner noise)."""
    events: List[Dict[str, Any]] = []
    sym = _stock(row)
    if not sym:
        return events

    wf = str(row.get("execution_workflow_state") or "").upper()
    setup = classify_setup_type(row)
    prev_wf = str(row.get("prior_execution_workflow_state") or "").upper()

    if wf == WF_PREPARE and prev_wf == WF_WAIT:
        events.append(
            {
                "type": "PREPARE",
                "level": "info",
                "symbol": sym,
                "message": f"{sym}: {setup} approaching execution readiness.",
            }
        )
    elif wf == WF_EXECUTABLE and prev_wf in (WF_PREPARE, WF_WAIT):
        events.append(
            {
                "type": "EXECUTION",
                "level": "action",
                "symbol": sym,
                "message": f"{sym}: trigger conditions satisfied — review conditional trade plan.",
            }
        )
    elif wf == WF_EXECUTABLE:
        plan = row.get("trade_plan") or {}
        if plan.get("entry_condition"):
            events.append(
                {
                    "type": "EXECUTION",
                    "level": "action",
                    "symbol": sym,
                    "message": plan["entry_condition"],
                }
            )
    elif wf == WF_EXIT_RISK:
        events.append(
            {
                "type": "EXIT_RISK",
                "level": "risk",
                "symbol": sym,
                "message": f"{sym}: structure weakening or exhaustion risk — review invalidation rules.",
            }
        )

    for w in row.get("invalidation_signals") or []:
        events.append(
            {
                "type": "RISK",
                "level": "warning",
                "symbol": sym,
                "message": w,
            }
        )

    for a in row.get("ees_alerts") or []:
        if isinstance(a, str) and a.strip():
            events.append(
                {
                    "type": "SCANNER",
                    "level": "info",
                    "symbol": sym,
                    "message": a.strip(),
                }
            )

    return events[:6]


def aggregate_session_events(rows: List[Dict[str, Any]], limit: int = 12) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in rows or []:
        for ev in build_row_execution_events(r):
            out.append(ev)
            if len(out) >= limit:
                return out
    return out

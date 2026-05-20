"""UI action layer — MONITOR / ARMED / ENTER by qualification state."""
from __future__ import annotations

from typing import Any, Dict, Optional

from backend.services.vajra.qualification_config import (
    STATE_ARMED,
    STATE_DISCOVERY,
    STATE_EXECUTABLE,
    STATE_REJECT,
    STATE_WATCHLIST,
)


def resolve_enter_action(
    *,
    entry_state: Optional[str],
    confidence: Optional[float],
    reject_reasons: Optional[list] = None,
    blocker_label: Optional[str] = None,
) -> Dict[str, Any]:
    state = (entry_state or STATE_DISCOVERY).strip().upper()
    conf = float(confidence) if confidence is not None else 0.0
    reasons = reject_reasons or []

    if state == STATE_EXECUTABLE:
        return {
            "enter_action": "ENTER",
            "enter_enabled": True,
            "enter_reason": f"Executable — conviction {conf:.0f}",
        }

    if state == STATE_REJECT:
        hint = reasons[0].replace("_", " ") if reasons else "low quality setup"
        return {
            "enter_action": "",
            "enter_enabled": False,
            "enter_reason": f"Rejected: {hint}",
        }

    if state == STATE_ARMED:
        hint = blocker_label or "One trigger away from execution"
        return {
            "enter_action": "ARMED",
            "enter_enabled": False,
            "enter_reason": f"Armed — {hint}",
        }

    if state == STATE_DISCOVERY:
        return {
            "enter_action": "MONITOR",
            "enter_enabled": False,
            "enter_reason": "Discovery — institutional attention, not near trigger",
        }

    if state == STATE_WATCHLIST:
        return {
            "enter_action": "ARMED",
            "enter_enabled": False,
            "enter_reason": "Watchlist — setup forming, not executable yet",
        }

    return {
        "enter_action": "MONITOR",
        "enter_enabled": False,
        "enter_reason": "Monitoring",
    }

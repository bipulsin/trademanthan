"""UI action layer — ENTER only for qualified EXECUTABLE setups."""
from __future__ import annotations

from typing import Any, Dict, Optional

from backend.services.vajra.trade_quality import EXECUTABLE_CONFIDENCE_MIN, STATE_EXECUTABLE, STATE_REJECT, STATE_WATCHLIST


def resolve_enter_action(
    *,
    entry_state: Optional[str],
    confidence: Optional[float],
    reject_reasons: Optional[list] = None,
) -> Dict[str, Any]:
    """
    ENTER enabled only when state == EXECUTABLE and confidence >= 75.
    REJECT → no button label (disabled EXTENDED-style).
    WATCHLIST → WATCH.
    """
    state = (entry_state or STATE_WATCHLIST).strip().upper()
    conf = float(confidence) if confidence is not None else 0.0
    reasons = reject_reasons or []

    if state == STATE_EXECUTABLE:
        return {
            "enter_action": "ENTER",
            "enter_enabled": True,
            "enter_reason": f"Executable — trade quality {conf:.0f}",
        }

    if state == STATE_REJECT:
        hint = reasons[0].replace("_", " ") if reasons else "low quality setup"
        return {
            "enter_action": "",
            "enter_enabled": False,
            "enter_reason": f"Rejected: {hint}",
        }

    return {
        "enter_action": "WATCH",
        "enter_enabled": False,
        "enter_reason": "Watchlist — setup forming, not executable yet",
    }

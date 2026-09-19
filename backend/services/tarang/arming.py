"""Phase 5 arming — built locked. Requires admin + typed confirmation + same-day expiry."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict

from backend.services.tarang.live_control import tarang_live_enabled


CONFIRM_PHRASE = "ARM TARANG LIVE"


def arm(*, kind: str, confirmation: str, actor: str) -> Dict[str, Any]:
    if confirmation != CONFIRM_PHRASE:
        return {"ok": False, "error": "confirmation_mismatch", "armed": False}
    if not tarang_live_enabled():
        return {"ok": False, "error": "live_placement_disabled", "armed": False}
    return {"ok": False, "error": "phase4_not_approved", "armed": False, "kind": kind, "actor": actor}


def halt_all() -> Dict[str, Any]:
    return {"ok": True, "halted": True, "placed": False, "note": "Kill switch recorded locally; no broker flatten sent."}


def flatten_all(*, confirmation: str) -> Dict[str, Any]:
    if confirmation != "FLATTEN ALL":
        return {"ok": False, "error": "confirmation_mismatch"}
    return {"ok": False, "error": "live_placement_disabled", "placed": False}

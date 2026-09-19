"""Live-order fail-closed controls. Default: nothing is sent to a broker."""
from __future__ import annotations

import os
from typing import Any, Dict

from backend.services.tarang.config import get_risk


class LivePlacementDisabled(Exception):
    def __init__(self, reason: str = "live_placement_disabled"):
        super().__init__(reason)
        self.reason = reason


def tarang_live_enabled() -> bool:
    raw = (os.getenv("TARANG_LIVE_ENABLED") or "false").strip().lower()
    return raw in ("1", "true", "yes")


def phase4_approved() -> bool:
    hold = get_risk().get("phase4_hold") or {}
    return bool(hold.get("approved"))


def live_send_allowed() -> bool:
    """All gates must pass. Production defaults fail closed."""
    if not tarang_live_enabled():
        return False
    if not phase4_approved():
        return False
    return False  # arming + credentials + allowlist still required; never auto-true


def live_limits() -> Dict[str, Any]:
    risk = get_risk()
    live = dict(risk.get("live_orders") or {})
    live.setdefault("allowlist_symbols", [])
    live.setdefault("max_qty", 1)
    live.setdefault("max_order_value_inr", 5000)
    live.setdefault("price_band_frac", 0.05)
    live.setdefault("freeze_qty", {"upstox_mcx": 50, "delta_india": 4000})
    live.setdefault("order_tag", "tarang-")
    return live


def assert_can_place() -> None:
    raise LivePlacementDisabled("live_placement_disabled")

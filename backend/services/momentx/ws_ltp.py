"""MomentX live LTP via shared Upstox market feed (provider ``momentx``)."""
from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from backend.services.momentx import state as mx_state

logger = logging.getLogger(__name__)

PROVIDER_NAME = "momentx"


def sync_momentx_subscriptions(*, force: bool = False) -> Dict[str, Any]:
    """Register Ready-to-Trade future keys on the shared feed; clear when empty."""
    from backend.config import settings

    if not getattr(settings, "UPSTOX_MARKET_FEED_ENABLED", True):
        return {"ok": True, "skipped": "feed_disabled"}

    try:
        from backend.services.breakfast_upstox_gate import defer_job_for_breakfast_exclusivity

        if defer_job_for_breakfast_exclusivity("momentx_ws_ltp_sync"):
            return {"ok": True, "skipped": "breakfast_exclusivity"}
    except Exception as e:
        logger.exception("momentx ws: breakfast check failed: %s", e)

    keys = mx_state.ready_future_keys()
    try:
        from backend.services.upstox_market_feed import set_feed_provider_keys

        set_feed_provider_keys(PROVIDER_NAME, keys)
    except Exception as e:
        logger.info("momentx ws: set_feed_provider_keys failed: %s", e)
        return {"ok": False, "error": str(e)[:200], "keys": len(keys)}

    out = {"ok": True, "keys": len(keys), "force": bool(force)}
    logger.info("momentx ws: synced subscriptions %s", out)
    return out


def on_upstox_momentx_tick(
    instrument_key: str,
    *,
    ltp: float,
    now: Optional[Any] = None,
) -> None:
    """Hot-path callback from ``upstox_market_feed`` ingest."""
    try:
        price = float(ltp)
    except (TypeError, ValueError):
        return
    if price <= 0:
        return
    mx_state.update_ltp(instrument_key, price)

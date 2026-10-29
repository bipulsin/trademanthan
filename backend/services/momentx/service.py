"""API-facing MomentX facade."""
from __future__ import annotations

from typing import Any, Dict, List

from backend.services.momentx import state as mx_state
from backend.services.momentx.config import CONFIG
from backend.services.momentx.scanner import ensure_scan_if_needed


def get_ready_payload(*, ensure: bool = True) -> Dict[str, Any]:
    if ensure:
        try:
            ensure_scan_if_needed()
        except Exception as e:
            import logging

            logging.getLogger(__name__).exception("momentx ensure_scan_if_needed failed: %s", e)
            mx_state.set_last_error(str(e)[:300])
    snap = mx_state.snapshot()
    cards: List[Dict[str, Any]] = list(snap.get("ready") or [])
    # Active first, then triggered, then stock name.
    cards.sort(
        key=lambda c: (
            0 if c.get("active", True) else 1,
            0 if c.get("trigger") else 1,
            str(c.get("stock") or ""),
        )
    )
    return {
        "ok": True,
        "scan_at": snap.get("scan_at"),
        "ema_at": snap.get("ema_at"),
        "market_status": snap.get("market_status"),
        "universe_size": snap.get("universe_size"),
        "qualified_count": snap.get("qualified_count"),
        "day_count": snap.get("day_count") if snap.get("day_count") is not None else len(cards),
        "board_date": snap.get("board_date"),
        "scan_duration_sec": snap.get("scan_duration_sec"),
        "scan_running": snap.get("scan_running"),
        "ema_running": snap.get("ema_running"),
        "last_error": snap.get("last_error"),
        "poll_sec": CONFIG.poll_ready_sec,
        "config": CONFIG.to_public_dict(),
        "ready": cards,
    }


def take_trade_log(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Record a Take Trade confirmation (no order placement)."""
    import logging

    logger = logging.getLogger(__name__)
    stock = str((payload or {}).get("stock") or "").strip().upper()
    logger.info(
        "momentx take_trade stock=%s future=%s ltp=%s ema5=%s ema15=%s ema50=%s",
        stock,
        (payload or {}).get("future_symbol"),
        (payload or {}).get("ltp"),
        (payload or {}).get("ema5"),
        (payload or {}).get("ema15"),
        (payload or {}).get("ema50"),
    )
    return {"ok": True, "logged": True, "stock": stock}

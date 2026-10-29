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
    # Keep Active→Locked and 15:00 lock-all fresh between scans.
    try:
        from backend.services.momentx.status_rules import refresh_board_statuses

        refreshed = refresh_board_statuses(mx_state.ready_cards())
        mx_state.set_ready_cards(refreshed)
    except Exception:
        pass
    snap = mx_state.snapshot()
    cards: List[Dict[str, Any]] = list(snap.get("ready") or [])
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
    """Record a Take Trade confirmation (no order placement) into the day log."""
    import logging

    from backend.services.momentx.persistence import record_take_trade

    logger = logging.getLogger(__name__)
    stock = str((payload or {}).get("stock") or "").strip().upper()
    logger.info(
        "momentx take_trade stock=%s future=%s entry=%s exit=%s ltp=%s",
        stock,
        (payload or {}).get("future_symbol"),
        (payload or {}).get("entry_price"),
        (payload or {}).get("exit_price"),
        (payload or {}).get("ltp"),
    )
    result = record_take_trade(payload or {})
    result["logged"] = bool(result.get("ok"))
    result["stock"] = stock
    return result

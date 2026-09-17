"""Live EMA5 entry snapshots for the Kavach checklist UI.

Arms the shared Upstox market feed (via rocket_ws_live) and returns current
10m EMA5 for checklist symbols so Entry fields can update between full /data polls.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


def _f(v: Any) -> Optional[float]:
    try:
        if v is None or v == "":
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def arm_live_feed() -> Dict[str, Any]:
    """Ensure Upstox WS + rocket 10m books are running for current-month futures."""
    out: Dict[str, Any] = {"ok": True, "feed_armed": False, "error": None}
    try:
        from backend.services.rocket_ws_live import ensure_rocket_feed_running

        ensure_rocket_feed_running()
        out["feed_armed"] = True
    except Exception as exc:
        logger.warning("daily_checklist live feed arm failed: %s", exc)
        out["ok"] = False
        out["error"] = str(exc)
    try:
        from backend.services.upstox_market_feed import feed_status

        out["feed_status"] = feed_status()
    except Exception:
        out["feed_status"] = None
    return out


def live_entry_snapshots(symbols: Optional[List[str]] = None) -> Dict[str, Any]:
    """Return live EMA5/entry for the given symbols (rocket WS first)."""
    arm = arm_live_feed()
    syms = []
    for s in symbols or []:
        u = (s or "").strip().upper()
        if u and u not in syms:
            syms.append(u)
    entries: List[Dict[str, Any]] = []
    if not syms:
        return {"ok": True, "entries": entries, "arm": arm}

    get_live = None
    try:
        from backend.services.rocket_ws_live import get_live_10m

        get_live = get_live_10m
    except Exception as exc:
        logger.debug("rocket get_live_10m unavailable: %s", exc)

    for sym in syms:
        row: Dict[str, Any] = {
            "symbol": sym,
            "ema5": None,
            "entry": None,
            "source": None,
            "age_sec": None,
        }
        if get_live is not None:
            try:
                live = get_live(sym)
            except Exception:
                live = None
            if live:
                e5 = _f(live.get("ema5"))
                if e5 is not None and e5 > 0:
                    px = round(float(e5), 2)
                    row["ema5"] = px
                    row["entry"] = px
                    row["source"] = "rocket_ws"
                    row["age_sec"] = live.get("age_sec")
        entries.append(row)

    return {"ok": True, "entries": entries, "arm": arm}

"""Idempotent order state machine, retries, sequencing, freeze-qty split (locked send)."""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Sequence

from backend.services.tarang.live_broker import MockBroker, place_or_shadow
from backend.services.tarang.live_control import live_limits

logger = logging.getLogger(__name__)

STATES = ("NEW", "ACK", "PARTIAL", "FILLED", "REJECTED", "CANCELLED", "TIMEOUT", "SHADOW")


def backoff_delays(retries: int = 3) -> List[float]:
    return [0.25 * (2 ** i) for i in range(max(retries, 1))]


def split_freeze_qty(qty: float, freeze: float) -> List[float]:
    freeze = float(freeze or 0)
    qty = float(qty or 0)
    if freeze <= 0 or qty <= freeze:
        return [qty] if qty else []
    parts = []
    left = qty
    while left > 1e-9:
        chunk = min(left, freeze)
        parts.append(chunk)
        left -= chunk
    return parts


def sequence_legs(legs: Sequence[Dict[str, Any]], *, unwind: bool = False) -> List[Dict[str, Any]]:
    """Buy longs first on entry; unwind shorts first if a short fails (caller decides)."""
    if unwind:
        return sorted(legs, key=lambda lg: 0 if str(lg.get("side") or "").upper() == "SELL" else 1)
    return sorted(legs, key=lambda lg: 0 if str(lg.get("side") or "").upper() == "BUY" else 1)


def price_protected(limit_px: float, ref_px: float, band_frac: float) -> bool:
    if ref_px <= 0:
        return False
    return abs(float(limit_px) - float(ref_px)) / float(ref_px) <= float(band_frac)


def submit_spread_shadow(
    legs: Sequence[Dict[str, Any]],
    *,
    venue: str,
    qty: float,
    trade_id: Optional[int] = None,
    client_order_id: Optional[str] = None,
) -> Dict[str, Any]:
    limits = live_limits()
    freeze = float((limits.get("freeze_qty") or {}).get(venue) or qty)
    if qty > float(limits.get("max_qty") or 1):
        return {"ok": False, "error": "max_qty", "sent": False}
    results = []
    ordered = sequence_legs(list(legs))
    for i, leg in enumerate(ordered):
        for chunk in split_freeze_qty(qty, freeze):
            payload = {
                "trade_id": trade_id,
                "venue": venue,
                "side": leg.get("side"),
                "symbol": leg.get("symbol") or leg.get("instrument_key"),
                "qty": chunk,
                "price": leg.get("mid") or leg.get("ask") or leg.get("bid"),
                "client_order_id": client_order_id or None,
                "leg_index": i,
                "tag": limits.get("order_tag"),
            }
            out = place_or_shadow(payload)
            results.append(out)
            if not out.get("ok") and str(leg.get("side") or "").upper() != "BUY":
                # short failed after longs — record unwind intent, send nothing
                unwind_payload = {
                    "trade_id": trade_id,
                    "venue": venue,
                    "action": "unwind_longs",
                    "reason": "short_leg_failed",
                }
                place_or_shadow(unwind_payload)
                return {"ok": False, "error": "short_leg_failed", "sent": False, "legs": results}
    return {"ok": True, "sent": False, "legs": results}


def retry_place(broker: MockBroker, payload: Dict[str, Any], retries: int = 3) -> Dict[str, Any]:
    last: Dict[str, Any] = {}
    for _ in backoff_delays(retries):
        last = broker.place(payload)
        if last.get("ok") or last.get("status") in ("REJECTED", "AUTH"):
            return last
    return last or {"ok": False, "error": "retries_exhausted"}


def recon_consider_tag_only(orders: Sequence[Dict[str, Any]], tag: str = "tarang-") -> List[Dict[str, Any]]:
    return [o for o in orders if str(o.get("client_order_id") or o.get("tag") or "").startswith(tag)]

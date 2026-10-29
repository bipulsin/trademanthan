"""Trading-day board merge: keep cards once seen; grey (inactive) when filters fail."""
from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, List, Optional


def board_date_str(d: date) -> str:
    return d.isoformat()


def merge_day_cards(
    *,
    prev_cards: List[Dict[str, Any]],
    qualified_cards: List[Dict[str, Any]],
    stage1_by_stock: Optional[Dict[str, Dict[str, Any]]] = None,
    now: Optional[datetime] = None,
) -> List[Dict[str, Any]]:
    """Union of previously seen cards and newly qualified ones for the same day.

    - Newly / still qualified → ``active=True``
    - Previously seen but not qualified this scan → kept with ``active=False``
    - ``stage1_by_stock`` refreshes Stage-1 values on inactive cards when available
    """
    now_iso = now.isoformat() if now is not None else None
    stage1_by_stock = stage1_by_stock or {}

    by_stock: Dict[str, Dict[str, Any]] = {}
    for prev in prev_cards or []:
        key = str(prev.get("stock") or "").upper()
        if not key:
            continue
        card = dict(prev)
        card["stock"] = key
        card["active"] = False
        by_stock[key] = card

    for qc in qualified_cards or []:
        key = str(qc.get("stock") or "").upper()
        if not key:
            continue
        prev = by_stock.get(key) or {}
        card = dict(qc)
        card["stock"] = key
        card["active"] = True
        card["first_seen_at"] = prev.get("first_seen_at") or now_iso or card.get("first_seen_at")
        # Preserve Stage-2 fields if the new card omitted them.
        for fld in (
            "ltp",
            "ltp_at",
            "ltp_dir",
            "ema5",
            "ema15",
            "ema50",
            "ema_ordered",
            "ema_at",
            "trigger",
            "trigger_since",
            "lot_size",
        ):
            if card.get(fld) is None and prev.get(fld) is not None:
                card[fld] = prev.get(fld)
        by_stock[key] = card

    for key, card in by_stock.items():
        if card.get("active"):
            continue
        s1 = stage1_by_stock.get(key)
        if s1:
            card["stage1"] = s1
        if not card.get("first_seen_at") and now_iso:
            card["first_seen_at"] = now_iso

    out = list(by_stock.values())
    out.sort(
        key=lambda c: (
            0 if c.get("active") else 1,
            0 if c.get("trigger") else 1,
            str(c.get("stock") or ""),
        )
    )
    return out

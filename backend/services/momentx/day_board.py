"""Trading-day board merge: keep cards once seen; status via status_rules."""
from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

from backend.services.momentx.status_rules import (
    STATUS_ACTIVE,
    apply_status_to_card,
    refresh_board_statuses,
)


def board_date_str(d: date) -> str:
    return d.isoformat()


def merge_day_cards(
    *,
    prev_cards: List[Dict[str, Any]],
    qualified_cards: List[Dict[str, Any]],
    stage1_by_stock: Optional[Dict[str, Dict[str, Any]]] = None,
    now: Optional[datetime] = None,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Union of previously seen cards and newly Stage-1 qualified ones.

    Returns ``(merged_cards, newly_filtered_cards)`` where newly filtered are
    symbols that first appeared on the day board this scan.
    """
    now_iso = now.isoformat() if now is not None else None
    stage1_by_stock = stage1_by_stock or {}
    prev_keys = {str(p.get("stock") or "").upper() for p in (prev_cards or []) if p.get("stock")}
    qualified_keys = {
        str(q.get("stock") or "").upper() for q in (qualified_cards or []) if q.get("stock")
    }

    by_stock: Dict[str, Dict[str, Any]] = {}
    for prev in prev_cards or []:
        key = str(prev.get("stock") or "").upper()
        if not key:
            continue
        card = dict(prev)
        card["stock"] = key
        card["filters_ok"] = key in qualified_keys
        by_stock[key] = card

    newly_filtered: List[Dict[str, Any]] = []
    for qc in qualified_cards or []:
        key = str(qc.get("stock") or "").upper()
        if not key:
            continue
        prev = by_stock.get(key) or {}
        was_new = key not in prev_keys
        card = dict(qc)
        card["stock"] = key
        card["filters_ok"] = True
        card["first_seen_at"] = prev.get("first_seen_at") or now_iso or card.get("first_seen_at")
        for fld in (
            "ltp",
            "ltp_at",
            "ltp_dir",
            "ema5",
            "ema10",
            "ema15",
            "ema50",
            "ema_ordered",
            "ema_at",
            "trigger",
            "trigger_since",
            "lot_size",
            "first_active_at",
            "active_since",
            "status",
        ):
            if card.get(fld) is None and prev.get(fld) is not None:
                card[fld] = prev.get(fld)
        re_filtered = str(prev.get("status") or "") == "filters_off"
        if was_new or re_filtered:
            card["status"] = STATUS_ACTIVE
            card.pop("active_since", None)
            newly_filtered.append(card)
        by_stock[key] = card

    for key, card in by_stock.items():
        if key in qualified_keys:
            continue
        s1 = stage1_by_stock.get(key)
        if s1:
            card["stage1"] = s1
        card["filters_ok"] = False
        if not card.get("first_seen_at") and now_iso:
            card["first_seen_at"] = now_iso

    out = [apply_status_to_card(c, now=now) for c in by_stock.values()]
    out = refresh_board_statuses(out, now=now)
    return out, newly_filtered

"""Card status: Active / Filters Off / Locked (+ time cutoffs)."""
from __future__ import annotations

from datetime import datetime, time as dt_time, timedelta
from typing import Any, Dict, List, Optional

import pytz

from backend.services.momentx.config import CONFIG

IST = pytz.timezone("Asia/Kolkata")

STATUS_ACTIVE = "active"
STATUS_FILTERS_OFF = "filters_off"
STATUS_LOCKED = "locked"


def _aware(now: Optional[datetime] = None) -> datetime:
    t = now or datetime.now(IST)
    if t.tzinfo is None:
        return IST.localize(t)
    return t.astimezone(IST)


def past_scan_cutoff(now: Optional[datetime] = None) -> bool:
    t = _aware(now).time()
    h, m = CONFIG.scan_cutoff_hhmm
    return t >= dt_time(h, m)


def past_lock_all(now: Optional[datetime] = None) -> bool:
    t = _aware(now).time()
    h, m = CONFIG.lock_all_hhmm
    return t >= dt_time(h, m)


def _parse_iso(iso: Any) -> Optional[datetime]:
    if not iso:
        return None
    try:
        s = str(iso).replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            return IST.localize(dt)
        return dt.astimezone(IST)
    except Exception:
        return None


def activate_card(card: Dict[str, Any], now: datetime) -> None:
    """Mark card Active and stamp first/current activation times."""
    now_iso = now.isoformat()
    was_first = not card.get("first_active_at")
    card["status"] = STATUS_ACTIVE
    card["active"] = True
    card["active_since"] = now_iso
    if was_first:
        card["first_active_at"] = now_iso
    try:
        from backend.services.momentx.persistence import record_activation

        lot = card.get("lot_size")
        try:
            lot_i = int(lot) if lot is not None else None
        except (TypeError, ValueError):
            lot_i = None
        record_activation(
            stock=str(card.get("stock") or ""),
            future_symbol=card.get("future_symbol"),
            future_instrument_key=card.get("future_instrument_key"),
            lot_size=lot_i,
            ltp=card.get("ltp") or ((card.get("stage1") or {}).get("price")),
            activated_at=now,
        )
    except Exception:
        pass


def apply_status_to_card(
    card: Dict[str, Any],
    *,
    now: Optional[datetime] = None,
    ema_cross_activate: bool = False,
) -> Dict[str, Any]:
    """Mutate and return card with current status.

    Priority:
    1. After lock-all time → Locked
    2. EMA(5)×EMA(10) cross up → Active
    3. Stage-1 filters failing → Filters Off
    4. Active longer than lock window → Locked
    5. filters ok + not locked → Active (including first appear)
    """
    now = _aware(now)
    filters_ok = bool(card.get("filters_ok", card.get("active", False)))
    card["filters_ok"] = filters_ok

    if past_lock_all(now):
        card["status"] = STATUS_LOCKED
        card["active"] = False
        return card

    if ema_cross_activate:
        card["filters_ok"] = True
        activate_card(card, now)
        return card

    if not filters_ok:
        card["status"] = STATUS_FILTERS_OFF
        card["active"] = False
        return card

    status = str(card.get("status") or "").lower()
    if status == STATUS_LOCKED:
        card["active"] = False
        return card

    if status == STATUS_ACTIVE:
        since = _parse_iso(card.get("active_since"))
        if since is None:
            card["active_since"] = now.isoformat()
            if not card.get("first_active_at"):
                card["first_active_at"] = now.isoformat()
            card["active"] = True
            return card
        age = now - since
        if age >= timedelta(seconds=int(CONFIG.active_lock_after_sec)):
            card["status"] = STATUS_LOCKED
            card["active"] = False
            return card
        card["active"] = True
        return card

    # filters ok and not yet Active/Locked → become Active
    activate_card(card, now)
    return card


def refresh_board_statuses(
    cards: List[Dict[str, Any]],
    *,
    now: Optional[datetime] = None,
) -> List[Dict[str, Any]]:
    now = _aware(now)
    out = [apply_status_to_card(dict(c), now=now) for c in (cards or [])]
    out.sort(
        key=lambda c: (
            {"active": 0, "locked": 1, "filters_off": 2}.get(str(c.get("status") or ""), 3),
            0 if c.get("trigger") else 1,
            str(c.get("stock") or ""),
        )
    )
    return out

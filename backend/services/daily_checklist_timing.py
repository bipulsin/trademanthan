"""GO timestamp, sticky window, and staleness gating for daily checklist."""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, Optional

import pytz

from backend.services.daily_checklist import D_GO, D_WATCH, SEC_GO, SEC_OUT, SEC_WATCH
from backend.services.rs_conviction_config import get_config

IST = pytz.timezone("Asia/Kolkata")


def _parse_dt(v: Any) -> Optional[datetime]:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.astimezone(IST) if v.tzinfo else v.replace(tzinfo=IST)
    try:
        s = str(v).replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        return dt.astimezone(IST) if dt.tzinfo else dt.replace(tzinfo=IST)
    except (TypeError, ValueError):
        return None


def _cfg_int(key: str, default: int) -> int:
    try:
        return int(get_config().get(key) or default)
    except (TypeError, ValueError):
        return default


def apply_staleness_cap(derived: Dict[str, Any], *, stale: bool) -> Dict[str, Any]:
    """Never allow GO on stale indicators."""
    out = dict(derived)
    if stale and out.get("section") == SEC_GO:
        out["section"] = SEC_WATCH
        out["decision"] = D_WATCH
        note = "Indicators stale — verify live chart before entry"
        prev = (out.get("eligibility_note") or "").strip()
        out["eligibility_note"] = f"{prev}; {note}" if prev else note
    out["indicator_stale"] = stale
    return out


def apply_go_timing(
    derived: Dict[str, Any],
    prev: Dict[str, Any],
    *,
    stale: bool,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Apply go_enter_first_at, go_sticky_until, and sticky display override."""
    now = now or datetime.now(IST)
    out = dict(derived)
    sticky_min = _cfg_int("go_sticky_minutes", 6)

    prev_section = (prev.get("section") or "").upper()
    prev_first = _parse_dt(prev.get("go_enter_first_at"))
    prev_sticky = _parse_dt(prev.get("go_sticky_until"))

    hard_out = out.get("section") == SEC_OUT and (
        out.get("state_ok") is False or out.get("time_ok") is False
    )

    raw_go = out.get("section") == SEC_GO and not stale

    go_first = prev_first
    sticky_until = prev_sticky

    if raw_go and not hard_out:
        if prev_section != SEC_GO or go_first is None:
            go_first = now
            sticky_until = now + timedelta(minutes=sticky_min)
    elif hard_out or stale:
        go_first = prev_first
        sticky_until = None
    elif prev_sticky and prev_sticky > now and prev_section == SEC_GO and not hard_out:
        if out.get("section") in (SEC_WATCH, SEC_GO) and out.get("gate_score", 0) >= 6:
            out["section"] = SEC_GO
            out["decision"] = D_GO
            out["go_sticky_active"] = True

    out["go_enter_first_at"] = go_first.isoformat() if go_first else None
    out["go_sticky_until"] = sticky_until.isoformat() if sticky_until else None
    out["go_sticky_active"] = bool(
        sticky_until and sticky_until > now and out.get("section") == SEC_GO
    )
    return out

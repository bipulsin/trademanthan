"""Vajra screener freeze and entry cutoff (IST, NSE cash/F&O session)."""
from __future__ import annotations

from datetime import datetime
from typing import Optional

import pytz

from backend.services.market_holiday import should_skip_scheduled_market_jobs_ist
from backend.services.vajra.candles import ist_minutes

IST = pytz.timezone("Asia/Kolkata")

# Last time the rating job may persist a new screener snapshot (inclusive through 15:25).
SCREENER_FREEZE_AFTER_MINUTES = 15 * 60 + 25
# No new discretionary activations from this time onward (inclusive from 15:30).
ENTRY_DISABLED_FROM_MINUTES = 15 * 60 + 30


def _ist_now(now: Optional[datetime] = None) -> datetime:
    if now is None:
        return datetime.now(IST)
    if now.tzinfo is None:
        return IST.localize(now)
    return now.astimezone(IST)


def is_vajra_screener_frozen_ist(now: Optional[datetime] = None) -> bool:
    """True when the screener must not be recomputed or overwritten (post 15:25 on trading days)."""
    now = _ist_now(now)
    if should_skip_scheduled_market_jobs_ist(now):
        return True
    return ist_minutes(now) > SCREENER_FREEZE_AFTER_MINUTES


def is_vajra_entry_disabled_ist(now: Optional[datetime] = None) -> bool:
    """True when ENTER / new trade activation must be blocked."""
    now = _ist_now(now)
    if should_skip_scheduled_market_jobs_ist(now):
        return True
    return ist_minutes(now) >= ENTRY_DISABLED_FROM_MINUTES


def screener_freeze_message() -> str:
    return "Screener frozen at 15:25 IST (last intraday scan)."


def entry_disabled_message() -> str:
    return "New entries closed at 15:30 IST."


def screener_freeze_skip_reason() -> str:
    return "screener_frozen"


def vajra_session_api_fields(now: Optional[datetime] = None) -> dict:
    frozen = is_vajra_screener_frozen_ist(now)
    entry_off = is_vajra_entry_disabled_ist(now)
    parts = []
    if frozen:
        parts.append(screener_freeze_message())
    if entry_off:
        parts.append(entry_disabled_message())
    return {
        "screener_frozen": frozen,
        "entry_disabled": entry_off,
        "session_notice": " ".join(parts) if parts else None,
    }

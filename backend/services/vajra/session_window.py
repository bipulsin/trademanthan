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


# Sector-first workflow: dynamic discovery vs calmer execution focus.
DISCOVERY_WINDOW_START_MINUTES = 9 * 60 + 15
DISCOVERY_WINDOW_END_MINUTES = 10 * 60
EXECUTION_WINDOW_START_MINUTES = 10 * 60


def is_vajra_discovery_window_ist(now: Optional[datetime] = None) -> bool:
    """9:15–10:00 IST — sector rotation and candidate discovery allowed."""
    now = _ist_now(now)
    if should_skip_scheduled_market_jobs_ist(now):
        return False
    m = ist_minutes(now)
    return DISCOVERY_WINDOW_START_MINUTES <= m < DISCOVERY_WINDOW_END_MINUTES


def is_vajra_execution_window_ist(now: Optional[datetime] = None) -> bool:
    """From 10:00 IST — focus / freeze workflow preferred over continuous rescan."""
    now = _ist_now(now)
    if should_skip_scheduled_market_jobs_ist(now):
        return False
    return ist_minutes(now) >= EXECUTION_WINDOW_START_MINUTES


def vajra_workflow_phase_fields(now: Optional[datetime] = None) -> dict:
    discovery = is_vajra_discovery_window_ist(now)
    execution = is_vajra_execution_window_ist(now)
    notice = None
    if discovery:
        notice = (
            "Discovery window (9:15–10:00 IST) — scan sector leaders and shortlist candidates."
        )
    elif execution:
        notice = (
            "Execution window (from 10:00 IST) — freeze Top 3 and use Focus Mode for stable execution."
        )
    return {
        "discovery_window": discovery,
        "execution_window": execution,
        "workflow_phase": "discovery" if discovery else ("execution" if execution else "pre_open"),
        "workflow_notice": notice,
    }


def vajra_session_api_fields(now: Optional[datetime] = None) -> dict:
    frozen = is_vajra_screener_frozen_ist(now)
    entry_off = is_vajra_entry_disabled_ist(now)
    parts = []
    if frozen:
        parts.append(screener_freeze_message())
    if entry_off:
        parts.append(entry_disabled_message())
    wf = vajra_workflow_phase_fields(now)
    if wf.get("workflow_notice"):
        parts.append(wf["workflow_notice"])
    return {
        "screener_frozen": frozen,
        "entry_disabled": entry_off,
        "session_notice": " ".join(parts) if parts else None,
        **wf,
    }

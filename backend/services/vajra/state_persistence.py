"""Per-session hysteresis state for stable qualification transitions."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, Optional, Tuple

PriorKey = Tuple[str, str]  # (session_date_iso, instrument_key_or_stock)

_PRIOR_CACHE: Dict[PriorKey, "PriorQualificationState"] = {}


@dataclass
class PriorQualificationState:
    qualification_state: str
    conviction_score: float = 0.0
    execution_score: float = 0.0
    discovery_score: float = 0.0
    breakout_score: float = 0.0


def _row_key(row: Dict[str, Any], session_date: Optional[date] = None) -> PriorKey:
    sd = session_date
    if sd is None:
        raw = row.get("session_date")
        if raw is not None:
            sd = raw if isinstance(raw, date) else date.fromisoformat(str(raw)[:10])
        else:
            from backend.services.smart_futures_session_date import effective_session_date_ist_for_trend

            sd = effective_session_date_ist_for_trend()
    ident = str(row.get("instrument_key") or row.get("stock") or row.get("security") or "")
    return (sd.isoformat(), ident)


def load_prior_state(row: Dict[str, Any], session_date: Optional[date] = None) -> Optional[PriorQualificationState]:
    key = _row_key(row, session_date)
    cached = _PRIOR_CACHE.get(key)
    if cached is not None:
        return cached
    prev_state = str(row.get("_prior_qualification_state") or row.get("entry_state") or "").upper()
    if not prev_state or prev_state == "REJECT":
        legacy = str(row.get("qualification_state") or "").upper()
        if legacy in ("DISCOVERY", "ARMED", "EXECUTABLE"):
            prev_state = legacy
        elif legacy == "WATCHLIST":
            prev_state = "ARMED" if _f(row.get("execution_score")) >= 55 else "DISCOVERY"
    if not prev_state:
        return None
    from backend.services.vajra.qualification_config import STATE_DISCOVERY, STATE_ARMED, STATE_EXECUTABLE

    if prev_state not in (STATE_DISCOVERY, STATE_ARMED, STATE_EXECUTABLE):
        return None
    st = PriorQualificationState(
        qualification_state=prev_state,
        conviction_score=_f(row.get("conviction_score") or row.get("confidence")),
        execution_score=_f(row.get("execution_score")),
        discovery_score=_f(row.get("discovery_score")),
        breakout_score=_f(row.get("breakout_score")),
    )
    _PRIOR_CACHE[key] = st
    return st


def save_prior_state(row: Dict[str, Any], session_date: Optional[date] = None) -> None:
    key = _row_key(row, session_date)
    _PRIOR_CACHE[key] = PriorQualificationState(
        qualification_state=str(row.get("qualification_state") or "REJECT"),
        conviction_score=_f(row.get("conviction_score") or row.get("confidence")),
        execution_score=_f(row.get("execution_score")),
        discovery_score=_f(row.get("discovery_score")),
        breakout_score=_f(row.get("breakout_score")),
    )


def clear_session_cache(session_date: Optional[date] = None) -> None:
    if session_date is None:
        _PRIOR_CACHE.clear()
        return
    prefix = session_date.isoformat()
    for k in list(_PRIOR_CACHE.keys()):
        if k[0] == prefix:
            _PRIOR_CACHE.pop(k, None)


def _f(v: Any) -> float:
    if v is None:
        return 0.0
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0

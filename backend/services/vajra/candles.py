"""Vajra candle preparation — opening session, incomplete bars, minimum history."""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence

import pytz

from backend.services.vajra.timeframes import MIN_SCAN_BARS, tf_minutes

IST = pytz.timezone("Asia/Kolkata")

# NSE regular session open (cash / F&O aligned for Vajra screening).
MARKET_OPEN_MINUTES = 9 * 60 + 15
OPENING_SESSION_END_MINUTES = 9 * 60 + 40  # through 9:40 — defer strict 5m execution checks

MIN_BARS_BY_TF: Dict[str, int] = {
    "5m": 30,
    "15m": 45,
    "30m": MIN_SCAN_BARS,
    "1hr": 25,
    "1d": 20,
}

OPENING_MIN_BARS_BY_TF: Dict[str, int] = {
    "5m": 25,
    "30m": 45,
    "1hr": 22,
}


def _aware_ist(now: Optional[datetime] = None) -> datetime:
    if now is None:
        return datetime.now(IST)
    if now.tzinfo is None:
        return IST.localize(now)
    return now.astimezone(IST)


def _parse_ts(ts: Any) -> Optional[datetime]:
    from backend.services.upstox_service import _parse_ts_to_aware_ist

    return _parse_ts_to_aware_ist(ts)


def ist_minutes(now: Optional[datetime] = None) -> int:
    t = _aware_ist(now)
    return t.hour * 60 + t.minute


def is_weekday_ist(now: Optional[datetime] = None) -> bool:
    return _aware_ist(now).weekday() < 5


def is_opening_session_ist(now: Optional[datetime] = None) -> bool:
    """09:15–09:40 IST on weekdays — first bars forming after cash open."""
    if not is_weekday_ist(now):
        return False
    m = ist_minutes(now)
    return MARKET_OPEN_MINUTES <= m <= OPENING_SESSION_END_MINUTES


def is_vajra_screening_ready_ist(now: Optional[datetime] = None) -> bool:
    """True from 09:20 IST (first completed 5m bar after 09:15 open)."""
    if not is_weekday_ist(now):
        return False
    return ist_minutes(now) >= 9 * 60 + 20


def min_bars_for_tf(tf_id: str, *, opening_session: bool = False) -> int:
    key = (tf_id or "").strip().lower()
    if opening_session:
        return OPENING_MIN_BARS_BY_TF.get(key, MIN_BARS_BY_TF.get(key, MIN_SCAN_BARS))
    return MIN_BARS_BY_TF.get(key, MIN_SCAN_BARS)


def is_bar_complete(
    candle: Dict[str, Any],
    bar_minutes: int,
    now: Optional[datetime] = None,
) -> bool:
    """Candle timestamp is bar open time; bar is complete when now >= open + period."""
    ts = _parse_ts(candle.get("timestamp"))
    if ts is None:
        return True
    end = ts + timedelta(minutes=max(1, int(bar_minutes)))
    return _aware_ist(now) >= end


def drop_incomplete_last_bar(
    candles: Sequence[Dict[str, Any]],
    tf_id: str,
    *,
    now: Optional[datetime] = None,
) -> List[Dict[str, Any]]:
    """Remove the forming intraday bucket so indicators use last completed bar."""
    if not candles:
        return []
    out = list(candles)
    if len(out) < 2:
        return out
    mins = tf_minutes(tf_id)
    last = out[-1]
    if not is_bar_complete(last, mins, now):
        return out[:-1]
    return out


def prepare_vajra_candles(
    candles: Optional[Sequence[Dict[str, Any]]],
    tf_id: str,
    *,
    now: Optional[datetime] = None,
) -> List[Dict[str, Any]]:
    """
    Sort OHLCV oldest→newest and drop the incomplete session bar during the opening window.
    """
    if not candles:
        return []
    out = sorted(candles, key=lambda c: str(c.get("timestamp") or ""))
    if is_opening_session_ist(now) or is_vajra_screening_ready_ist(now):
        out = drop_incomplete_last_bar(out, tf_id, now=now)
    return out


def has_sufficient_bars(
    candles: Sequence[Dict[str, Any]],
    tf_id: str,
    *,
    now: Optional[datetime] = None,
) -> bool:
    opening = is_opening_session_ist(now) or (
        is_vajra_screening_ready_ist(now) and ist_minutes(now) < 10 * 60
    )
    need = min_bars_for_tf(tf_id, opening_session=opening)
    return len(candles) >= need


def opening_session_skip_5m_validation(now: Optional[datetime] = None) -> bool:
    """Before ~09:35 IST, skip strict 5m execution re-validation (use discovery row)."""
    if not is_weekday_ist(now):
        return False
    m = ist_minutes(now)
    return MARKET_OPEN_MINUTES <= m < 9 * 60 + 35

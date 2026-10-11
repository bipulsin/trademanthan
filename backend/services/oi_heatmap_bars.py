"""
OI heatmap columns aligned to the OI-Buildup Pine script.

  OI Signal  — last completed 15-minute bar vs the previous 15-minute bar
  Prev-OI    — that previous 15-minute bar vs the bar before it
  MTF-OI     — last completed 60-minute bar vs the prior 60-minute bar
  HTF-OI     — forming daily bar vs the prior completed day; when the session
               is closed and today has no bar, last completed day vs the day before
  OI chg %   — 15-minute OI percent that feeds OI Signal, (oi / prior oi - 1) * 100
  Volume     — current 15-minute volume vs the average of the previous 5
               completed 15-minute volumes (current bar excluded)

Missing close or OI is left unset. It is never treated as zero.
"""
from __future__ import annotations

from datetime import datetime, time as dt_time, timedelta
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pytz

from backend.services.oi_integration import interpret_oi_signal

IST = pytz.timezone("Asia/Kolkata")

SESSION_CLOSE = dt_time(15, 30)
SESSION_OPEN_MINUTES = 9 * 60 + 15
VOLUME_FLAT_FRACTION = 0.005  # 0.5% of the prior-5 average

_SIGNAL_LABELS = {
    "LONG_UNWINDING": "LONG_UNWIND",
    "SHORT_BUILDUP": "SHORT_BUILDUP",
    "LONG_BUILDUP": "LONG_BUILDUP",
    "SHORT_COVERING": "SHORT_COVER",
    "NEUTRAL": "NEUTRAL",
}


def _normalize_signal(sig: str) -> str:
    return _SIGNAL_LABELS.get(sig, sig or "NEUTRAL")


def bar_time(bar: Dict[str, Any]) -> Optional[datetime]:
    raw = bar.get("timestamp")
    if raw is None:
        return None
    if isinstance(raw, datetime):
        dt = raw
    else:
        text = str(raw).strip()
        if not text:
            return None
        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00").replace(" ", "T"))
        except ValueError:
            return None
    if dt.tzinfo is None:
        return IST.localize(dt)
    return dt.astimezone(IST)


def bar_date_iso(bar: Dict[str, Any]) -> str:
    ts = str(bar.get("timestamp") or "")
    if len(ts) >= 10 and ts[4] == "-" and ts[7] == "-":
        return ts[:10]
    dt = bar_time(bar)
    return dt.date().isoformat() if dt else ""


def _num(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _close(bar: Dict[str, Any]) -> Optional[float]:
    return _num(bar.get("close"))


def _oi(bar: Dict[str, Any]) -> Optional[float]:
    if "oi" not in bar or bar.get("oi") is None or bar.get("oi") == "":
        return None
    return _num(bar.get("oi"))


def _volume(bar: Dict[str, Any]) -> Optional[float]:
    if "volume" not in bar or bar.get("volume") is None or bar.get("volume") == "":
        return None
    return _num(bar.get("volume"))


def sort_bars(bars: Optional[Sequence[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    out = [b for b in (bars or []) if isinstance(b, dict) and bar_time(b)]
    out.sort(key=lambda b: bar_time(b) or datetime.min.replace(tzinfo=IST))
    return out


def bar_end(start: datetime, bar_minutes: int) -> datetime:
    """Nominal bar end, capped at the 15:30 IST session close.

    The last NSE hour starts at 15:15 and finishes at 15:30, so start+60
    would still look open after the close.
    """
    nominal = start + timedelta(minutes=int(bar_minutes))
    session_close = start.replace(
        hour=SESSION_CLOSE.hour,
        minute=SESSION_CLOSE.minute,
        second=0,
        microsecond=0,
    )
    if start < session_close:
        return min(nominal, session_close)
    return nominal


def bar_is_complete(start: datetime, bar_minutes: int, now: datetime) -> bool:
    return now >= bar_end(start, bar_minutes)


def last_completed_index(
    bars: Sequence[Dict[str, Any]],
    bar_minutes: int,
    now: datetime,
) -> Optional[int]:
    for i in range(len(bars) - 1, -1, -1):
        start = bar_time(bars[i])
        if start is not None and bar_is_complete(start, bar_minutes, now):
            return i
    return None


def pct_change(current: Optional[float], prior: Optional[float]) -> Optional[float]:
    """(current / prior - 1) * 100. Missing or a zero prior stays unset."""
    if current is None or prior is None or prior == 0:
        return None
    return (float(current) / float(prior) - 1.0) * 100.0


def classify_buildup(
    price_now: Optional[float],
    price_prior: Optional[float],
    oi_now: Optional[float],
    oi_prior: Optional[float],
) -> Tuple[Optional[str], Optional[float], Optional[float]]:
    """Return (signal, price%, oi%). Either missing input leaves the signal unset."""
    price_pct = pct_change(price_now, price_prior)
    oi_pct = pct_change(oi_now, oi_prior)
    if price_pct is None or oi_pct is None:
        return None, price_pct, oi_pct
    sig = _normalize_signal(interpret_oi_signal(price_pct, oi_pct))
    return sig, price_pct, oi_pct


def volume_arrow(current: Optional[float], prior_five: Sequence[Optional[float]]) -> Optional[str]:
    """Up / down / flat versus the mean of the previous five volumes.

    A gap smaller than 0.5% of that average is flat. Any missing volume is blank.
    """
    if current is None or len(prior_five) != 5:
        return None
    nums: List[float] = []
    for value in prior_five:
        if value is None:
            return None
        nums.append(float(value))
    avg = sum(nums) / 5.0
    if avg == 0:
        if float(current) == 0:
            return "flat"
        return "up" if float(current) > 0 else "down"
    if abs(float(current) - avg) < VOLUME_FLAT_FRACTION * abs(avg):
        return "flat"
    if float(current) > avg:
        return "up"
    return "down"


def aggregate_15m_to_60m(bars_15: Optional[Sequence[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    """Session-aligned 60-minute bars from 15-minute futures candles.

    Buckets start at 09:15 IST. Close and OI come from the last 15-minute
    candle in the hour (the script's bar close). Used when native hours/1
    candles are unavailable.
    """
    buckets: Dict[datetime, List[Dict[str, Any]]] = {}
    for bar in sort_bars(bars_15):
        start = bar_time(bar)
        if start is None:
            continue
        mins = start.hour * 60 + start.minute
        if mins < SESSION_OPEN_MINUTES or mins >= 15 * 60 + 30:
            continue
        offset = mins - SESSION_OPEN_MINUTES
        bucket_min = SESSION_OPEN_MINUTES + (offset // 60) * 60
        key = start.replace(
            hour=bucket_min // 60,
            minute=bucket_min % 60,
            second=0,
            microsecond=0,
        )
        buckets.setdefault(key, []).append(bar)
    out: List[Dict[str, Any]] = []
    for key in sorted(buckets):
        rows = buckets[key]
        last = rows[-1]
        volumes = [_volume(row) for row in rows]
        rec: Dict[str, Any] = {
            "timestamp": key.isoformat(),
            "close": last.get("close"),
            "oi": last.get("oi"),
        }
        if all(v is not None for v in volumes):
            rec["volume"] = float(sum(v or 0.0 for v in volumes))
        out.append(rec)
    return out


def htf_daily_pair(
    daily_bars: Optional[Sequence[Dict[str, Any]]],
    *,
    now: datetime,
    market_closed: bool,
) -> Optional[Tuple[Dict[str, Any], Dict[str, Any]]]:
    """Forming day vs prior day, or last completed day vs the day before it.

    A session that is still open with no bar for today stays unset, so a
    stale prior day is not shown as the forming day.
    """
    usable: List[Dict[str, Any]] = []
    for bar in sort_bars(daily_bars):
        if _close(bar) is None or _oi(bar) is None:
            continue
        usable.append(bar)
    if len(usable) < 2:
        return None
    last = usable[-1]
    prior = usable[-2]
    last_day = bar_date_iso(last)
    today = now.astimezone(IST).date().isoformat()
    if last_day < today and not market_closed:
        return None
    return last, prior


def htf_record_from_daily(
    daily_bars: Optional[Sequence[Dict[str, Any]]],
    *,
    now: datetime,
    market_closed: bool,
) -> Optional[Dict[str, Any]]:
    pair = htf_daily_pair(daily_bars, now=now, market_closed=market_closed)
    if pair is None:
        return None
    last, prior = pair
    sig, _price_pct, _oi_pct = classify_buildup(
        _close(last), _close(prior), _oi(last), _oi(prior)
    )
    if sig is None:
        return None
    return {
        "htf_oi_signal": sig,
        "price_from": float(_close(prior) or 0),
        "price_to": float(_close(last) or 0),
        "oi_from": int(float(_oi(prior) or 0)),
        "oi_to": int(float(_oi(last) or 0)),
        "bar_latest": str(last.get("timestamp") or ""),
        "bar_prior": str(prior.get("timestamp") or ""),
        "sessions_back": 1,
    }


def classify_heatmap_timeframes(
    *,
    bars_15: Optional[Sequence[Dict[str, Any]]],
    bars_60: Optional[Sequence[Dict[str, Any]]] = None,
    daily_bars: Optional[Sequence[Dict[str, Any]]] = None,
    now: datetime,
    market_closed: bool,
) -> Dict[str, Any]:
    """Column values for one futures contract. Missing inputs stay None."""
    now_ist = now.astimezone(IST) if now.tzinfo else IST.localize(now)
    m15 = sort_bars(bars_15)
    hours = sort_bars(bars_60) if bars_60 else []
    if not any(_oi(bar) is not None and _close(bar) is not None for bar in hours):
        hours = aggregate_15m_to_60m(m15)

    oi_signal: Optional[str] = None
    prev_oi_signal: Optional[str] = None
    oi_chg_pct: Optional[float] = None
    sig_idx = last_completed_index(m15, 15, now_ist)
    if sig_idx is not None and sig_idx >= 1:
        current = m15[sig_idx]
        previous = m15[sig_idx - 1]
        oi_signal, _p, oi_chg_pct = classify_buildup(
            _close(current), _close(previous), _oi(current), _oi(previous)
        )
        if sig_idx >= 2:
            before = m15[sig_idx - 2]
            prev_oi_signal, _pp, _po = classify_buildup(
                _close(previous), _close(before), _oi(previous), _oi(before)
            )

    mtf_oi_signal: Optional[str] = None
    hour_idx = last_completed_index(hours, 60, now_ist)
    if hour_idx is not None and hour_idx >= 1:
        mtf_oi_signal, _hp, _ho = classify_buildup(
            _close(hours[hour_idx]),
            _close(hours[hour_idx - 1]),
            _oi(hours[hour_idx]),
            _oi(hours[hour_idx - 1]),
        )

    htf = htf_record_from_daily(daily_bars, now=now_ist, market_closed=market_closed)

    volume: Optional[str] = None
    if m15:
        current_vol = _volume(m15[-1])
        prior_vols = [_volume(bar) for bar in m15[-6:-1]]
        volume = volume_arrow(current_vol, prior_vols)

    return {
        "oi_signal": oi_signal,
        "prev_oi_signal": prev_oi_signal,
        "mtf_oi_signal": mtf_oi_signal,
        "htf_oi_signal": None if htf is None else htf.get("htf_oi_signal"),
        "oi_chg_pct": oi_chg_pct,
        "volume_arrow": volume,
        "htf_record": htf,
    }

"""Trap-CE 10m long simulation: trigger next-open entry, trap SL, BE, EMA10 trail."""
from __future__ import annotations

from datetime import date, datetime, time
from typing import Any, Dict, List, Optional

import pytz

from backend.services.trap_ce.config import (
    BE_R,
    EMA_TRAIL_PERIOD,
    FORCE_EXIT_TIME,
    SKIP_NO_BARS,
    SKIP_NO_ENTRY,
    SKIP_NO_TRIGGER,
    SKIP_NON_POSITIVE_R,
    TRAIL_ARM_R,
)
from backend.services.vajra.indicators import ema_series

IST = pytz.timezone("Asia/Kolkata")


def _f(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _bar_start(bar: Dict[str, Any]) -> Optional[datetime]:
    start = bar.get("bar_start")
    if isinstance(start, datetime):
        return start.astimezone(IST) if start.tzinfo else IST.localize(start)
    from backend.services.upstox_service import _parse_ts_to_aware_ist

    return _parse_ts_to_aware_ist(bar.get("timestamp"))


def find_trigger_index(bars: List[Dict[str, Any]], trigger_time: time) -> int:
    for i, b in enumerate(bars):
        ts = _bar_start(b)
        if ts is None:
            continue
        if ts.hour == trigger_time.hour and ts.minute == trigger_time.minute:
            return i
    return -1


def simulate_trap_ce_long(
    bars: List[Dict[str, Any]],
    *,
    trigger_time: time,
    lot_size: int,
    session_date: date,
    symbol: str = "",
    instrument_key: str = "",
    future_symbol: str = "",
) -> Dict[str, Any]:
    """
    Entry = open of 10m bar after trigger. Initial SL = trigger 10m low.
    Always 1 lot (no skip / resize on 1R INR). BE at +1R (high touch).
    Trail at +1.5R: exit on confirmed close below EMA10. Square-off 15:15 IST.
    """
    base = {
        "symbol": symbol,
        "instrument_key": instrument_key,
        "future_symbol": future_symbol,
        "session_date": session_date.isoformat(),
        "trigger_time": trigger_time.strftime("%H:%M"),
        "lot_size": lot_size,
        "qty": lot_size,
        "taken": False,
    }
    if not bars:
        return {**base, "skip_reason": SKIP_NO_BARS}
    t_idx = find_trigger_index(bars, trigger_time)
    if t_idx < 0:
        return {**base, "skip_reason": SKIP_NO_TRIGGER}
    e_idx = t_idx + 1
    if e_idx >= len(bars):
        return {**base, "skip_reason": SKIP_NO_ENTRY}

    trigger = bars[t_idx]
    entry_bar = bars[e_idx]
    entry = _f(entry_bar.get("open"))
    sl0 = _f(trigger.get("low"))
    r_pts = entry - sl0
    if r_pts <= 0:
        return {**base, "skip_reason": SKIP_NON_POSITIVE_R, "entry": entry, "sl_initial": sl0}
    risk_inr = r_pts * float(lot_size)

    closes = [_f(b.get("close")) for b in bars]
    emas = ema_series(closes, EMA_TRAIL_PERIOD)
    stop = sl0
    be_armed = False
    trail_armed = False
    exit_px = None
    exit_reason = None
    exit_i = None

    for i in range(e_idx, len(bars)):
        bar = bars[i]
        ts = _bar_start(bar)
        if ts is None:
            continue
        o, h, l, c = _f(bar.get("open")), _f(bar.get("high")), _f(bar.get("low")), _f(bar.get("close"))
        if ts.time() >= FORCE_EXIT_TIME:
            exit_px, exit_reason, exit_i = o, "eod_1515", i
            break
        if trail_armed:
            ema = emas[i] if i < len(emas) else None
            if ema is not None and c < ema:
                exit_px, exit_reason, exit_i = c, "trail_ema10_close", i
                break
        else:
            if o <= stop:
                exit_px, exit_reason, exit_i = o, ("be" if be_armed else "sl"), i
                break
            if l <= stop:
                exit_px, exit_reason, exit_i = stop, ("be" if be_armed else "sl"), i
                break
        if not trail_armed and h >= entry + TRAIL_ARM_R * r_pts:
            trail_armed = True
            be_armed = True
        elif not trail_armed and not be_armed and h >= entry + BE_R * r_pts:
            be_armed = True
            stop = entry

    if exit_px is None:
        last = bars[-1]
        ts = _bar_start(last)
        exit_px = _f(last.get("close"))
        exit_reason = "eod_last_bar"
        exit_i = len(bars) - 1

    pnl_pts = exit_px - entry
    r_real = pnl_pts / r_pts if r_pts else 0.0
    pnl_inr = pnl_pts * float(lot_size)
    exit_ts = _bar_start(bars[exit_i]) if exit_i is not None else None
    return {
        **base,
        "taken": True,
        "entry": entry,
        "sl_initial": sl0,
        "r_points": r_pts,
        "risk_inr": risk_inr,
        "exit": exit_px,
        "exit_reason": exit_reason,
        "exit_time": exit_ts.isoformat() if exit_ts else None,
        "pnl_points": pnl_pts,
        "pnl_inr": pnl_inr,
        "r_realized": r_real,
        "be_armed": be_armed,
        "trail_armed": trail_armed,
        "win": pnl_pts > 0,
    }

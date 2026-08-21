"""BT-1 — pullback legacy (nearer EMA10/VWAP) vs v2 (dual VWAP+EMA10 reset)."""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from backend.services.kavach_bt_checkpoint.config import PULLBACK_HARD_BLOCK_N


def _f(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def count_pullbacks_legacy_on_10m(
    bars: List[Dict[str, Any]],
) -> Tuple[List[int], List[int]]:
    """Legacy nearer-EMA10/VWAP pullback counts per bar (long, short series).

    Mirrors Pine/kavach_readiness nearer logic but operates on already-built
    10m bars with ema5/ema10/vwap (session-scoped).
    """
    n = len(bars)
    pb_long_s = [0] * n
    pb_short_s = [0] * n
    if n < 2:
        return pb_long_s, pb_short_s

    pb_long = 0
    pb_short = 0
    in_pull_long = False
    in_pull_short = False
    bars_since_long = 0
    bars_since_short = 0
    ema_above_prev: Optional[bool] = None

    for i in range(n):
        close = _f(bars[i]["close"]) or 0.0
        vwap = _f(bars[i].get("vwap"))
        ema10 = _f(bars[i].get("ema10"))
        ema5 = _f(bars[i].get("ema5"))
        high = _f(bars[i]["high"]) or close
        low = _f(bars[i]["low"]) or close
        if vwap is None or ema10 is None or ema5 is None:
            pb_long_s[i] = pb_long
            pb_short_s[i] = pb_short
            continue

        ema_above = ema5 > vwap
        ema_below = ema5 < vwap
        if ema_above_prev is not None:
            if ema_above and not ema_above_prev:
                pb_long = 0
                in_pull_long = False
                bars_since_long = 0
            if ema_below and ema_above_prev:
                pb_short = 0
                in_pull_short = False
                bars_since_short = 0
        ema_above_prev = ema_above

        nearer = ema10 if abs(close - ema10) <= abs(close - vwap) else vwap
        if i > 0:
            c_prev = _f(bars[i - 1]["close"]) or 0.0
            v_prev = _f(bars[i - 1].get("vwap")) or vwap
            e10_prev = _f(bars[i - 1].get("ema10")) or ema10
            nearer_prev = e10_prev if abs(c_prev - e10_prev) <= abs(c_prev - v_prev) else v_prev
        else:
            nearer_prev = nearer
            c_prev = close

        if ema_above:
            if not in_pull_long and low <= nearer and i > 0 and c_prev > nearer_prev:
                in_pull_long = True
            if in_pull_long and close > nearer:
                pb_long += 1
                in_pull_long = False
                bars_since_long = 0
            else:
                bars_since_long += 1
                if bars_since_long >= 3 and pb_long > 0:
                    pb_long = 0
        else:
            in_pull_long = False
            bars_since_long = 0

        if ema_below:
            if not in_pull_short and high >= nearer and i > 0 and c_prev < nearer_prev:
                in_pull_short = True
            if in_pull_short and close < nearer:
                pb_short += 1
                in_pull_short = False
                bars_since_short = 0
            else:
                bars_since_short += 1
                if bars_since_short >= 3 and pb_short > 0:
                    pb_short = 0
        else:
            in_pull_short = False
            bars_since_short = 0

        pb_long_s[i] = pb_long
        pb_short_s[i] = pb_short

    return pb_long_s, pb_short_s


def count_pullbacks_v2_on_10m(
    bars: List[Dict[str, Any]],
) -> Tuple[List[int], List[int], List[Dict[str, bool]]]:
    """New pullback model (22-Aug checkpoint).

    Reset to 0 only when the **same candle** touches both VWAP and EMA10.
    After reset, increment when price touches EMA5 within the current leg
    (long: low <= EMA5 while EMA5 > VWAP; short: high >= EMA5 while EMA5 < VWAP).
    """
    n = len(bars)
    pb_long_s = [0] * n
    pb_short_s = [0] * n
    flags: List[Dict[str, bool]] = []
    if n == 0:
        return pb_long_s, pb_short_s, flags

    pb_long = 0
    pb_short = 0
    # After a dual reset, next EMA5 touch increments once per touch episode
    in_touch_long = False
    in_touch_short = False

    for i in range(n):
        close = _f(bars[i]["close"]) or 0.0
        high = _f(bars[i]["high"]) or close
        low = _f(bars[i]["low"]) or close
        vwap = _f(bars[i].get("vwap"))
        ema10 = _f(bars[i].get("ema10"))
        ema5 = _f(bars[i].get("ema5"))

        touch_vwap = vwap is not None and low <= vwap <= high
        touch_ema10 = ema10 is not None and low <= ema10 <= high
        touch_ema5 = ema5 is not None and low <= ema5 <= high
        dual_reset = touch_vwap and touch_ema10

        if dual_reset:
            pb_long = 0
            pb_short = 0
            in_touch_long = False
            in_touch_short = False

        leg_long = ema5 is not None and vwap is not None and ema5 > vwap
        leg_short = ema5 is not None and vwap is not None and ema5 < vwap

        if not dual_reset and touch_ema5 and leg_long:
            if not in_touch_long:
                pb_long += 1
                in_touch_long = True
        else:
            if not touch_ema5:
                in_touch_long = False

        if not dual_reset and touch_ema5 and leg_short:
            if not in_touch_short:
                pb_short += 1
                in_touch_short = True
        else:
            if not touch_ema5:
                in_touch_short = False

        pb_long_s[i] = pb_long
        pb_short_s[i] = pb_short
        flags.append(
            {
                "touched_vwap": bool(touch_vwap),
                "touched_ema10": bool(touch_ema10),
                "touched_ema5": bool(touch_ema5),
                "dual_reset": bool(dual_reset),
            }
        )

    return pb_long_s, pb_short_s, flags


def pullback_at_entry(
    bars: List[Dict[str, Any]],
    entry_time: Any,
    direction: str,
) -> Dict[str, Any]:
    """Resolve pullback counts at the last 10m bar with bar_end <= entry_time."""
    from datetime import datetime

    import pytz

    IST = pytz.timezone("Asia/Kolkata")

    def _parse(t: Any) -> Optional[datetime]:
        if t is None:
            return None
        if isinstance(t, datetime):
            dt = t
        else:
            try:
                dt = datetime.fromisoformat(str(t).replace("Z", "+00:00"))
            except Exception:
                return None
        if dt.tzinfo is None:
            return IST.localize(dt)
        return dt.astimezone(IST)

    et = _parse(entry_time)
    leg_legacy, short_legacy = count_pullbacks_legacy_on_10m(bars)
    leg_v2, short_v2, flags = count_pullbacks_v2_on_10m(bars)
    is_long = str(direction).upper() in ("LONG", "BUY", "B")

    idx = -1
    if et is not None:
        for i, b in enumerate(bars):
            be = b.get("bar_end")
            be_dt = _parse(be)
            if be_dt is not None and be_dt <= et:
                idx = i
    if idx < 0 and bars:
        idx = 0

    if idx < 0:
        return {
            "pb_legacy": None,
            "pb_v2": None,
            "pb_hard_blocked": False,
            "bar_idx": None,
            "flags": None,
        }

    pb_leg = leg_legacy[idx] if is_long else short_legacy[idx]
    pb_v2 = leg_v2[idx] if is_long else short_v2[idx]
    return {
        "pb_legacy": int(pb_leg),
        "pb_v2": int(pb_v2),
        "pb_hard_blocked": int(pb_v2) >= PULLBACK_HARD_BLOCK_N,
        "bar_idx": idx,
        "flags": flags[idx] if flags else None,
        "pb_legacy_long": leg_legacy[idx],
        "pb_legacy_short": short_legacy[idx],
        "pb_v2_long": leg_v2[idx],
        "pb_v2_short": short_v2[idx],
    }


def pb_bucket(n: Optional[int]) -> str:
    if n is None:
        return "NA"
    if n >= PULLBACK_HARD_BLOCK_N:
        return "5+"
    return str(int(n))

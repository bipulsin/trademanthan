"""Weekly screen, swing points, and reference levels."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import List, Optional, Sequence, Tuple

from backend.services.mtf_swing.bars import Bar, WeekKey, back_adjust_to_last, week_key
from backend.services.mtf_swing.config import CLUSTER_PCT


@dataclass
class WeekStats:
    key: WeekKey
    low: float
    high: float
    close: float
    last_date: date
    last_ts: datetime
    basis_contract: str
    adjusted: List[Bar]


@dataclass
class Levels:
    side: str
    sl: float
    t1: float
    trigger: float
    basis_contract: str
    screen_date: date
    valid_week: WeekKey
    # Opposite extreme of previous week (reward anchor for potential RR / v1 T1).
    opp_extreme: float = 0.0
    # Timestamp of the bar that set the SL (sweep or pivot); used as trigger look-back.
    sl_pivot_ts: Optional[datetime] = None
    sl_basis: str = "abs_extreme"


def week_stats(raw_bars: Sequence[Bar]) -> Optional[WeekStats]:
    if not raw_bars:
        return None
    last = raw_bars[-1]
    return WeekStats(
        key=week_key(last.session_date),
        low=min(b.low for b in raw_bars),
        high=max(b.high for b in raw_bars),
        close=last.close,
        last_date=last.session_date,
        last_ts=last.ts,
        basis_contract=last.contract,
        adjusted=list(raw_bars),
    )


def adjusted_pair(w2_raw: Sequence[Bar], w1_raw: Sequence[Bar]) -> Tuple[Optional[WeekStats], Optional[WeekStats]]:
    """Back-adjust W2+W1 together so levels share the last W1 contract's basis."""
    if not w1_raw:
        return None, None
    combined = list(w2_raw) + list(w1_raw)
    adjusted = back_adjust_to_last(combined)
    n2 = len(w2_raw)
    w2_adj = adjusted[:n2]
    w1_adj = adjusted[n2:]
    return week_stats(w2_adj), week_stats(w1_adj)


def candidate_side(w1: WeekStats, w2: WeekStats) -> Optional[str]:
    """Long and short are mutually exclusive (W1.high cannot be both <= and > W2.high)."""
    if w1.low < w2.low and w2.low < w1.close < w2.high and w1.high <= w2.high:
        return "LONG"
    if w1.high > w2.high and w2.low < w1.close < w2.high and w1.low >= w2.low:
        return "SHORT"
    return None


def swing_high_indexes(bars: Sequence[Bar]) -> List[int]:
    """Confirmed swing highs. Index i needs both neighbors; not usable before bar i+1 exists."""
    out: List[int] = []
    for i in range(1, len(bars) - 1):
        h = bars[i].high
        if h > bars[i - 1].high and h > bars[i + 1].high:
            out.append(i)
    return out


def swing_low_indexes(bars: Sequence[Bar]) -> List[int]:
    out: List[int] = []
    for i in range(1, len(bars) - 1):
        lo = bars[i].low
        if lo < bars[i - 1].low and lo < bars[i + 1].low:
            out.append(i)
    return out


def _clustered(newer: float, older: float, pct: float, prefer_higher: bool) -> bool:
    base = min(abs(newer), abs(older))
    if base <= 0:
        return False
    if abs(newer - older) / base > pct + 1e-12:
        return False
    return True


def pick_swing_high(bars: Sequence[Bar], before_ts: datetime, pct: float = CLUSTER_PCT) -> Optional[int]:
    idxs = [i for i in swing_high_indexes(bars) if bars[i].ts < before_ts]
    if not idxs:
        return None
    i1 = idxs[-1]
    if len(idxs) >= 2:
        i0 = idxs[-2]
        h1 = bars[i1].high
        h0 = bars[i0].high
        if _clustered(h1, h0, pct, True) and h0 > h1:
            return i0
    return i1


def pick_swing_low(bars: Sequence[Bar], before_ts: datetime, pct: float = CLUSTER_PCT) -> Optional[int]:
    idxs = [i for i in swing_low_indexes(bars) if bars[i].ts < before_ts]
    if not idxs:
        return None
    i1 = idxs[-1]
    if len(idxs) >= 2:
        i0 = idxs[-2]
        l1 = bars[i1].low
        l0 = bars[i0].low
        if _clustered(l1, l0, pct, False) and l0 < l1:
            return i0
    return i1


def _sweep_low_ts(bars: Sequence[Bar]) -> datetime:
    low = min(b.low for b in bars)
    ts = bars[0].ts
    for b in bars:
        if b.low == low:
            ts = b.ts
    return ts


def _sweep_high_ts(bars: Sequence[Bar]) -> datetime:
    high = max(b.high for b in bars)
    ts = bars[0].ts
    for b in bars:
        if b.high == high:
            ts = b.ts
    return ts


def _pivot_sl_long(bars: Sequence[Bar], w2_low: float) -> Optional[Tuple[float, datetime]]:
    """Lowest confirmed swing low in W1 strictly below previous week's low."""
    best: Optional[Tuple[float, datetime]] = None
    for i in swing_low_indexes(bars):
        lo = bars[i].low
        if lo >= w2_low:
            continue
        if best is None or lo < best[0]:
            best = (lo, bars[i].ts)
    return best


def _pivot_sl_short(bars: Sequence[Bar], w2_high: float) -> Optional[Tuple[float, datetime]]:
    """Highest confirmed swing high in W1 strictly above previous week's high."""
    best: Optional[Tuple[float, datetime]] = None
    for i in swing_high_indexes(bars):
        hi = bars[i].high
        if hi <= w2_high:
            continue
        if best is None or hi > best[0]:
            best = (hi, bars[i].ts)
    return best


def build_levels(
    side: str,
    w1: WeekStats,
    w2: WeekStats,
    *,
    cluster_pct: float = CLUSTER_PCT,
    sl_basis: str = "abs_extreme",
) -> Tuple[Optional[Levels], Optional[str]]:
    """Return (levels, discard_reason).

    Reasons: NO_SWING, LEVELS_ILLOGICAL, NO_PIVOT_SL.
    ``sl_basis=pivot_outside_prev_week`` places SL on a confirmed swing outside W2
    and anchors the breakout-trigger look-back at that pivot's timestamp.
    """
    bars = w1.adjusted
    basis = (sl_basis or "abs_extreme").strip().lower()
    if side == "LONG":
        opp = w2.high
        if basis == "pivot_outside_prev_week":
            pivot = _pivot_sl_long(bars, w2.low)
            if pivot is None:
                return None, "NO_PIVOT_SL"
            sl, sl_ts = pivot
        else:
            sl = w1.low
            sl_ts = _sweep_low_ts(bars)
        t1 = opp
        idx = pick_swing_high(bars, sl_ts, cluster_pct)
        if idx is None:
            return None, "NO_SWING"
        trigger = bars[idx].high
        if not (sl < trigger < t1):
            return None, "LEVELS_ILLOGICAL"
    else:
        opp = w2.low
        if basis == "pivot_outside_prev_week":
            pivot = _pivot_sl_short(bars, w2.high)
            if pivot is None:
                return None, "NO_PIVOT_SL"
            sl, sl_ts = pivot
        else:
            sl = w1.high
            sl_ts = _sweep_high_ts(bars)
        t1 = opp
        idx = pick_swing_low(bars, sl_ts, cluster_pct)
        if idx is None:
            return None, "NO_SWING"
        trigger = bars[idx].low
        if not (t1 < trigger < sl):
            return None, "LEVELS_ILLOGICAL"
    from datetime import timedelta

    valid = week_key(w1.last_date + timedelta(days=7))
    return (
        Levels(
            side=side,
            sl=sl,
            t1=t1,
            trigger=trigger,
            basis_contract=w1.basis_contract,
            screen_date=w1.last_date,
            valid_week=valid,
            opp_extreme=opp,
            sl_pivot_ts=sl_ts,
            sl_basis=basis,
        ),
        None,
    )

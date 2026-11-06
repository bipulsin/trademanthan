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


def build_levels(
    side: str,
    w1: WeekStats,
    w2: WeekStats,
    *,
    cluster_pct: float = CLUSTER_PCT,
) -> Tuple[Optional[Levels], Optional[str]]:
    """Return (levels, discard_reason). Reason is NO_SWING or LEVELS_ILLOGICAL."""
    bars = w1.adjusted
    if side == "LONG":
        sl = w1.low
        t1 = w2.high
        sweep_ts = _sweep_low_ts(bars)
        idx = pick_swing_high(bars, sweep_ts, cluster_pct)
        if idx is None:
            return None, "NO_SWING"
        trigger = bars[idx].high
        if not (sl < trigger < t1):
            return None, "LEVELS_ILLOGICAL"
    else:
        sl = w1.high
        t1 = w2.low
        sweep_ts = _sweep_high_ts(bars)
        idx = pick_swing_low(bars, sweep_ts, cluster_pct)
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
        ),
        None,
    )

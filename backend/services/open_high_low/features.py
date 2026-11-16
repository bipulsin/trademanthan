"""Entry-decision features for OpenHighLow (no look-ahead past 09:25 / third open)."""
from __future__ import annotations

from datetime import date, time
from typing import Any, Dict, List, Optional, Sequence, Set

from backend.services.open_high_low.bars import Bar, bar_at, session_bars
from backend.services.open_high_low.config import (
    EMA_PERIOD_5M,
    FIRST_CANDLE,
    RVOL_LOOKBACK_SESSIONS,
    SECOND_CANDLE,
)
from backend.services.open_high_low.data import previous_trading_day
from backend.services.open_high_low.indicators import ema_last
from backend.services.open_high_low.signals import Setup


def closes_through_second(
    bars_5m: Sequence[Bar],
    session: date,
) -> List[float]:
    """5m closes from history through the day's second candle (09:20), not third."""
    out: List[float] = []
    for b in sorted(bars_5m, key=lambda x: x.ts):
        if b.session_date < session:
            out.append(float(b.close))
        elif b.session_date == session and b.ts.time() <= SECOND_CANDLE:
            out.append(float(b.close))
        elif b.session_date > session:
            break
    return out


def ema30_at_decision(
    bars_5m: Sequence[Bar],
    session: date,
    *,
    period: int = EMA_PERIOD_5M,
) -> Optional[float]:
    """EMA on 5m closes through first two candles of ``session`` (excludes entry candle)."""
    closes = closes_through_second(bars_5m, session)
    return ema_last(closes, period=period)


def prev_session_close(
    daily_bars: Sequence[Bar],
    session: date,
    holidays: Optional[Set[date]] = None,
) -> Optional[float]:
    prev = previous_trading_day(session, holidays)
    for b in daily_bars:
        if b.session_date == prev:
            return float(b.close)
    return None


def first_candle_rvol(
    bars_5m: Sequence[Bar],
    session: date,
    *,
    lookback: int = RVOL_LOOKBACK_SESSIONS,
    holidays: Optional[Set[date]] = None,
) -> Optional[float]:
    """First-candle volume / avg first-candle volume over prior ``lookback`` sessions."""
    first = bar_at(bars_5m, session, FIRST_CANDLE)
    if first is None:
        return None
    # Collect prior session dates present in the series (same contract window).
    sessions = sorted({b.session_date for b in bars_5m if b.session_date < session})
    if holidays is not None:
        # Prefer calendar trading days that exist in bars; already filtered by presence.
        pass
    prior = sessions[-lookback:] if len(sessions) >= 1 else []
    vols: List[float] = []
    for d in prior:
        fb = bar_at(bars_5m, d, FIRST_CANDLE)
        if fb is not None and fb.volume > 0:
            vols.append(float(fb.volume))
    if not vols or first.volume <= 0:
        return None
    avg = sum(vols) / float(len(vols))
    if avg <= 0:
        return None
    return float(first.volume) / avg


def nifty_first_candle_features(
    nifty_bars: Sequence[Bar],
    session: date,
) -> Dict[str, Any]:
    """Nifty 50 index 09:15 candle direction and % change."""
    first = bar_at(nifty_bars, session, FIRST_CANDLE)
    if first is None or first.open == 0:
        return {"nifty_first_dir": None, "nifty_first_pct": None}
    chg = (first.close - first.open) / first.open * 100.0
    if first.close > first.open:
        direction = "UP"
    elif first.close < first.open:
        direction = "DOWN"
    else:
        direction = "FLAT"
    return {"nifty_first_dir": direction, "nifty_first_pct": float(chg)}


def build_entry_features(
    setup: Setup,
    bars_5m: Sequence[Bar],
    daily_bars: Sequence[Bar],
    *,
    atr: Optional[float],
    nifty_bars: Optional[Sequence[Bar]] = None,
    holidays: Optional[Set[date]] = None,
    ema_period: int = EMA_PERIOD_5M,
    rvol_lookback: int = RVOL_LOOKBACK_SESSIONS,
) -> Dict[str, Any]:
    """All no-look-ahead features known at third-candle open (entry decision)."""
    entry = float(setup.third.open)
    atr_f = float(atr) if atr is not None and float(atr) > 0 else None

    ema = ema30_at_decision(bars_5m, setup.session, period=ema_period)
    if ema is not None and ema != 0:
        dist_pct = (entry - ema) / ema * 100.0
    else:
        dist_pct = None
    if ema is not None and atr_f is not None:
        dist_atr = (entry - ema) / atr_f
    else:
        dist_atr = None

    prev_c = prev_session_close(daily_bars, setup.session, holidays)
    if prev_c is not None and prev_c != 0:
        move_pct = (entry - prev_c) / prev_c * 100.0
        day_open = float(setup.first.open)
        gap = (day_open - prev_c) / prev_c * 100.0
    else:
        move_pct = None
        gap = None

    first_range = float(setup.first.high) - float(setup.first.low)
    first_range_pct = (first_range / entry * 100.0) if entry else None
    first_range_atr = (first_range / atr_f) if atr_f is not None else None

    rvol = first_candle_rvol(
        bars_5m, setup.session, lookback=rvol_lookback, holidays=holidays
    )

    nifty = nifty_first_candle_features(nifty_bars or [], setup.session)

    return {
        "ema30_5m": float(ema) if ema is not None else None,
        "entry_dist_ema30_pct": float(dist_pct) if dist_pct is not None else None,
        "entry_dist_ema30_atr": float(dist_atr) if dist_atr is not None else None,
        "prev_close": float(prev_c) if prev_c is not None else None,
        "move_prev_close_to_entry_pct": float(move_pct) if move_pct is not None else None,
        "gap_pct": float(gap) if gap is not None else None,
        "first_range_pct": float(first_range_pct) if first_range_pct is not None else None,
        "first_range_atr": float(first_range_atr) if first_range_atr is not None else None,
        "first_rvol_20d": float(rvol) if rvol is not None else None,
        "nifty_first_dir": nifty.get("nifty_first_dir"),
        "nifty_first_pct": nifty.get("nifty_first_pct"),
        "same_side_count_0925": None,  # filled after day aggregation
    }


def annotate_same_side_counts(feature_rows: Sequence[Dict[str, Any]]) -> None:
    """In-place: set same_side_count_0925 from all same-day same-side signals."""
    from collections import defaultdict

    counts: Dict[tuple, int] = defaultdict(int)
    for row in feature_rows:
        key = (str(row.get("date") or ""), str(row.get("side") or ""))
        counts[key] += 1
    for row in feature_rows:
        key = (str(row.get("date") or ""), str(row.get("side") or ""))
        row["same_side_count_0925"] = int(counts[key])

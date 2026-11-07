"""Weekly market-regime filter (no look-ahead).

At each screening Friday close, classify Nifty (default) as Bullish / Bearish /
Neutral from weekly closes available up through that session. The label applies
to the following week's trades only.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Sequence, Tuple

from backend.services.mtf_swing.bars import Bar, week_key


@dataclass(frozen=True)
class RegimePoint:
    screen_date: date
    week: Tuple[int, int]
    close: float
    ma: float
    prev_ma: Optional[float]
    regime: str  # Bullish | Bearish | Neutral
    rising: bool
    falling: bool


def ema(values: Sequence[float], length: int) -> List[Optional[float]]:
    """Standard EMA. Index i is None until ``length`` samples exist (seed = SMA)."""
    out: List[Optional[float]] = [None] * len(values)
    if length <= 0 or not values:
        return out
    if len(values) < length:
        return out
    seed = sum(values[:length]) / float(length)
    out[length - 1] = seed
    mult = 2.0 / (length + 1.0)
    prev = seed
    for i in range(length, len(values)):
        prev = (values[i] - prev) * mult + prev
        out[i] = prev
    return out


def sma(values: Sequence[float], length: int) -> List[Optional[float]]:
    out: List[Optional[float]] = [None] * len(values)
    if length <= 0:
        return out
    running = 0.0
    for i, v in enumerate(values):
        running += v
        if i >= length:
            running -= values[i - length]
        if i + 1 >= length:
            out[i] = running / float(length)
    return out


def ma_series(values: Sequence[float], length: int, ma_type: str = "ema") -> List[Optional[float]]:
    kind = (ma_type or "ema").strip().lower()
    if kind == "sma":
        return sma(values, length)
    return ema(values, length)


def weekly_closes(bars: Sequence[Bar]) -> List[Tuple[date, Tuple[int, int], float]]:
    """Last bar close of each ISO week, in chronological order."""
    by_week: Dict[Tuple[int, int], Bar] = {}
    for b in bars:
        wk = week_key(b.session_date)
        prev = by_week.get(wk)
        if prev is None or b.ts >= prev.ts:
            by_week[wk] = b
    ordered = sorted(by_week.items(), key=lambda kv: (kv[0][0], kv[0][1]))
    return [(b.session_date, wk, float(b.close)) for wk, b in ordered]


def classify_regime(close: float, ma: float, prev_ma: Optional[float]) -> Tuple[str, bool, bool]:
    rising = prev_ma is not None and ma > prev_ma
    falling = prev_ma is not None and ma < prev_ma
    if close > ma and rising:
        return "Bullish", rising, falling
    if close < ma and falling:
        return "Bearish", rising, falling
    return "Neutral", rising, falling


def build_regime_series(
    bars: Sequence[Bar],
    *,
    ma_length: int = 10,
    ma_type: str = "ema",
) -> List[RegimePoint]:
    """One regime label per completed week, using only closes up to that week."""
    weeks = weekly_closes(bars)
    closes = [c for _, _, c in weeks]
    mas = ma_series(closes, ma_length, ma_type)
    out: List[RegimePoint] = []
    for i, (d, wk, close) in enumerate(weeks):
        ma = mas[i]
        if ma is None:
            continue
        prev_ma = None
        for j in range(i - 1, -1, -1):
            if mas[j] is not None:
                prev_ma = mas[j]
                break
        regime, rising, falling = classify_regime(close, ma, prev_ma)
        out.append(
            RegimePoint(
                screen_date=d,
                week=wk,
                close=close,
                ma=float(ma),
                prev_ma=None if prev_ma is None else float(prev_ma),
                regime=regime,
                rising=rising,
                falling=falling,
            )
        )
    return out


def regime_by_screen_date(points: Sequence[RegimePoint]) -> Dict[str, str]:
    return {p.screen_date.isoformat(): p.regime for p in points}


def regime_allows(side: str, regime: str, neutral_action: str = "block") -> bool:
    """Whether a screened side may trade under the given weekly regime."""
    r = (regime or "Neutral").strip().title()
    s = (side or "").upper()
    if r == "Bullish":
        return s == "LONG"
    if r == "Bearish":
        return s == "SHORT"
    # Neutral
    action = (neutral_action or "block").strip().lower()
    if action == "allow_both":
        return True
    return False


def regime_log_rows(
    points: Sequence[RegimePoint],
    *,
    symbol: str,
    ma_type: str,
    ma_length: int,
    run_ts: str = "",
) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    for p in points:
        rows.append(
            {
                "screen_date": p.screen_date.isoformat(),
                "week": f"{p.week[0]}-W{p.week[1]:02d}",
                "symbol": symbol,
                "close": f"{p.close:.4f}",
                "ma": f"{p.ma:.4f}",
                "prev_ma": "" if p.prev_ma is None else f"{p.prev_ma:.4f}",
                "regime": p.regime,
                "ma_type": ma_type,
                "ma_length": str(int(ma_length)),
                "run_ts": run_ts,
            }
        )
    return rows

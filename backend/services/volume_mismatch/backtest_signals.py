"""Gap + Bollinger Band signal logic — backtest only (not live scanner)."""
from __future__ import annotations

import logging
from datetime import date
from typing import Any, Dict, List, Optional, Sequence

import pandas as pd
import pandas_ta as ta

from backend.services.upstox_service import UpstoxService
from backend.services.upstox_service import _parse_ts_to_aware_ist
from backend.services.volume_mismatch.candles import (
    batch_fetch_candles,
    clear_candle_cache,
    first_15m_bar_for_session,
    previous_day_close,
)


def _candle_date(ts: Any) -> Optional[date]:
    dt = _parse_ts_to_aware_ist(ts)
    return dt.date() if dt is not None else None
from backend.services.volume_mismatch.signal_engine import compute_gap_percent

logger = logging.getLogger(__name__)

# Standard daily BB: 20-period SMA ± 2σ on closes completed before signal session.
BB_LENGTH = 20
BB_STD_DEV = 2.0
DAILY_DAYS_BACK = 60
M15_DAYS_BACK = 35

# Band comparison uses the first 15m bar close (09:15–09:30 IST), not open.
BB_COMPARE_FIELD = "close"


def _f(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def daily_closes_before_session(
    daily_candles: Sequence[Dict[str, Any]],
    session_date: date,
) -> List[float]:
    """Ascending daily closes strictly before ``session_date``."""
    rows: List[tuple] = []
    for c in daily_candles:
        d = _candle_date(c.get("timestamp"))
        if d is None:
            continue
        if d >= session_date:
            continue
        cl = _f(c.get("close"))
        if cl <= 0:
            continue
        rows.append((d, cl))
    rows.sort(key=lambda x: x[0])
    return [cl for _, cl in rows]


def bollinger_bands_as_of_session(
    daily_candles: Sequence[Dict[str, Any]],
    session_date: date,
    *,
    length: int = BB_LENGTH,
    std_dev: float = BB_STD_DEV,
) -> Optional[Dict[str, float]]:
    """
    Bollinger Bands from daily closes ending the day before ``session_date``.

    Uses pandas_ta ``bbands`` (same 20 / 2 convention as strategy_runner).
    Returns bands as of the last completed daily bar before the session.
    """
    closes = daily_closes_before_session(daily_candles, session_date)
    if len(closes) < length:
        return None
    df = pd.DataFrame({"close": closes})
    bb = ta.bbands(df["close"], length=length, std=std_dev)
    if bb is None or bb.empty:
        return None
    last = bb.iloc[-1]
    upper_key = f"BBU_{length}_{std_dev}"
    middle_key = f"BBM_{length}_{std_dev}"
    lower_key = f"BBL_{length}_{std_dev}"
    try:
        upper = float(last[upper_key])
        middle = float(last[middle_key])
        lower = float(last[lower_key])
    except (KeyError, TypeError, ValueError):
        return None
    if any(x != x for x in (upper, middle, lower)):  # NaN check
        return None
    return {
        "bb_upper": round(upper, 4),
        "bb_middle": round(middle, 4),
        "bb_lower": round(lower, 4),
    }


def evaluate_gap_bb_signal(
    *,
    symbol: str,
    future_symbol: str,
    instrument_key: str,
    first_bar: Dict[str, Any],
    previous_close: float,
    bb: Dict[str, float],
) -> Optional[Dict[str, Any]]:
    """
    LONG: gap down (open < prev close) and first 15m close below lower BB.
    SHORT: gap up (open > prev close) and first 15m close above upper BB.
    """
    o = _f(first_bar.get("open"))
    h = _f(first_bar.get("high"))
    l = _f(first_bar.get("low"))
    c = _f(first_bar.get(BB_COMPARE_FIELD))
    if o <= 0 or h <= 0 or l <= 0 or c <= 0 or previous_close <= 0:
        return None

    gap = compute_gap_percent(o, previous_close)
    if gap is None:
        return None

    upper = bb["bb_upper"]
    lower = bb["bb_lower"]

    if o < previous_close and c < lower:
        direction = "LONG"
    elif o > previous_close and c > upper:
        direction = "SHORT"
    else:
        return None

    return {
        "symbol": symbol,
        "future_symbol": future_symbol,
        "instrument_key": instrument_key,
        "direction": direction,
        "gap_percent": round(gap, 4),
        "previous_close": round(previous_close, 4),
        "first_15m_open": round(o, 4),
        "first_15m_high": round(h, 4),
        "first_15m_low": round(l, 4),
        "first_15m_close": round(c, 4),
        "bb_upper": upper,
        "bb_middle": bb["bb_middle"],
        "bb_lower": lower,
    }


def collect_gap_bb_signals_for_date(
    upstox: UpstoxService,
    universe: List[Dict[str, Any]],
    trade_date: date,
    *,
    max_workers: int = 24,
) -> List[Dict[str, Any]]:
    """Run gap + BB logic for one session (no DB write)."""
    if not universe:
        return []

    clear_candle_cache()
    keys = [u["instrument_key"] for u in universe if u.get("instrument_key")]

    candles_15m = batch_fetch_candles(
        upstox,
        keys,
        "minutes/15",
        days_back=M15_DAYS_BACK,
        range_end_date=trade_date,
        max_workers=max_workers,
    )
    candles_1d = batch_fetch_candles(
        upstox,
        keys,
        "days/1",
        days_back=DAILY_DAYS_BACK,
        range_end_date=trade_date,
        max_workers=max_workers,
    )

    signals: List[Dict[str, Any]] = []
    for u in universe:
        ik = u["instrument_key"]
        sym = u["symbol"]
        bars_15 = candles_15m.get(ik) or []
        bars_1d = candles_1d.get(ik) or []
        first_bar = first_15m_bar_for_session(bars_15, trade_date)
        if not first_bar:
            continue
        prev_close = previous_day_close(bars_1d, trade_date)
        if prev_close is None or prev_close <= 0:
            continue
        bb = bollinger_bands_as_of_session(bars_1d, trade_date)
        if not bb:
            continue

        sig = evaluate_gap_bb_signal(
            symbol=sym,
            future_symbol=u.get("future_symbol") or sym,
            instrument_key=ik,
            first_bar=first_bar,
            previous_close=prev_close,
            bb=bb,
        )
        if sig:
            row = dict(sig)
            row["trade_date"] = trade_date.isoformat()
            signals.append(row)
    return signals

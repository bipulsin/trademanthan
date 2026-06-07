"""Gap + Bollinger Band signal logic — backtest only (not live scanner)."""
from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pandas as pd
import pandas_ta as ta

from backend.services.upstox_service import _parse_ts_to_aware_ist
from backend.services.volume_mismatch.candles import (
    BacktestDailyCache,
    fetch_first_15m_bar_for_session,
)
from backend.services.volume_mismatch.signal_engine import compute_gap_percent

logger = logging.getLogger(__name__)

# Standard daily BB: 20-period SMA ± 2σ on closes completed before signal session.
BB_LENGTH = 20
BB_STD_DEV = 2.0

# Band comparison uses the first 15m bar close (09:15–09:30 IST), not open.
BB_COMPARE_FIELD = "close"


def _candle_date(ts: Any) -> Optional[date]:
    dt = _parse_ts_to_aware_ist(ts)
    return dt.date() if dt is not None else None


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


def _bb_band_from_row(
    row: pd.Series,
    prefix: str,
    length: int,
    std_dev: float,
) -> Optional[float]:
    """Resolve one BB band column across pandas_ta naming variants."""
    candidates = [
        f"{prefix}_{length}_{std_dev}",
        f"{prefix}_{length}_{std_dev}_{std_dev}",
        f"{prefix}_{length}_{int(std_dev)}",
        f"{prefix}_{length}_{int(std_dev)}_{int(std_dev)}",
    ]
    for key in candidates:
        if key in row.index:
            try:
                val = float(row[key])
            except (TypeError, ValueError):
                continue
            if val == val:
                return val
    stem = f"{prefix}_{length}_"
    for col in row.index:
        if str(col).startswith(stem):
            try:
                val = float(row[col])
            except (TypeError, ValueError):
                continue
            if val == val:
                return val
    return None


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
    upper = _bb_band_from_row(last, "BBU", length, std_dev)
    middle = _bb_band_from_row(last, "BBM", length, std_dev)
    lower = _bb_band_from_row(last, "BBL", length, std_dev)
    if upper is None or middle is None or lower is None:
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


def _scan_one_symbol(
    upstox: Any,
    u: Dict[str, Any],
    trade_date: date,
    daily_cache: BacktestDailyCache,
) -> Tuple[Optional[Dict[str, Any]], Dict[str, int]]:
    """Returns (signal_or_none, api_call_counts)."""
    stats = {"daily": 0, "m15": 0, "gap": 0, "bb": 0}
    ik = u.get("instrument_key") or ""
    sym = u.get("symbol") or ""
    if not ik:
        return None, stats

    prev_close, daily_fetched = daily_cache.previous_close(upstox, ik, trade_date)
    if daily_fetched:
        stats["daily"] += 1

    first_bar, m15_fetched = fetch_first_15m_bar_for_session(
        upstox,
        ik,
        trade_date,
        persistent_cache=daily_cache.persistent,
    )
    if m15_fetched:
        stats["m15"] += 1
    if not first_bar:
        return None, stats

    o = _f(first_bar.get("open"))
    if o <= 0 or prev_close is None or prev_close <= 0:
        return None, stats

    if o == prev_close:
        return None, stats

    stats["gap"] += 1
    daily_bars, bb_fetched = daily_cache.daily_bars_for_bb(
        upstox, ik, trade_date, min_closes=BB_LENGTH
    )
    if bb_fetched:
        stats["daily"] += 1
    stats["bb"] += 1

    bb = bollinger_bands_as_of_session(daily_bars, trade_date)
    if not bb:
        return None, stats

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
        return row, stats
    return None, stats


def collect_gap_bb_signals_for_date(
    upstox: Any,
    universe: List[Dict[str, Any]],
    trade_date: date,
    *,
    max_workers: int = 2,
    daily_cache: Optional[BacktestDailyCache] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Run gap + BB logic for one session (no DB write).

    Gap check uses prev daily close + first 15m bar only. BB is computed only
    when open != previous close. Returns (signals, timing/stats dict).
    """
    t0 = time.monotonic()
    if not universe:
        return [], {"elapsed_sec": 0.0, "symbols": 0}

    cache = daily_cache if daily_cache is not None else BacktestDailyCache()
    signals: List[Dict[str, Any]] = []
    totals = {
        "daily_api": 0,
        "m15_api": 0,
        "gaps": 0,
        "bb_evals": 0,
        "symbols": len(universe),
        "cache_daily_hit": 0,
        "cache_m15_hit": 0,
    }
    if cache.persistent is not None:
        cache.persistent.reset_day_stats()

    workers = min(max(1, max_workers), len(universe))
    if workers == 1:
        for u in universe:
            sig, st = _scan_one_symbol(upstox, u, trade_date, cache)
            totals["daily_api"] += st["daily"]
            totals["m15_api"] += st["m15"]
            totals["gaps"] += st["gap"]
            totals["bb_evals"] += st["bb"]
            if sig:
                signals.append(sig)
    else:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futs = {
                pool.submit(_scan_one_symbol, upstox, u, trade_date, cache): u
                for u in universe
            }
            for fut in as_completed(futs):
                try:
                    sig, st = fut.result()
                except Exception as e:
                    u = futs[fut]
                    logger.debug("VM gap+BB scan %s: %s", u.get("symbol"), e)
                    continue
                totals["daily_api"] += st["daily"]
                totals["m15_api"] += st["m15"]
                totals["gaps"] += st["gap"]
                totals["bb_evals"] += st["bb"]
                if sig:
                    signals.append(sig)

    totals["elapsed_sec"] = round(time.monotonic() - t0, 2)
    totals["signals"] = len(signals)
    if cache.persistent is not None:
        ds = cache.persistent.day_stats()
        totals["cache_daily_hit"] = ds.get("daily_disk_hit", 0)
        totals["cache_m15_hit"] = ds.get("m15_disk_hit", 0)
        totals["cache_daily_api"] = ds.get("daily_api", 0)
        totals["cache_m15_api"] = ds.get("m15_api", 0)
    return signals, totals

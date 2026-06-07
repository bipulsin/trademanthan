"""Gap + Bollinger Band signal logic — uses shared ``signal_rules``."""
from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

from backend.services.volume_mismatch.candles import (
    BacktestDailyCache,
    fetch_first_15m_bar_for_session,
)
from backend.services.volume_mismatch.signal_rules import (
    BB_LENGTH,
    _f,
    bollinger_bands_as_of_session,
    compute_relative_volume,
    evaluate_vm_signal,
)

logger = logging.getLogger(__name__)

# Re-export for callers that imported from here.
MIN_GAP_PCT_LONG = -1.0
MIN_GAP_PCT_SHORT = 1.0
BB_COMPARE_FIELD = "open"


def evaluate_gap_bb_signal(
    *,
    symbol: str,
    future_symbol: str,
    instrument_key: str,
    first_bar: Dict[str, Any],
    previous_close: float,
    bb: Dict[str, float],
    relative_volume: Optional[float] = None,
) -> Optional[Dict[str, Any]]:
    """Thin wrapper around unified ``evaluate_vm_signal`` (backtest row shape)."""
    return evaluate_vm_signal(
        symbol=symbol,
        future_symbol=future_symbol,
        instrument_key=instrument_key,
        first_bar=first_bar,
        previous_close=previous_close,
        bb=bb,
        relative_volume=relative_volume,
        include_volume_split=True,
    )


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

    rel_vol: Optional[float] = None
    if daily_cache.persistent is not None:
        m15_bars = daily_cache.persistent.get_m15_candles(ik)
        rel_vol = compute_relative_volume(first_bar, m15_bars, trade_date)

    sig = evaluate_gap_bb_signal(
        symbol=sym,
        future_symbol=u.get("future_symbol") or sym,
        instrument_key=ik,
        first_bar=first_bar,
        previous_close=prev_close,
        bb=bb,
        relative_volume=rel_vol,
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

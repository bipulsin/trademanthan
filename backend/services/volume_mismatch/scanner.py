"""Daily 09:28 scan — first 10m volume mismatch candidates.

Live path: shared ``candle_cache`` only.
- First opening bar = aggregate of curr-month **5m** bars 09:15 + 09:20 (10m window).
- Daily BB / prev close from morning aux ``days/1`` warm.
No independent Upstox candle loops; no minutes/15 dependency.
"""
from __future__ import annotations

import logging
import time
from datetime import date
from typing import Any, Dict, List, Optional

from backend.database import SessionLocal
from backend.services.smart_futures_session_date import effective_session_date_ist_for_trend
from backend.services.volume_mismatch.candles import (
    BB_DAILY_DAYS_BACK,
    batch_fetch_candles,
    candle_fetch_stats,
    clear_candle_cache,
    first_10m_bar_from_5m,
    first_10m_volumes_by_session,
    previous_day_close,
)
from backend.services.volume_mismatch.constants import (
    DEFAULT_GAP_THRESHOLD_PCT,
    RELATIVE_VOLUME_LOOKBACK_SESSIONS,
)
from backend.services.volume_mismatch.repository import upsert_signal
from backend.services.volume_mismatch.signal_engine import evaluate_mismatch
from backend.services.volume_mismatch.signal_rules import bollinger_bands_as_of_session
from backend.services.volume_mismatch.universe import load_volume_mismatch_universe

logger = logging.getLogger(__name__)


def collect_volume_mismatch_signals_for_date(
    upstox: Any,
    universe: List[Dict[str, Any]],
    trade_date: date,
    *,
    gap_threshold: float = DEFAULT_GAP_THRESHOLD_PCT,
    max_workers: int = 24,
    allow_rest: bool = False,
) -> List[Dict[str, Any]]:
    """Run mismatch logic for one session (no DB write)."""
    if not universe:
        return []

    clear_candle_cache()
    keys = [u["instrument_key"] for u in universe if u.get("instrument_key")]

    # Opening range from centralized 5m cache (aggregated to first 10m).
    candles_5m = batch_fetch_candles(
        upstox,
        keys,
        "minutes/5",
        days_back=35,
        max_workers=max_workers,
        allow_rest=allow_rest,
    )
    candles_1d = batch_fetch_candles(
        upstox,
        keys,
        "days/1",
        days_back=BB_DAILY_DAYS_BACK,
        range_end_date=trade_date if allow_rest else None,
        max_workers=max_workers,
        allow_rest=allow_rest,
    )

    signals: List[Dict[str, Any]] = []
    for u in universe:
        ik = u["instrument_key"]
        sym = u["symbol"]
        bars_5 = candles_5m.get(ik) or []
        bars_1d = candles_1d.get(ik) or []
        first_bar = first_10m_bar_from_5m(bars_5, trade_date)
        if not first_bar:
            continue
        prev_close = previous_day_close(bars_1d, trade_date)
        if prev_close is None or prev_close <= 0:
            continue

        o = float(first_bar.get("open") or 0)
        if o <= 0 or o == prev_close:
            continue

        bb = bollinger_bands_as_of_session(bars_1d, trade_date)
        if not bb:
            continue

        hist_vols = first_10m_volumes_by_session(
            bars_5,
            before_date=trade_date,
            max_sessions=RELATIVE_VOLUME_LOOKBACK_SESSIONS,
        )
        rel_vol: Optional[float] = None
        try:
            today_vol = float(first_bar.get("volume") or 0)
        except (TypeError, ValueError):
            today_vol = 0.0
        if hist_vols:
            avg = sum(v for _, v in hist_vols) / len(hist_vols)
            if avg > 0:
                rel_vol = today_vol / avg

        sig = evaluate_mismatch(
            symbol=sym,
            future_symbol=u.get("future_symbol") or sym,
            instrument_key=ik,
            first_bar=first_bar,
            previous_close=prev_close,
            relative_volume=rel_vol,
            bb=bb,
            gap_threshold=gap_threshold,
        )
        if sig:
            row = sig.to_dict()
            row["trade_date"] = trade_date.isoformat()
            signals.append(row)
    return signals


def run_volume_mismatch_scan(
    *,
    trade_date: Optional[date] = None,
    gap_threshold: float = DEFAULT_GAP_THRESHOLD_PCT,
) -> Dict[str, Any]:
    t0 = time.perf_counter()
    sd = trade_date or effective_session_date_ist_for_trend()
    universe = load_volume_mismatch_universe()
    if not universe:
        return {"success": False, "error": "empty_universe", "trade_date": str(sd)}

    signals = collect_volume_mismatch_signals_for_date(
        None, universe, sd, gap_threshold=gap_threshold, allow_rest=False
    )
    for row in signals:
        row["entry_status"] = "WAITING"

    db = SessionLocal()
    try:
        for row in signals:
            upsert_signal(db, sd, row)
        db.commit()
    except Exception as e:
        db.rollback()
        logger.error("VM scan persist failed: %s", e, exc_info=True)
        raise
    finally:
        db.close()

    elapsed = round(time.perf_counter() - t0, 3)
    long_n = sum(1 for s in signals if s.get("direction") == "LONG")
    short_n = sum(1 for s in signals if s.get("direction") == "SHORT")
    cstats = candle_fetch_stats()
    logger.info(
        "Volume Mismatch scan %s: %s signals (LONG=%s SHORT=%s) in %.3fs / universe=%s "
        "(shared_hits=%s in-memory=%s api=%s) first_bar=10m_from_5m",
        sd,
        len(signals),
        long_n,
        short_n,
        elapsed,
        len(universe),
        cstats.get("shared_hit", 0),
        cstats.get("cache_hit", 0),
        cstats.get("api", 0),
    )
    return {
        "success": True,
        "trade_date": str(sd),
        "universe_count": len(universe),
        "signal_count": len(signals),
        "long_count": long_n,
        "short_count": short_n,
        "elapsed_sec": elapsed,
        "candle_stats": cstats,
        "first_bar_tf": "10m_from_5m",
    }

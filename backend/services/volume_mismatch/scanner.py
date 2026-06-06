"""Daily 09:30:30 scan — first 15m volume mismatch candidates."""
from __future__ import annotations

import logging
import time
from datetime import date
from typing import Any, Dict, List, Optional

from backend.config import settings
from backend.database import SessionLocal
from backend.services.smart_futures_session_date import effective_session_date_ist_for_trend
from backend.services.upstox_service import UpstoxService
from backend.services.volume_mismatch.candles import (
    batch_fetch_candles,
    clear_candle_cache,
    first_15m_bar_for_session,
    first_15m_volumes_by_session,
    previous_day_close,
)
from backend.services.volume_mismatch.constants import (
    DEFAULT_GAP_THRESHOLD_PCT,
    RELATIVE_VOLUME_LOOKBACK_SESSIONS,
)
from backend.services.volume_mismatch.repository import upsert_signal
from backend.services.volume_mismatch.signal_engine import evaluate_mismatch
from backend.services.volume_mismatch.universe import load_volume_mismatch_universe

logger = logging.getLogger(__name__)


def collect_volume_mismatch_signals_for_date(
    upstox: UpstoxService,
    universe: List[Dict[str, Any]],
    trade_date: date,
    *,
    gap_threshold: float = DEFAULT_GAP_THRESHOLD_PCT,
    max_workers: int = 24,
) -> List[Dict[str, Any]]:
    """Run mismatch logic for one session (no DB write)."""
    if not universe:
        return []

    clear_candle_cache()
    keys = [u["instrument_key"] for u in universe if u.get("instrument_key")]

    candles_15m = batch_fetch_candles(
        upstox,
        keys,
        "minutes/15",
        days_back=35,
        range_end_date=trade_date,
        max_workers=max_workers,
    )
    candles_1d = batch_fetch_candles(
        upstox,
        keys,
        "days/1",
        days_back=12,
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

        hist_vols = first_15m_volumes_by_session(
            bars_15,
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

    upstox = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    signals = collect_volume_mismatch_signals_for_date(
        upstox, universe, sd, gap_threshold=gap_threshold
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
    logger.info(
        "Volume Mismatch scan %s: %s signals (LONG=%s SHORT=%s) in %.3fs / universe=%s",
        sd,
        len(signals),
        long_n,
        short_n,
        elapsed,
        len(universe),
    )
    return {
        "success": True,
        "trade_date": str(sd),
        "universe_count": len(universe),
        "signal_count": len(signals),
        "long_count": long_n,
        "short_count": short_n,
        "elapsed_sec": elapsed,
    }

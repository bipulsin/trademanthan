"""Upstox market data helpers for Smart Futures."""
from __future__ import annotations

import logging
import os
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pytz

from backend.services.smart_futures.indicators import atr_wilder
from backend.services.upstox_service import upstox_service

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")


def _max_symbols() -> int:
    try:
        return int(os.getenv("SMART_FUTURES_MAX_SYMBOLS", "250"))
    except ValueError:
        return 250


def get_1h_candles(instrument_key: str, days_back: int = 30) -> Optional[List[Dict[str, Any]]]:
    return upstox_service.get_historical_candles_by_instrument_key(
        instrument_key, interval="hours/1", days_back=days_back
    )


def get_5m_candles(instrument_key: str, days_back: int = 5) -> Optional[List[Dict[str, Any]]]:
    return upstox_service.get_historical_candles_by_instrument_key(
        instrument_key, interval="minutes/5", days_back=days_back
    )


def get_15m_candles(instrument_key: str, days_back: int = 5) -> Optional[List[Dict[str, Any]]]:
    return upstox_service.get_historical_candles_by_instrument_key(
        instrument_key, interval="minutes/15", days_back=days_back
    )


def get_1m_candles(instrument_key: str, days_back: int = 3) -> Optional[List[Dict[str, Any]]]:
    return upstox_service.get_historical_candles_by_instrument_key(
        instrument_key, interval="minutes/1", days_back=days_back
    )


def brick_size_from_1h(instrument_key: str) -> Optional[float]:
    """ATR(14) on 1-hour candles as Renko brick size (spec: ATR 1H period 14)."""
    candles = get_1h_candles(instrument_key, days_back=45)
    if not candles or len(candles) < 20:
        return None
    candles.sort(key=lambda c: c.get("timestamp") or "")
    highs = [float(c["high"]) for c in candles]
    lows = [float(c["low"]) for c in candles]
    closes = [float(c["close"]) for c in candles]
    atr_val = atr_wilder(highs, lows, closes, period=14)
    if atr_val is None or atr_val <= 0:
        return None
    return round(atr_val, 6)


def closes_from_candles(candles: Sequence[Dict[str, Any]]) -> List[float]:
    candles = sorted(candles, key=lambda c: c.get("timestamp") or "")
    return [float(c["close"]) for c in candles]


def prefilter_gap_volume_atr(
    instrument_key: str,
    candles_5m: Sequence[Dict[str, Any]],
    candles_15m: Sequence[Dict[str, Any]],
    prev_close: float,
) -> Tuple[bool, str, Dict[str, Any]]:
    """
    Layer 1:
    - Gap: abs(open-prev)/prev >= 0.7%
    - First 15m volume vs prior day first 15m (proxy using 5m candles)
    - Move >= 0.5 * ATR(14,15m)
    """
    meta: Dict[str, Any] = {}
    if not candles_5m or len(candles_5m) < 10:
        return False, "no_5m_data", meta
    c5 = sorted(candles_5m, key=lambda c: c.get("timestamp") or "")
    today = datetime.now(IST).date()
    # session candles for today (IST date in timestamp string)
    def _day(ts: str) -> Optional[date]:
        try:
            if not ts:
                return None
            part = str(ts).replace("Z", "+00:00")
            dt = datetime.fromisoformat(part)
            if dt.tzinfo is None:
                dt = pytz.UTC.localize(dt)
            return dt.astimezone(IST).date()
        except Exception:
            return None

    today_c = [x for x in c5 if _day(x.get("timestamp") or "") == today]
    if len(today_c) < 3:
        today_c = c5[-40:]  # fallback: recent tail
    o0 = float(today_c[0]["open"])
    c0 = float(today_c[0]["close"])
    gap_pct = abs(o0 - prev_close) / prev_close * 100.0 if prev_close > 0 else 0.0
    meta["gap_pct"] = round(gap_pct, 4)
    if gap_pct < 0.7:
        return False, "gap_filter", meta

    vol_first15 = sum(float(x.get("volume") or 0) for x in today_c[:3])
    prior_5m = [x for x in c5 if _day(x.get("timestamp") or "") and _day(x.get("timestamp") or "") < today]
    prior_sessions: List[List[Dict[str, Any]]] = []
    if prior_5m:
        d0 = _day(prior_5m[-1].get("timestamp") or "")
        bucket: List[Dict[str, Any]] = []
        for x in prior_5m[-200:]:
            dx = _day(x.get("timestamp") or "")
            if dx != d0 and bucket:
                prior_sessions.append(bucket)
                bucket = []
                d0 = dx
            bucket.append(x)
        if bucket:
            prior_sessions.append(bucket)
    vol_prev_avg = None
    if prior_sessions:
        vols = []
        for sess in prior_sessions[-5:]:
            vols.append(sum(float(x.get("volume") or 0) for x in sess[:3]))
        if vols:
            vol_prev_avg = sum(vols) / len(vols)
    meta["vol_first15"] = vol_first15
    meta["vol_prev_avg"] = vol_prev_avg
    if vol_prev_avg and vol_prev_avg > 0 and vol_first15 <= vol_prev_avg:
        return False, "volume_spike", meta

    if not candles_15m or len(candles_15m) < 16:
        return False, "no_15m_atr", meta
    c15 = sorted(candles_15m, key=lambda c: c.get("timestamp") or "")
    highs = [float(x["high"]) for x in c15]
    lows = [float(x["low"]) for x in c15]
    closes = [float(x["close"]) for x in c15]
    atr15 = atr_wilder(highs, lows, closes, period=14)
    if atr15 is None or atr15 <= 0:
        return False, "atr15", meta
    move = abs(float(c5[-1]["close"]) - o0)
    meta["move"] = move
    meta["atr15"] = atr15
    if move < 0.5 * atr15:
        return False, "intraday_move", meta

    return True, "ok", meta


def quote_prev_close_and_open(instrument_key: str) -> Tuple[Optional[float], Optional[float]]:
    """Best-effort prev close and today's open from OHLC API."""
    ohlc = upstox_service.get_ohlc_data(instrument_key) or {}
    pc = ohlc.get("close")
    op = ohlc.get("open")
    try:
        prev_close = float(pc) if pc is not None else None
        open_ = float(op) if op is not None else None
    except (TypeError, ValueError):
        prev_close, open_ = None, None
    return prev_close, open_

"""
Shared pre-market scanner metrics (aligned with test_premkt_scanner.py).

Uses Upstox historical daily + 5m only: IST session dates, holiday-safe prior session,
52w range from history before session date, gap at open, momentum from prior session 5m EMA spread.
"""
from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any, Dict, List, Optional

import pytz

from backend.services.smart_futures_picker.indicators import compute_obv_slope_daily, ema_slope_norm_m5
from backend.services.upstox_service import UpstoxService

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")

# Weighted composite (same as test harness / product spec)
W_OBV = 0.30
W_GAP = 0.25
W_RANGE = 0.25
W_MOM = 0.20


def parse_candle_date_ist(ts: Any) -> Optional[date]:
    """Calendar date in Asia/Kolkata for an Upstox candle timestamp."""
    if ts is None:
        return None
    s = str(ts).strip()
    try:
        if "T" in s:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = IST.localize(dt)
            else:
                dt = dt.astimezone(IST)
            return dt.date()
        return date.fromisoformat(s[:10])
    except ValueError:
        return None


def sort_candles(candles: Optional[List[dict]]) -> List[dict]:
    if not candles:
        return []
    return sorted(candles, key=lambda c: str(c.get("timestamp") or ""))


def score_premarket_raw(
    upstox: UpstoxService,
    stock: str,
    instrument_key: str,
    session_date: date,
) -> Dict[str, Any]:
    """
    Return raw OBV / gap / range / momentum for one equity key on ``session_date`` (NSE session).

    On error, ``{"error": "..."}``; otherwise metrics + ``stock``, ``instrument_key``.
    """
    ikey = (instrument_key or "").strip()
    out: Dict[str, Any] = {"stock": stock.upper().strip(), "instrument_key": ikey, "error": None}
    if not ikey:
        out["error"] = "missing instrument_key"
        return out

    sim = session_date

    try:
        daily_raw = upstox.get_historical_candles_by_instrument_key(
            ikey, interval="days/1", days_back=320, range_end_date=sim
        )
        daily = sort_candles(daily_raw)
        if len(daily) < 30:
            out["error"] = f"insufficient daily history ({len(daily)})"
            return out

        def _cd(c: dict) -> Optional[date]:
            return parse_candle_date_ist(c.get("timestamp"))

        completed_before_sim = [c for c in daily if _cd(c) is not None and _cd(c) < sim]
        if len(completed_before_sim) < 11:
            out["error"] = f"need 11+ daily bars before {sim} (got {len(completed_before_sim)})"
            return out

        pre_eff = _cd(completed_before_sim[-1])
        prev_close = float(completed_before_sim[-1]["close"])
        if prev_close <= 0:
            out["error"] = "bad prev_close"
            return out

        tail10 = completed_before_sim[-10:]
        closes = [float(x["close"]) for x in tail10]
        vols = [float(x.get("volume") or 0) for x in tail10]
        obv_slope = compute_obv_slope_daily(closes, vols)

        highs = [float(x["high"]) for x in completed_before_sim]
        lows = [float(x["low"]) for x in completed_before_sim]
        w52_hi = max(highs)
        w52_lo = min(lows)
        if w52_hi - w52_lo <= 1e-9:
            out["error"] = "degenerate 52w range"
            return out

        daily_to_sim = upstox.get_historical_candles_by_instrument_key(
            ikey, interval="days/1", days_back=5, range_end_date=sim
        )
        daily_to_sim = sort_candles(daily_to_sim)
        sim_bar = None
        for c in daily_to_sim:
            if parse_candle_date_ist(c.get("timestamp")) == sim:
                sim_bar = c
                break

        day_open = float(sim_bar.get("open") or 0) if sim_bar else 0.0
        if day_open <= 0:
            m5today = upstox.get_historical_candles_by_instrument_key(
                ikey, interval="minutes/5", days_back=2, range_end_date=sim
            )
            m5today = sort_candles(m5today)
            for c in m5today:
                ts = str(c.get("timestamp") or "")
                try:
                    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                    if dt.tzinfo is None:
                        dt = IST.localize(dt)
                    else:
                        dt = dt.astimezone(IST)
                except Exception:
                    continue
                if dt.date() != sim:
                    continue
                if dt.hour < 9 or (dt.hour == 9 and dt.minute < 15):
                    continue
                o = float(c.get("open") or 0)
                if o > 0:
                    day_open = o
                    break

        if day_open <= 0:
            out["error"] = f"no session open for {sim} (no daily or intraday 5m yet)"
            return out

        gap_pct = (day_open - prev_close) / prev_close * 100.0
        gap_strength = abs(gap_pct)
        range_pos = (day_open - w52_lo) / (w52_hi - w52_lo + 1e-12)
        range_pos = max(0.0, min(1.0, float(range_pos)))

        assert pre_eff is not None
        m5 = upstox.get_historical_candles_by_instrument_key(
            ikey, interval="minutes/5", days_back=5, range_end_date=pre_eff
        )
        m5 = sort_candles(m5)
        closes_m5: List[float] = []
        for c in m5:
            ts = str(c.get("timestamp") or "")
            if not ts:
                continue
            try:
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = IST.localize(dt)
                else:
                    dt = dt.astimezone(IST)
            except Exception:
                continue
            if dt.date() != pre_eff:
                continue
            if dt.hour < 9 or (dt.hour == 9 and dt.minute < 15):
                continue
            if dt.hour > 15 or (dt.hour == 15 and dt.minute > 35):
                continue
            closes_m5.append(float(c["close"]))

        if len(closes_m5) >= 20:
            mom = ema_slope_norm_m5(closes_m5)
        else:
            mom = 0.0

        out["obv_slope"] = float(obv_slope)
        out["gap_pct_signed"] = float(gap_pct)
        out["gap_strength"] = float(gap_strength)
        out["range_position"] = float(range_pos)
        out["momentum"] = float(mom)
        out["ltp"] = float(day_open)
        return out
    except Exception as e:
        logger.debug("premarket_scoring skip %s: %s", stock, e)
        out["error"] = str(e)
        return out


def min_max_norm(values: List[float]) -> List[float]:
    if not values:
        return []
    lo = min(values)
    hi = max(values)
    span = hi - lo
    if span <= 1e-12:
        return [0.5 for _ in values]
    return [(v - lo) / span for v in values]


def composite_weighted(
    obv_n: float,
    gap_n: float,
    range_n: float,
    mom_n: float,
) -> float:
    return W_OBV * obv_n + W_GAP * gap_n + W_RANGE * range_n + W_MOM * mom_n

"""Setup qualification with explicit reject reasons (for diagnostics)."""
from __future__ import annotations

from datetime import date, time
from typing import Any, Dict, List, Optional, Tuple

import pytz

from backend.services.open_low_15m.candles import native_first_15m_bar
from backend.services.open_low_15m.config import (
    OPEN_LOW_TOL_PCT,
    RANGE_ATR_MULT,
    RISK_INR_MAX,
    RISK_INR_MIN,
)
from backend.services.open_low_15m.indicators import bar_indicators
from backend.services.volume_mismatch.candles import _parse_ts

IST = pytz.timezone("Asia/Kolkata")


def _f(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _open_equals_low(o: float, l: float) -> bool:
    if o <= 0:
        return False
    return abs(o - l) <= o * (OPEN_LOW_TOL_PCT / 100.0)


def evaluate_setup(
    *,
    symbol: str,
    future_symbol: str,
    instrument_key: str,
    session_date: date,
    candles_15m: List[Dict[str, Any]],
    prev_close: float,
    lot_size: int,
    max_gap_pct: float = 2.0,
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """Return (setup_dict, reject_reason). Uses native 09:15 15m bar + intraday EMA10/VWAP/ST."""
    first = native_first_15m_bar(candles_15m, session_date)
    if first is None:
        return None, "no_native_15m_bar"

    o, h, l, close_px = _f(first.get("open")), _f(first.get("high")), _f(first.get("low")), _f(first.get("close"))
    if o <= 0:
        return None, "bad_open"
    if not _open_equals_low(o, l):
        return None, "open_not_low"

    if prev_close > 0:
        gap_pct = abs(o - prev_close) / prev_close * 100.0
        if gap_pct > max_gap_pct:
            return None, f"gap_{gap_pct:.2f}pct"

    warmup: List[Dict[str, Any]] = []
    for candle in sorted(candles_15m, key=lambda x: str(x.get("timestamp") or "")):
        ts = _parse_ts(candle.get("timestamp"))
        if ts is None:
            continue
        t = ts.astimezone(IST)
        if t.date() > session_date:
            continue
        if t.date() == session_date and t.time() > time(9, 15):
            break
        warmup.append(candle)
    if first not in warmup:
        warmup.append(first)

    hist_h = [_f(b.get("high")) for b in warmup]
    hist_l = [_f(b.get("low")) for b in warmup]
    hist_c = [_f(b.get("close")) for b in warmup]
    hist_v = [_f(b.get("volume")) for b in warmup]
    ind0 = bar_indicators(hist_h, hist_l, hist_c, hist_v)

    ema10 = ind0.get("ema10")
    if ema10 is not None and close_px <= ema10:
        return None, "close_below_ema10"
    if close_px <= ind0["vwap"]:
        return None, "close_below_vwap"
    if ind0["supertrend_dir"] != 1:
        return None, "supertrend_bearish"

    rng = h - l
    atr14 = ind0.get("atr14")
    use_alt_sl = atr14 is not None and rng > RANGE_ATR_MULT * float(atr14)
    sl_primary = l
    sl_alt = l + 0.5 * rng
    entry_px = h
    sl_used = sl_alt if use_alt_sl else sl_primary
    sl_type = "alternative" if use_alt_sl else "primary"
    r_dist = entry_px - sl_used
    if r_dist <= 0:
        return None, "invalid_r"

    risk_inr = r_dist * lot_size
    if risk_inr < RISK_INR_MIN:
        return None, f"risk_below_{RISK_INR_MIN:.0f}"
    if risk_inr > RISK_INR_MAX:
        return None, f"risk_above_{RISK_INR_MAX:.0f}"

    gain_pct = (close_px - o) / o * 100.0 if o > 0 else 0.0
    return {
        "symbol": symbol,
        "future_symbol": future_symbol,
        "instrument_key": instrument_key,
        "session_date": session_date.isoformat(),
        "setup_open": o,
        "setup_high": h,
        "setup_low": l,
        "setup_close": close_px,
        "setup_range": rng,
        "first_15m_gain_pct": round(gain_pct, 4),
        "prev_close": prev_close,
        "entry_trigger": entry_px,
        "sl_type": sl_type,
        "sl_price": sl_used,
        "sl_primary": sl_primary,
        "sl_alternative": sl_alt,
        "r_distance": r_dist,
        "lot_size": lot_size,
        "risk_inr": round(risk_inr, 2),
        "atr14_at_setup": atr14,
        "use_alt_sl": use_alt_sl,
        "ema10_at_setup": ema10,
        "vwap_at_setup": ind0.get("vwap"),
    }, None

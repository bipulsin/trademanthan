"""Conviction board signal components from 5m candles (slope, accumulation, whipsaw)."""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

from backend.services.kavach_volume import last_closed_bar_index
from backend.services.relative_strength_scanner import _f, _parse_ist_date, _sorted_candles
from backend.services.vajra.indicators import cumulative_vwap, ema_series

logger = logging.getLogger(__name__)


def _today_slice(candles: List[Dict]) -> Tuple[List[Dict], int]:
    """Return (today_candles, first_today_index in full list)."""
    if not candles:
        return [], 0
    last_date = _parse_ist_date(candles[-1].get("timestamp"))
    first_today = 0
    for i, c in enumerate(candles):
        if _parse_ist_date(c.get("timestamp")) == last_date:
            first_today = i
            break
    return candles[first_today:], first_today


def _aggregate_10m_closes(candles: List[Dict]) -> List[float]:
    """Pair 5m bars into 10m closes (use 2nd bar close of each pair)."""
    closes = [_f(c.get("close")) for c in candles]
    out: List[float] = []
    for i in range(1, len(closes), 2):
        out.append(closes[i])
    if len(closes) % 2 == 1 and len(closes) >= 1:
        out.append(closes[-1])
    return out


def _vwap_series_today(candles: List[Dict]) -> List[float]:
    today, _ = _today_slice(candles)
    if not today:
        return []
    highs = [_f(c.get("high")) for c in today]
    lows = [_f(c.get("low")) for c in today]
    closes = [_f(c.get("close")) for c in today]
    vols = [_f(c.get("volume")) for c in today]
    return cumulative_vwap(highs, lows, closes, vols)


def whipsaw_cross_count(candles: List[Dict], *, lookback_10m_bars: int = 9) -> int:
    """Count VWAP crosses by 10-min close over trailing ~90 minutes."""
    today, _ = _today_slice(candles)
    if len(today) < 4:
        return 0
    vwap_s = _vwap_series_today(candles)
    if len(vwap_s) < 2:
        return 0
    # Map 10m close index to approximate vwap index (2 bars per 10m)
    closes_10m = _aggregate_10m_closes(today)
    n = min(len(closes_10m), len(vwap_s) // 2 + 1)
    if n < 2:
        return 0
    start = max(0, n - lookback_10m_bars)
    crosses = 0
    for i in range(start + 1, n):
        vwap_i = vwap_s[min(i * 2, len(vwap_s) - 1)]
        vwap_prev = vwap_s[min((i - 1) * 2, len(vwap_s) - 1)]
        prev_above = closes_10m[i - 1] > vwap_prev
        curr_above = closes_10m[i] > vwap_i
        if prev_above != curr_above:
            crosses += 1
    return crosses


def normalized_vwap_slope(
    candles: List[Dict], atr_daily_pct: float, cfg: Dict[str, Any]
) -> float:
    """VWAP change over ~30 min / ATR, scaled 0–100."""
    today, first_today = _today_slice(candles)
    vwap_s = _vwap_series_today(candles)
    closed = last_closed_bar_index(candles)
    if closed < first_today or len(vwap_s) < 7:
        return 0.0
    # ~30 min = 6 five-minute bars (today-relative indices)
    idx_now = min(len(vwap_s) - 1, closed - first_today)
    idx_prev = max(0, idx_now - 6)
    if idx_now <= idx_prev:
        return 0.0
    price = _f(candles[closed].get("close"), 1.0)
    atr = price * max(atr_daily_pct, 0.001) / 100.0
    if atr <= 0:
        return 0.0
    slope = (vwap_s[idx_now] - vwap_s[idx_prev]) / atr
    ref = float(cfg.get("slope_ref_atr_per_30m") or 0.5)
    raw = abs(slope) / ref * 100.0
    return max(0.0, min(100.0, raw))


def _obv_series(closes: List[float], volumes: List[float]) -> List[float]:
    obv = [0.0]
    for i in range(1, len(closes)):
        if closes[i] > closes[i - 1]:
            obv.append(obv[-1] + volumes[i])
        elif closes[i] < closes[i - 1]:
            obv.append(obv[-1] - volumes[i])
        else:
            obv.append(obv[-1])
    return obv


def accumulation_signal(
    candles: List[Dict], side: str, cfg: Dict[str, Any]
) -> Tuple[float, bool, bool]:
    """
    Returns (score 0/40/100, accum_active, low_confidence_bootstrap).
    2-of-3: rel volume, price efficiency, OBV divergence.
    """
    today, _ = _today_slice(candles)
    if len(today) < 8:
        return 0.0, False, True

    closed = last_closed_bar_index(candles)
    if closed < 0:
        return 0.0, False, True

    # Last ~30-60 min window (6–12 bars)
    win = min(12, len(today))
    window = today[-win:]
    vols = [_f(c.get("volume")) for c in window]
    closes = [_f(c.get("close")) for c in window]
    total_vol = sum(vols) or 1.0
    net_chg = abs(closes[-1] - closes[0])
    avg_vol = total_vol / len(vols)

    # Bootstrap: compare to session average volume per bar (no 10-day baseline yet)
    session_vols = [_f(c.get("volume")) for c in today]
    session_avg = sum(session_vols) / max(len(session_vols), 1)
    rel_vol = avg_vol / session_avg if session_avg > 0 else 1.0
    vol_mult = float(cfg.get("accum_vol_multiple") or 1.5)
    low_conf = True  # until intraday baseline table exists

    hits = 0
    if rel_vol >= vol_mult:
        hits += 1

    # Price efficiency: low movement per volume unit vs median
    efficiency = net_chg / total_vol if total_vol > 0 else 0.0
    eff_threshold = net_chg / (total_vol * 2) if total_vol else 0.0
    if efficiency < max(eff_threshold, 1e-9) and rel_vol >= 1.2:
        hits += 1

    is_bull = side.upper() in ("BULL", "LONG", "BULLISH")
    obv = _obv_series(closes, vols)
    price_slope = closes[-1] - closes[0]
    obv_slope = obv[-1] - obv[0] if len(obv) >= 2 else 0.0
    if is_bull and obv_slope > 0 and price_slope <= closes[0] * 0.001:
        hits += 1
    if not is_bull and obv_slope < 0 and price_slope >= -closes[0] * 0.001:
        hits += 1

    if hits >= 2:
        return 100.0, True, low_conf
    if hits == 1:
        return 40.0, False, low_conf
    return 0.0, False, low_conf


def ema10_10min(candles: List[Dict]) -> Optional[float]:
    """EMA(10) of 10-min closes for passive exit reference."""
    today, _ = _today_slice(candles)
    closes_10m = _aggregate_10m_closes(today)
    if len(closes_10m) < 10:
        return None
    return ema_series(closes_10m, 10)[-1]


def gap_history_atr(candles: List[Dict], atr_daily_pct: float) -> List[float]:
    """Last 3 closed-bar EMA5-VWAP gaps in ATR units (for radar convergence)."""
    today, first = _today_slice(candles)
    if len(today) < 3:
        return []
    all_c = _sorted_candles(candles)
    closes = [_f(c.get("close")) for c in all_c]
    ema5 = ema_series(closes, 5)
    vwap_s = _vwap_series_today(candles)
    closed = last_closed_bar_index(candles)
    if closed < 0:
        return []
    price = _f(all_c[closed].get("close"), 1.0)
    atr = price * max(atr_daily_pct, 0.001) / 100.0
    if atr <= 0:
        return []
    gaps: List[float] = []
    for off in (0, 1, 2):
        idx = closed - off
        if idx < first or idx >= len(ema5):
            continue
        vi = min(idx - first, len(vwap_s) - 1)
        if vi < 0:
            continue
        gaps.append((ema5[idx] - vwap_s[vi]) / atr)
    return list(reversed(gaps))


def compute_symbol_signals(
    candles: Optional[List[Dict]],
    *,
    side: str,
    atr_daily_pct: float,
    cfg: Dict[str, Any],
) -> Dict[str, Any]:
    """All conviction signal fields for one symbol."""
    if not candles or len(candles) < 40:
        return {
            "slope_component": 0.0,
            "accum_component": 0.0,
            "accum_active": False,
            "accum_low_confidence": True,
            "whipsaw_cross_count": 0,
            "ema10_10m": None,
            "gap_history": [],
        }
    candles = _sorted_candles(candles)
    slope = normalized_vwap_slope(candles, atr_daily_pct, cfg)
    accum, accum_active, low_conf = accumulation_signal(candles, side, cfg)
    crosses = whipsaw_cross_count(candles)
    return {
        "slope_component": round(slope, 2),
        "accum_component": round(accum, 2),
        "accum_active": accum_active,
        "accum_low_confidence": low_conf,
        "whipsaw_cross_count": crosses,
        "ema10_10m": ema10_10min(candles),
        "gap_history": gap_history_atr(candles, atr_daily_pct),
    }

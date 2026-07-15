"""Shared VWAP-quality scoring for Expansion Watch and READY NOW gating.

Single implementation for slope steepening + flip-flop / whipsaw checks so
checklist and expansion detector cannot drift apart.
"""
from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pytz

from backend.services.kavach_10m import aggregate_10m_bars, last_closed_10m_pair_end_idx
from backend.services.kavach_momentum_ignition_validate import THRESHOLD_VWAP_SLOPE
from backend.services.kavach_volume import last_closed_bar_index
from backend.services.relative_strength_scanner import _f, _sorted_candles
from backend.services.rs_conviction_config import get_config
from backend.services.rs_conviction_signals import (
    _today_slice,
    _vwap_series_today,
    normalized_vwap_slope,
    whipsaw_cross_count,
)

IST = pytz.timezone("Asia/Kolkata")

# Default: allow at most 1 VWAP cross in lookback; 2+ = unstable (matches conviction soft penalty start).
DEFAULT_MAX_WHIPSAW_CROSSES = 1


def ready_vwap_quality_gate_enabled() -> bool:
    """Live READY filter; default off until shadow day clears (READY_VWAP_QUALITY_GATE=1)."""
    return os.environ.get("READY_VWAP_QUALITY_GATE", "0").strip().lower() in (
        "1",
        "true",
        "yes",
    )


def signed_vwap_slope_atr(candles: List[Dict], atr_daily_pct: float) -> float:
    """Raw signed VWAP change over ~30m / ATR (same window as normalized_vwap_slope)."""
    today, first_today = _today_slice(candles)
    vwap_s = _vwap_series_today(candles)
    closed = last_closed_bar_index(candles)
    if closed < first_today or len(vwap_s) < 7:
        return 0.0
    idx_now = min(len(vwap_s) - 1, closed - first_today)
    idx_prev = max(0, idx_now - 6)
    if idx_now <= idx_prev:
        return 0.0
    price = _f(candles[closed].get("close"), 1.0)
    atr = price * max(atr_daily_pct, 0.001) / 100.0
    if atr <= 0:
        return 0.0
    return (vwap_s[idx_now] - vwap_s[idx_prev]) / atr


def vwap_extension_pct(candles: List[Dict[str, Any]]) -> Optional[float]:
    """Signed (close - vwap) / vwap on the last closed bar. Shadow research only."""
    if not candles:
        return None
    candles = _sorted_candles(candles)
    today, first_today = _today_slice(candles)
    vwap_s = _vwap_series_today(candles)
    closed = last_closed_bar_index(candles)
    if closed < 0 or closed < first_today or not vwap_s:
        return None
    idx = min(len(vwap_s) - 1, closed - first_today)
    if idx < 0:
        return None
    vwap = float(vwap_s[idx] or 0.0)
    if vwap <= 0:
        return None
    close = _f(candles[closed].get("close"))
    if close is None:
        return None
    return round((float(close) - vwap) / vwap, 6)


def vwap_slope_steepening(
    candles: List[Dict[str, Any]],
    *,
    side: str,
    atr_daily_pct: float,
    cfg: Optional[Dict[str, Any]] = None,
) -> Tuple[bool, float, float]:
    """Validated VWAP-slope steepening in trade direction.

    Returns (passed, normalized_score_0_100, signed_slope_atr).
    """
    cfg = cfg or get_config()
    score = float(normalized_vwap_slope(candles, atr_daily_pct, cfg))
    signed = float(signed_vwap_slope_atr(candles, atr_daily_pct))
    is_short = (side or "LONG").upper() in ("SHORT", "BEAR", "BEARISH")
    direction_ok = signed < 0 if is_short else signed > 0
    passed = score >= THRESHOLD_VWAP_SLOPE and direction_ok
    return passed, score, signed


def _parse_ts(val: Any) -> Optional[datetime]:
    if val is None:
        return None
    if isinstance(val, datetime):
        dt = val
    else:
        try:
            dt = datetime.fromisoformat(str(val).replace("Z", "+00:00"))
        except Exception:
            return None
    if dt.tzinfo is None:
        return IST.localize(dt)
    return dt.astimezone(IST)


def adverse_confirmed_vwap_close_since(
    candles: List[Dict[str, Any]],
    *,
    side: str,
    since: Optional[Any] = None,
) -> Dict[str, Any]:
    """True when any confirmed 10m close is on the wrong side of VWAP since ``since``.

    ``since`` = first-qualified / promoted_at; if None, uses session open (today slice).
    """
    if not candles:
        return {"flip_flop": False, "adverse_closes": 0, "first_adverse_hm": None}
    candles = _sorted_candles(candles)
    pair_end = last_closed_10m_pair_end_idx(candles)
    bars = [b for b in aggregate_10m_bars(candles) if b["end_5m_idx"] <= pair_end]
    if not bars:
        return {"flip_flop": False, "adverse_closes": 0, "first_adverse_hm": None}

    vwap_s = _vwap_series_today(candles)
    today, first_today = _today_slice(candles)
    since_dt = _parse_ts(since)
    is_long = (side or "LONG").upper() not in ("SHORT", "BEAR", "BEARISH")
    adverse = 0
    first_hm = None

    for b in bars:
        end_idx = int(b.get("end_5m_idx") or -1)
        if end_idx < 0 or end_idx >= len(candles):
            continue
        ts = _parse_ts(candles[end_idx].get("timestamp"))
        if ts is None:
            continue
        if since_dt is not None and ts < since_dt:
            continue
        if today and first_today >= 0:
            # Restrict to today session bars
            if end_idx < first_today:
                continue
        close = float(b["close"])
        # Map 5m end index into today-relative VWAP series
        vwap_i = end_idx - first_today if first_today >= 0 else end_idx
        if not vwap_s or vwap_i < 0 or vwap_i >= len(vwap_s):
            continue
        vwap = float(vwap_s[vwap_i])
        wrong = (close < vwap) if is_long else (close > vwap)
        if wrong:
            adverse += 1
            if first_hm is None:
                first_hm = ts.strftime("%H:%M")

    return {
        "flip_flop": adverse > 0,
        "adverse_closes": adverse,
        "first_adverse_hm": first_hm,
    }


def _adx_14_at_forming(candles: List[Dict[str, Any]], as_of: datetime) -> Optional[float]:
    """Kavach live ADX: 10m bars, length=14, include_forming — same as entry gate."""
    from backend.services.kavach_10m import (
        _10m_series_upto_live,
        last_live_10m_pair_end_idx,
    )
    from backend.services.smart_futures_picker.indicators import adx_value

    sliced = [c for c in _sorted_candles(candles) if (_parse_ts(c.get("timestamp")) or as_of) <= as_of]
    if not sliced or len(sliced) < 40:
        return None
    pair_end = last_live_10m_pair_end_idx(sliced, now=as_of)
    if pair_end < 0:
        return None
    bars_10m = _10m_series_upto_live(sliced, pair_end)
    if len(bars_10m) < 5:
        return None
    highs = [float(b["high"]) for b in bars_10m]
    lows = [float(b["low"]) for b in bars_10m]
    closes = [float(b["close"]) for b in bars_10m]
    return adx_value(highs, lows, closes, 14)


def consecutive_steep_bars(
    candles: List[Dict[str, Any]],
    *,
    atr_daily_pct: float,
    n_bars: int = 3,
    now: Optional[datetime] = None,
    cfg: Optional[Dict[str, Any]] = None,
    require_adx_gt: Optional[float] = None,
) -> Dict[str, Any]:
    """Last ``n_bars`` closed 5m bars: slope ≥50 in one consistent direction.

    When ``require_adx_gt`` is set, each bar must also have Kavach ADX(14) above it.
    """
    cfg = cfg or get_config()
    fail: Dict[str, Any] = {
        "ok": False,
        "direction": None,
        "count": 0,
        "slope_scores": [],
        "adx_values": [],
        "bar_timestamps": [],
        "signed_slopes": [],
    }
    if not candles or len(candles) < 20 or n_bars < 1:
        return fail
    candles = _sorted_candles(candles)
    closed_idx = last_closed_bar_index(candles, now=now)
    if closed_idx < 0 or closed_idx + 1 < n_bars:
        return fail

    scores: List[float] = []
    signed_list: List[float] = []
    adxs: List[Optional[float]] = []
    stamps: List[str] = []
    direction: Optional[str] = None

    for offset in range(n_bars - 1, -1, -1):
        idx = closed_idx - offset
        as_of = _parse_ts(candles[idx].get("timestamp"))
        if as_of is None:
            return fail
        sliced = [c for c in candles[: idx + 1]]
        score = float(normalized_vwap_slope(sliced, atr_daily_pct, cfg))
        signed = float(signed_vwap_slope_atr(sliced, atr_daily_pct))
        if score < THRESHOLD_VWAP_SLOPE or signed == 0:
            return {**fail, "count": len(scores), "slope_scores": scores}
        bar_dir = "SHORT" if signed < 0 else "LONG"
        if direction is None:
            direction = bar_dir
        elif bar_dir != direction:
            return {**fail, "count": len(scores), "slope_scores": scores}
        adx_v: Optional[float] = None
        if require_adx_gt is not None:
            adx_v = _adx_14_at_forming(candles, as_of)
            if adx_v is None or adx_v <= require_adx_gt:
                return {
                    **fail,
                    "count": len(scores),
                    "slope_scores": scores,
                    "adx_values": adxs + [adx_v],
                }
        scores.append(round(score, 2))
        signed_list.append(round(signed, 4))
        adxs.append(round(adx_v, 2) if adx_v is not None else None)
        stamps.append(as_of.isoformat())

    return {
        "ok": True,
        "direction": direction,
        "count": n_bars,
        "slope_scores": scores,
        "adx_values": adxs,
        "bar_timestamps": stamps,
        "signed_slopes": signed_list,
    }


def consecutive_steep_adx_window(
    candles: List[Dict[str, Any]],
    *,
    atr_daily_pct: float,
    n_bars: int = 3,
    now: Optional[datetime] = None,
    cfg: Optional[Dict[str, Any]] = None,
    adx_min: float = 20.0,
) -> Dict[str, Any]:
    """Promotion gate: N consecutive steep bars AND ADX > adx_min on each."""
    return consecutive_steep_bars(
        candles,
        atr_daily_pct=atr_daily_pct,
        n_bars=n_bars,
        now=now,
        cfg=cfg,
        require_adx_gt=adx_min,
    )


def vwap_slope_deterioration_bars(
    candles: List[Dict[str, Any]],
    *,
    side: str,
    atr_daily_pct: float,
    n_bars: int = 2,
    now: Optional[datetime] = None,
    cfg: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """True when last ``n_bars`` closed 5m bars each fail steep-in-direction.

    Fail = slope_score < 50 OR signed slope against trade direction.
    """
    cfg = cfg or get_config()
    out: Dict[str, Any] = {
        "deteriorated": False,
        "slope_scores": [],
        "signed_slopes": [],
        "bar_timestamps": [],
    }
    if not candles or n_bars < 1:
        return out
    candles = _sorted_candles(candles)
    closed_idx = last_closed_bar_index(candles, now=now)
    if closed_idx < 0 or closed_idx + 1 < n_bars:
        return out
    is_short = (side or "LONG").upper() in ("SHORT", "BEAR", "BEARISH")
    scores: List[float] = []
    signed_list: List[float] = []
    stamps: List[str] = []
    for offset in range(n_bars - 1, -1, -1):
        idx = closed_idx - offset
        as_of = _parse_ts(candles[idx].get("timestamp"))
        if as_of is None:
            return out
        sliced = candles[: idx + 1]
        score = float(normalized_vwap_slope(sliced, atr_daily_pct, cfg))
        signed = float(signed_vwap_slope_atr(sliced, atr_daily_pct))
        scores.append(round(score, 2))
        signed_list.append(round(signed, 4))
        stamps.append(as_of.isoformat() if as_of else "")
        direction_ok = signed < 0 if is_short else signed > 0
        steep_ok = score >= THRESHOLD_VWAP_SLOPE and direction_ok
        if steep_ok:
            out["slope_scores"] = scores
            out["signed_slopes"] = signed_list
            out["bar_timestamps"] = stamps
            return out
    out["deteriorated"] = True
    out["slope_scores"] = scores
    out["signed_slopes"] = signed_list
    out["bar_timestamps"] = stamps
    return out


def score_vwap_quality(
    candles: List[Dict[str, Any]],
    *,
    side: str,
    atr_daily_pct: float,
    cfg: Optional[Dict[str, Any]] = None,
    since: Optional[Any] = None,
    max_whipsaw_crosses: Optional[int] = None,
) -> Dict[str, Any]:
    """VWAP-quality for READY / expansion: steepening + no flip-flop + limited crosses.

    This is the single function both ``rs_expansion_watch`` and checklist READY gating call.
    """
    cfg = cfg or get_config()
    if max_whipsaw_crosses is None:
        max_whipsaw_crosses = int(
            cfg.get("ready_max_whipsaw_crosses")
            if cfg.get("ready_max_whipsaw_crosses") is not None
            else DEFAULT_MAX_WHIPSAW_CROSSES
        )

    empty = {
        "steep_ok": False,
        "slope_score": 0.0,
        "signed_slope_atr": 0.0,
        "flip_flop": False,
        "adverse_closes": 0,
        "first_adverse_hm": None,
        "whipsaw_crosses": 0,
        "unstable": True,
        "quality_pass": False,
        "threshold_vwap_slope": THRESHOLD_VWAP_SLOPE,
        "max_whipsaw_crosses": max_whipsaw_crosses,
    }
    if not candles or len(candles) < 20:
        return empty

    steep_ok, score, signed = vwap_slope_steepening(
        candles, side=side, atr_daily_pct=atr_daily_pct, cfg=cfg
    )
    flip = adverse_confirmed_vwap_close_since(candles, side=side, since=since)
    crosses = int(whipsaw_cross_count(candles))
    unstable = bool(flip["flip_flop"]) or crosses > max_whipsaw_crosses
    quality_pass = bool(steep_ok) and not unstable

    return {
        "steep_ok": steep_ok,
        "slope_score": round(score, 2),
        "signed_slope_atr": round(signed, 4),
        "flip_flop": bool(flip["flip_flop"]),
        "adverse_closes": int(flip["adverse_closes"]),
        "first_adverse_hm": flip.get("first_adverse_hm"),
        "whipsaw_crosses": crosses,
        "unstable": unstable,
        "quality_pass": quality_pass,
        "threshold_vwap_slope": THRESHOLD_VWAP_SLOPE,
        "max_whipsaw_crosses": max_whipsaw_crosses,
    }

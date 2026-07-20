"""10-minute Kavach evaluation — chart parity for locked-symbol live recompute.

Pairs consecutive 5m bars into 10m OHLCV aligned with TradingView session
boundaries (first close 09:25 IST, then 09:35, 09:45, …). Pairing always
resets at each session date so 09:15+09:20 form the first 10m bar.

PARITY vs TWCTO Kavach Pine v3.0 (``TWCTO_Kavach_v3_0``):
- SuperTrend: ATR period 10, **multiplier 1.5** (not classic 3.0).
- MACD: **12 / 26 / 9** (updated 21-Jul with Pine v3.0 default).
- Panel EMA vs VWAP / Trend votes use Pine ``emaLen`` default **9** (script name
  ``ema5Raw``); READY entry price still uses true EMA5.
- VWAP purity: last 8 **10m** bars (5m series resampled with ``bar_size=2``).
- Panel Trend row = 2-of-3 majority of (MACD line vs signal, Supertrend, EMA vs VWAP).
- Residual: Upstox 5m→10m vs TV native 10m feed; session VWAP from 5m H/L/C/V.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pytz

from backend.services.kavach_confidence import (
    REGIME_TREND,
    VWAP_CONSISTENCY_BARS,
    compute_vwap_purity_pct,
    detect_market_regime,
    resolve_score_and_grade,
    vwap_opposite_side_consecutive,
)
from backend.services.kavach_engine import BEARISH_STATES, compute_trade_score, evaluate_kavach, KavachInput
from backend.services.kavach_volume import (
    _f,
    _parse_ist,
    cumulative_volume_tod_ratio,
    last_closed_bar_index,
    volume_participation_label,
)
from backend.services.relative_strength_scanner import (
    _current_and_prev_day_close,
    _macd_last,
    _parse_ist_date,
    _sorted_candles,
)
from backend.services.rs_conviction_signals import ema10_10min
from backend.services.smart_futures_exit import _supertrend_dir_last_two
from backend.services.smart_futures_picker.indicators import adx_value
from backend.services.vajra.indicators import cumulative_vwap, ema_series

IST = pytz.timezone("Asia/Kolkata")
BAR_MINUTES_5M = 5

# TWCTO Kavach Pine v3.0 Layer-1 defaults (chart panel parity).
PINE_ST_PERIOD = 10
PINE_ST_MULT = 1.5
PINE_MACD_FAST = 12
PINE_MACD_SLOW = 26
PINE_MACD_SIGNAL = 9
PINE_EMA_LEN = 9  # input "EMA Length"; script variable ema5Raw


def aggregate_10m_bars(candles: List[Dict]) -> List[Dict[str, Any]]:
    """Pair same-day 5m bars → 10m OHLCV (close = 2nd bar close).

    Pairing resets at each session date boundary so the first 10m bar of a
    session is always that session's 09:15+09:20, regardless of the global
    index of 09:15 in the multi-day fetch buffer. (NSE has 75 five-minute bars
    per day — an odd count — so global-index pairing misaligned every other
    prior-day count and dropped 09:15.)
    """
    candles = _sorted_candles(candles)
    out: List[Dict[str, Any]] = []
    n = len(candles)
    i = 0
    while i < n:
        session_date = _parse_ist_date(candles[i].get("timestamp"))
        j = i + 1
        while j < n and _parse_ist_date(candles[j].get("timestamp")) == session_date:
            j += 1
        # Pair within [i, j) only — never across the session boundary.
        rel = 0
        while i + rel + 1 < j:
            g0 = i + rel
            g1 = i + rel + 1
            b0, b1 = candles[g0], candles[g1]
            end_ts = _parse_ist(b1.get("timestamp"))
            bar_end = end_ts + timedelta(minutes=BAR_MINUTES_5M) if end_ts else None
            out.append(
                {
                    "open": _f(b0.get("open") or b0.get("close")),
                    "high": max(_f(b0.get("high")), _f(b1.get("high"))),
                    "low": min(_f(b0.get("low")), _f(b1.get("low"))),
                    "close": _f(b1.get("close")),
                    "volume": _f(b0.get("volume")) + _f(b1.get("volume")),
                    "end_5m_idx": g1,
                    "timestamp": b1.get("timestamp"),
                    "bar_end": bar_end,
                }
            )
            rel += 2
        i = j
    return out


def last_closed_10m_pair_end_idx(candles: List[Dict], *, now: Optional[datetime] = None) -> int:
    """Global 5m index of the last fully closed 10m bar's 2nd constituent, or -1."""
    if not candles:
        return -1
    candles = _sorted_candles(candles)
    closed_5m = last_closed_bar_index(candles, now=now)
    if closed_5m < 0:
        return -1
    last_date = _parse_ist_date(candles[closed_5m].get("timestamp"))
    first_today = 0
    for i, c in enumerate(candles):
        if _parse_ist_date(c.get("timestamp")) == last_date:
            first_today = i
            break
    rel = closed_5m - first_today
    if rel < 1:
        return -1
    pair_end_rel = rel if rel % 2 == 1 else rel - 1
    return first_today + pair_end_rel


def _10m_series_upto(candles: List[Dict], pair_end_5m_idx: int) -> List[Dict[str, Any]]:
    """All complete 10m bars whose 2nd 5m index <= pair_end_5m_idx."""
    return [b for b in aggregate_10m_bars(candles) if b["end_5m_idx"] <= pair_end_5m_idx]


def _volume_ratio_10m(volumes_10m: List[float]) -> float:
    if not volumes_10m:
        return 0.0
    ema_s = ema_series(volumes_10m, 20)
    denom = ema_s[-1] if ema_s else 0.0
    return (volumes_10m[-1] / denom) if denom > 0 else 0.0


def last_live_10m_pair_end_idx(candles: List[Dict], *, now: Optional[datetime] = None) -> int:
    """Like ``last_closed_10m_pair_end_idx`` but includes the latest fetched 5m bar.

    TradingView panel indicators update on the forming bar; conflict checks that
    only use the last *closed* 10m bar can lag the panel by up to ~10 minutes.
    """
    if not candles:
        return -1
    candles = _sorted_candles(candles)
    last = len(candles) - 1
    last_date = _parse_ist_date(candles[last].get("timestamp"))
    first_today = 0
    for i, c in enumerate(candles):
        if _parse_ist_date(c.get("timestamp")) == last_date:
            first_today = i
            break
    rel = last - first_today
    if rel < 0:
        return -1
    # Prefer a complete 10m pair when available; otherwise allow the unpaired
    # trailing 5m (forming first half) as the eval end so live panel can flip.
    if rel >= 1 and rel % 2 == 1:
        return last
    if rel >= 1 and rel % 2 == 0:
        # Odd-length day relative index 0,2,4… → incomplete pair; still use last.
        return last
    return last if rel == 0 else -1


def _10m_series_upto_live(candles: List[Dict], end_5m_idx: int) -> List[Dict[str, Any]]:
    """Complete 10m bars through end, plus a synthetic forming bar if needed."""
    candles = _sorted_candles(candles)
    if end_5m_idx < 0 or end_5m_idx >= len(candles):
        return []
    last_date = _parse_ist_date(candles[end_5m_idx].get("timestamp"))
    first_today = 0
    for i, c in enumerate(candles):
        if _parse_ist_date(c.get("timestamp")) == last_date:
            first_today = i
            break
    rel = end_5m_idx - first_today
    if rel % 2 == 1:
        return _10m_series_upto(candles, end_5m_idx)
    # Incomplete pair: all complete pairs before this bar + synthetic 10m from
    # the single trailing 5m (matches TV updating on the open 10m bucket).
    complete_end = end_5m_idx - 1 if rel >= 1 else -1
    bars = _10m_series_upto(candles, complete_end) if complete_end >= first_today else []
    c = candles[end_5m_idx]
    end_ts = _parse_ist(c.get("timestamp"))
    bars.append(
        {
            "open": _f(c.get("open") or c.get("close")),
            "high": _f(c.get("high")),
            "low": _f(c.get("low")),
            "close": _f(c.get("close")),
            "volume": _f(c.get("volume")),
            "end_5m_idx": end_5m_idx,
            "timestamp": c.get("timestamp"),
            "bar_end": (end_ts + timedelta(minutes=BAR_MINUTES_5M)) if end_ts else None,
            "forming": True,
        }
    )
    return bars


def metrics_from_10m_candles(
    candles: List[Dict],
    *,
    ranking_type: str,
    nifty_pct: float,
    now: Optional[datetime] = None,
    include_forming: bool = False,
) -> Optional[Dict[str, Any]]:
    """Evaluate Kavach on 10m bars through ``now`` (or wall clock).

    When ``include_forming`` is True, include the latest fetched 5m bar so live
    panel Trend/ST/MACD can flip before the 10m bucket closes (DIR CONFLICT).
    """
    if not candles or len(candles) < 40:
        return None
    candles = _sorted_candles(candles)
    split = _current_and_prev_day_close(candles)
    if split is None:
        return None
    _, previous_close, first_today = split

    if include_forming:
        pair_end = last_live_10m_pair_end_idx(candles, now=now)
        bars_10m = _10m_series_upto_live(candles, pair_end)
    else:
        pair_end = last_closed_10m_pair_end_idx(candles, now=now)
        bars_10m = _10m_series_upto(candles, pair_end)
    if pair_end < first_today:
        return None

    if len(bars_10m) < 5:
        return None

    closes_10m = [b["close"] for b in bars_10m]
    highs_10m = [b["high"] for b in bars_10m]
    lows_10m = [b["low"] for b in bars_10m]
    vols_10m = [b["volume"] for b in bars_10m]
    closed_price = closes_10m[-1]

    ema5_s = ema_series(closes_10m, 5)
    ema9_s = ema_series(closes_10m, 9)
    ema5, ema9 = ema5_s[-1], ema9_s[-1]
    ema9_slope = ema9_s[-1] - ema9_s[-2] if len(ema9_s) >= 2 else 0.0
    # Pine v2.6 panel EMA (ema5Raw) = EMA(emaLen) default 9 — used for Trend / EMA-VWAP votes.
    panel_ema_s = ema_series(closes_10m, PINE_EMA_LEN)
    panel_ema = panel_ema_s[-1]

    # Session VWAP at 10m bar close (5m cumulative through pair_end).
    t_highs = [_f(c.get("high")) for c in candles[first_today : pair_end + 1]]
    t_lows = [_f(c.get("low")) for c in candles[first_today : pair_end + 1]]
    t_closes = [_f(c.get("close")) for c in candles[first_today : pair_end + 1]]
    t_vols = [_f(c.get("volume")) for c in candles[first_today : pair_end + 1]]
    vwap_series = cumulative_vwap(t_highs, t_lows, t_closes, t_vols) if t_closes else [closed_price]
    vwap = vwap_series[-1]

    macd, macd_signal, macd_hist = _macd_last(
        closes_10m, PINE_MACD_FAST, PINE_MACD_SLOW, PINE_MACD_SIGNAL
    )
    adx = adx_value(highs_10m, lows_10m, closes_10m, 14) or 0.0
    _, st_curr_dir = _supertrend_dir_last_two(
        highs_10m, lows_10m, closes_10m, period=PINE_ST_PERIOD, multiplier=PINE_ST_MULT
    )
    st_curr = None if st_curr_dir is None else (st_curr_dir > 0)

    volume_ratio = _volume_ratio_10m(vols_10m)
    volume_tod_ratio = cumulative_volume_tod_ratio(candles, closed_idx=pair_end, now=now)
    vol_label = volume_participation_label(volume_ratio, volume_tod_ratio)

    prev_idx = max(0, len(closes_10m) - 2)
    st_prev_dir, _ = _supertrend_dir_last_two(
        highs_10m[: prev_idx + 1],
        lows_10m[: prev_idx + 1],
        closes_10m[: prev_idx + 1],
        period=PINE_ST_PERIOD,
        multiplier=PINE_ST_MULT,
    )
    st_prev = None if st_prev_dir is None else (st_prev_dir > 0)
    macd_prev, sig_prev, _ = (
        _macd_last(
            closes_10m[: prev_idx + 1], PINE_MACD_FAST, PINE_MACD_SLOW, PINE_MACD_SIGNAL
        )
        if prev_idx >= PINE_MACD_SLOW
        else (macd, macd_signal, 0.0)
    )
    ema5_prev = ema5_s[prev_idx] if prev_idx < len(ema5_s) else ema5
    vwap_prev = vwap_series[-2] if len(vwap_series) >= 2 else vwap
    regime = detect_market_regime(
        st_prev=st_prev,
        st_curr=st_curr,
        macd_prev=macd_prev,
        macd_sig_prev=sig_prev,
        macd_curr=macd,
        macd_sig_curr=macd_signal,
        ema5_prev=ema5_prev,
        vwap_prev=vwap_prev,
        ema5_curr=ema5,
        vwap_curr=vwap,
    )

    kav = evaluate_kavach(
        KavachInput(
            price=closed_price,
            ema5=ema5,
            ema9=ema9,
            ema9_slope=ema9_slope,
            vwap=vwap,
            supertrend_bullish=st_curr,
            macd=macd,
            macd_signal=macd_signal,
            macd_histogram=macd_hist,
            adx=adx,
            volume_ratio=volume_ratio,
        )
    )
    stock_pct = (closed_price - previous_close) / previous_close * 100.0 if previous_close else 0.0
    relative_strength = stock_pct - nifty_pct
    trade_score_raw = compute_trade_score(
        rs=relative_strength,
        state=kav.state,
        volume_ratio=volume_ratio,
        adx=adx,
        price=closed_price,
        vwap=vwap,
        ranking_type=ranking_type,
    )
    purity_dir = "SHORT" if kav.state in BEARISH_STATES else "LONG"
    # Pine: last purityLen completed *chart-TF* (10m) bars on signal side of VWAP.
    # t_closes is 5m — resample with bar_size=2 (never bar_size=1 / 5m lookback).
    purity = compute_vwap_purity_pct(
        t_closes, vwap_series, direction=purity_dir, bar_size=2, num_bars=8
    )

    last_bar = bars_10m[-1]
    bar_end: Optional[datetime] = last_bar.get("bar_end")
    if bar_end is None:
        end_ts = _parse_ist(last_bar.get("timestamp"))
        bar_end = end_ts + timedelta(minutes=BAR_MINUTES_5M) if end_ts else datetime.now(IST)

    ema10 = ema10_10min(candles[: pair_end + 1])
    resolved = resolve_score_and_grade(
        trade_score_raw,
        vol_label,
        purity,
        regime,
        close=closed_price,
        ema10=ema10,
        vwap=vwap,
    )
    trade_score = resolved["trade_score"]
    stretch = resolved.get("stretch") or {}

    # Pine dashboard rows: Trend (2-of-3), Supertrend, MACD (line vs signal).
    ema_above = panel_ema > vwap if vwap else False
    ema_below = panel_ema < vwap if vwap else False
    st_bull = st_curr is True
    st_bear = st_curr is False
    macd_bull = macd > macd_signal
    macd_bear = macd < macd_signal
    trend_bull_votes = (1 if macd_bull else 0) + (1 if st_bull else 0) + (1 if ema_above else 0)
    trend_bear_votes = (1 if macd_bear else 0) + (1 if st_bear else 0) + (1 if ema_below else 0)
    if trend_bull_votes >= 2:
        panel_trend = "Bullish"
    elif trend_bear_votes >= 2:
        panel_trend = "Bearish"
    else:
        panel_trend = "Mixed"

    return {
        "relative_strength": relative_strength,
        "trade_score": trade_score,
        "trade_score_raw": trade_score_raw,
        "volume_ratio": volume_ratio,
        "volume_tod_ratio": volume_tod_ratio,
        "volume_label": vol_label,
        "volumes_10m": vols_10m[-8:],
        "vwap_purity_pct": purity,
        "market_regime": regime,
        "confidence_grade": resolved["confidence_grade"],
        "stretch": stretch,
        "kavach_state": kav.state,
        "ema5": ema5,
        "ema10_10m": ema10,
        "panel_ema": panel_ema,
        "vwap": vwap,
        "supertrend": (1.0 if st_curr else (-1.0 if st_curr is False else 0.0)),
        "macd": macd,
        "macd_signal": macd_signal,
        "macd_histogram": macd_hist,
        "adx": adx,
        "ranking_type": ranking_type,
        "price": closed_price,
        "bar_evaluated_at": bar_end,
        "pair_end_5m_idx": pair_end,
        "timeframe": "10m",
        "scan_time": now or datetime.now(IST),
        "include_forming": bool(include_forming),
        "forming": bool(last_bar.get("forming")),
        "panel_trend": panel_trend,
        "pine_params": {
            "st_period": PINE_ST_PERIOD,
            "st_mult": PINE_ST_MULT,
            "macd": (PINE_MACD_FAST, PINE_MACD_SLOW, PINE_MACD_SIGNAL),
            "ema_len": PINE_EMA_LEN,
        },
    }


def lock_vwap_trend_broken_10m(
    candles: List[Dict],
    *,
    lock_direction: str,
    num_bars: int = VWAP_CONSISTENCY_BARS,
    now: Optional[datetime] = None,
) -> Optional[bool]:
    """R1 lock removal: last N confirmed 10m closes all on opposite side of session VWAP.

    Uses the same N as Layer 3 VWAP purity (``VWAP_CONSISTENCY_BARS``). Returns None
    when candles are insufficient to evaluate (caller should not remove on None).
    """
    if not candles or len(candles) < 40:
        return None
    candles = _sorted_candles(candles)
    split = _current_and_prev_day_close(candles)
    if split is None:
        return None
    _, _, first_today = split
    pair_end = last_closed_10m_pair_end_idx(candles, now=now)
    if pair_end < first_today:
        return None
    bars_10m = _10m_series_upto(candles, pair_end)
    if len(bars_10m) < num_bars:
        return None

    # Session VWAP on 5m through each 10m bar end; sample closes at 10m ends via bar_size=1
    # on a dense series aligned to 10m closes + matching VWAP at those ends.
    closes_10m: List[float] = []
    vwaps_10m: List[float] = []
    for b in bars_10m:
        end_idx = int(b["end_5m_idx"])
        t_highs = [_f(c.get("high")) for c in candles[first_today : end_idx + 1]]
        t_lows = [_f(c.get("low")) for c in candles[first_today : end_idx + 1]]
        t_closes = [_f(c.get("close")) for c in candles[first_today : end_idx + 1]]
        t_vols = [_f(c.get("volume")) for c in candles[first_today : end_idx + 1]]
        if not t_closes:
            continue
        v_series = cumulative_vwap(t_highs, t_lows, t_closes, t_vols)
        closes_10m.append(float(b["close"]))
        vwaps_10m.append(float(v_series[-1]))

    if len(closes_10m) < num_bars:
        return None
    return vwap_opposite_side_consecutive(
        closes_10m,
        vwaps_10m,
        lock_direction=lock_direction,
        num_bars=num_bars,
        bar_size=1,
    )


def timeline_states(
    candles: List[Dict],
    *,
    ranking_type: str,
    nifty_pct: float = 0.0,
    start_min: int = 9 * 60 + 15,
    end_min: int = 13 * 60 + 40,
) -> List[Dict[str, Any]]:
    """Walk each closed 10m bar in [start_min, end_min] IST and return state rows."""
    candles = _sorted_candles(candles)
    last_date = _parse_ist_date(candles[-1].get("timestamp")) if candles else None
    if not last_date:
        return []
    rows: List[Dict[str, Any]] = []
    seen_ends: set = set()
    for i in range(len(candles)):
        ts = _parse_ist(candles[i].get("timestamp"))
        if ts is None or _parse_ist_date(candles[i].get("timestamp")) != last_date:
            continue
        bar_end = ts + timedelta(minutes=BAR_MINUTES_5M)
        m = bar_end.hour * 60 + bar_end.minute
        if m < start_min or m > end_min:
            continue
        pair_end = last_closed_10m_pair_end_idx(candles, now=bar_end)
        if pair_end < 0:
            continue
        pe_ts = _parse_ist(candles[pair_end].get("timestamp"))
        if pe_ts is None:
            continue
        pe_end = pe_ts + timedelta(minutes=BAR_MINUTES_5M)
        key = pe_end.isoformat()
        if key in seen_ends:
            continue
        seen_ends.add(key)
        m = metrics_from_10m_candles(candles, ranking_type=ranking_type, nifty_pct=nifty_pct, now=bar_end)
        if not m:
            continue
        rows.append(
            {
                "bar_end_ist": pe_end.strftime("%H:%M"),
                "kavach_state": m["kavach_state"],
                "trade_score": m["trade_score"],
                "confidence_grade": m["confidence_grade"],
                "volume_label": m["volume_label"],
                "adx": round(m["adx"], 1),
                "price": round(m["price"], 2),
                "ema5": round(m["ema5"], 2),
                "ema10": round(m["ema10_10m"] or 0, 2),
                "vwap": round(m["vwap"], 2),
            }
        )
    return rows

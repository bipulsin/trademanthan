"""10-minute Kavach evaluation — chart parity for locked-symbol live recompute.

Pairs consecutive 5m bars into 10m OHLCV aligned with TradingView session
boundaries (first close 09:25 IST, then 09:35, 09:45, …).

PARITY NOTE (residual vs Pine):
- Source data are Upstox 5m candles aggregated to 10m; TV may use native 10m feed.
- Session VWAP uses cumulative 5m H/L/C/V from today's open (not 10m bar VWAP).
- EMA5/EMA9 are computed on 10m closes; Pine Kavach labels may show "EMA10" in UI
  but the engine spec uses EMA5/EMA9 (same as kavach_engine.KavachInput).
- Volume ratio uses summed 5m volume in the 10m bar vs EMA(20) of prior 10m volumes.
- SuperTrend/MACD/ADX use standard periods on the 10m OHLCV series.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pytz

from backend.services.kavach_confidence import (
    REGIME_TREND,
    compute_confidence_grade,
    compute_vwap_purity_pct,
    detect_market_regime,
    format_confidence_display,
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


def aggregate_10m_bars(candles: List[Dict]) -> List[Dict[str, Any]]:
    """Pair same-day 5m bars → 10m OHLCV (close = 2nd bar close)."""
    candles = _sorted_candles(candles)
    out: List[Dict[str, Any]] = []
    for i in range(1, len(candles), 2):
        b0, b1 = candles[i - 1], candles[i]
        d0 = _parse_ist_date(b0.get("timestamp"))
        d1 = _parse_ist_date(b1.get("timestamp"))
        if d0 != d1:
            continue
        end_ts = _parse_ist(b1.get("timestamp"))
        bar_end = end_ts + timedelta(minutes=BAR_MINUTES_5M) if end_ts else None
        out.append(
            {
                "open": _f(b0.get("open") or b0.get("close")),
                "high": max(_f(b0.get("high")), _f(b1.get("high"))),
                "low": min(_f(b0.get("low")), _f(b1.get("low"))),
                "close": _f(b1.get("close")),
                "volume": _f(b0.get("volume")) + _f(b1.get("volume")),
                "end_5m_idx": i,
                "timestamp": b1.get("timestamp"),
                "bar_end": bar_end,
            }
        )
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


def metrics_from_10m_candles(
    candles: List[Dict],
    *,
    ranking_type: str,
    nifty_pct: float,
    now: Optional[datetime] = None,
) -> Optional[Dict[str, Any]]:
    """Evaluate Kavach on closed 10m bars through ``now`` (or wall clock)."""
    if not candles or len(candles) < 40:
        return None
    candles = _sorted_candles(candles)
    split = _current_and_prev_day_close(candles)
    if split is None:
        return None
    _, previous_close, first_today = split

    pair_end = last_closed_10m_pair_end_idx(candles, now=now)
    if pair_end < first_today:
        return None

    bars_10m = _10m_series_upto(candles, pair_end)
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

    # Session VWAP at 10m bar close (5m cumulative through pair_end).
    t_highs = [_f(c.get("high")) for c in candles[first_today : pair_end + 1]]
    t_lows = [_f(c.get("low")) for c in candles[first_today : pair_end + 1]]
    t_closes = [_f(c.get("close")) for c in candles[first_today : pair_end + 1]]
    t_vols = [_f(c.get("volume")) for c in candles[first_today : pair_end + 1]]
    vwap_series = cumulative_vwap(t_highs, t_lows, t_closes, t_vols) if t_closes else [closed_price]
    vwap = vwap_series[-1]

    macd, macd_signal, macd_hist = _macd_last(closes_10m)
    adx = adx_value(highs_10m, lows_10m, closes_10m, 14) or 0.0
    _, st_curr_dir = _supertrend_dir_last_two(highs_10m, lows_10m, closes_10m)
    st_curr = None if st_curr_dir is None else (st_curr_dir > 0)

    volume_ratio = _volume_ratio_10m(vols_10m)
    volume_tod_ratio = cumulative_volume_tod_ratio(candles, closed_idx=pair_end, now=now)
    vol_label = volume_participation_label(volume_ratio, volume_tod_ratio)

    prev_idx = max(0, len(closes_10m) - 2)
    st_prev_dir, _ = _supertrend_dir_last_two(
        highs_10m[: prev_idx + 1], lows_10m[: prev_idx + 1], closes_10m[: prev_idx + 1]
    )
    st_prev = None if st_prev_dir is None else (st_prev_dir > 0)
    macd_prev, sig_prev, _ = (
        _macd_last(closes_10m[: prev_idx + 1]) if prev_idx >= 26 else (macd, macd_signal, 0.0)
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
    trade_score = compute_trade_score(
        rs=relative_strength,
        state=kav.state,
        volume_ratio=volume_ratio,
        adx=adx,
        price=closed_price,
        vwap=vwap,
        ranking_type=ranking_type,
    )
    purity_dir = "SHORT" if kav.state in BEARISH_STATES else "LONG"
    purity = compute_vwap_purity_pct(t_closes, vwap_series, direction=purity_dir, bar_size=1, num_bars=8)
    grade, floor = compute_confidence_grade(trade_score, vol_label, purity, regime)

    last_bar = bars_10m[-1]
    bar_end: Optional[datetime] = last_bar.get("bar_end")
    if bar_end is None:
        end_ts = _parse_ist(last_bar.get("timestamp"))
        bar_end = end_ts + timedelta(minutes=BAR_MINUTES_5M) if end_ts else datetime.now(IST)

    ema10 = ema10_10min(candles[: pair_end + 1])

    return {
        "relative_strength": relative_strength,
        "trade_score": trade_score,
        "volume_ratio": volume_ratio,
        "volume_label": vol_label,
        "vwap_purity_pct": purity,
        "market_regime": regime,
        "confidence_grade": format_confidence_display(grade, floor),
        "kavach_state": kav.state,
        "ema5": ema5,
        "ema10_10m": ema10,
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
    }


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

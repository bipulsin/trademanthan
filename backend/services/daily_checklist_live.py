"""Live Kavach recompute for locked checklist symbols (candle cache)."""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from types import SimpleNamespace
from typing import Any, Dict, Optional

import pytz
from sqlalchemy import text

from backend.services.daily_checklist import (
    _auto_fields_from_rs,
)
from backend.services.kavach_confidence import (
    REGIME_TREND,
    compute_confidence_grade,
    compute_vwap_purity_pct,
    detect_market_regime,
    format_confidence_display,
)
from backend.services.kavach_engine import BEARISH_STATES, compute_trade_score, evaluate_kavach, KavachInput
from backend.services.kavach_volume import closed_bar_volume_ratio, cumulative_volume_tod_ratio, volume_participation_label
from backend.services.rs_conviction_candles import candles_cache_only, load_instrument_atr_maps
from backend.services.relative_strength_scanner import (
    _current_and_prev_day_close,
    _f,
    _macd_last,
    _sorted_candles,
    _supertrend_dir_last_two,
    last_closed_bar_index,
    RANKING_BEARISH,
    RANKING_BULLISH,
)

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")


def _ranking_for_direction(direction: str) -> str:
    return RANKING_BEARISH if direction == "SHORT" else RANKING_BULLISH


def _metrics_from_candles(candles, *, ranking_type: str, nifty_pct: float) -> Optional[Dict[str, Any]]:
    """Build scanner-equivalent metrics dict from cached candles only."""
    if not candles or len(candles) < 40:
        return None
    candles = _sorted_candles(candles)
    split = _current_and_prev_day_close(candles)
    if split is None:
        return None
    current_price, previous_close, first_today = split
    closes = [_f(c.get("close")) for c in candles]
    highs = [_f(c.get("high")) for c in candles]
    lows = [_f(c.get("low")) for c in candles]
    volumes = [_f(c.get("volume")) for c in candles]

    from backend.services.smart_futures_picker.indicators import adx_value
    from backend.services.vajra.indicators import cumulative_vwap, ema_series

    closed_idx = last_closed_bar_index(candles)
    if closed_idx < 0:
        return None
    closed_price = closes[closed_idx]

    ema5_s = ema_series(closes[: closed_idx + 1], 5)
    ema9_s = ema_series(closes[: closed_idx + 1], 9)
    ema5, ema9 = ema5_s[-1], ema9_s[-1]
    ema9_slope = ema9_s[-1] - ema9_s[-2] if len(ema9_s) >= 2 else 0.0

    t_highs = highs[first_today : closed_idx + 1]
    t_lows = lows[first_today : closed_idx + 1]
    t_closes = closes[first_today : closed_idx + 1]
    t_vols = volumes[first_today : closed_idx + 1]
    vwap = cumulative_vwap(t_highs, t_lows, t_closes, t_vols)[-1] if t_closes else closed_price

    macd, macd_signal, macd_hist = _macd_last(closes[: closed_idx + 1])
    adx = adx_value(highs[: closed_idx + 1], lows[: closed_idx + 1], closes[: closed_idx + 1], 14) or 0.0
    _, st_curr_dir = _supertrend_dir_last_two(
        highs[: closed_idx + 1], lows[: closed_idx + 1], closes[: closed_idx + 1]
    )
    st_curr = None if st_curr_dir is None else (st_curr_dir > 0)
    _, _, volume_ratio = closed_bar_volume_ratio(volumes, closed_idx=closed_idx)
    volume_tod_ratio = cumulative_volume_tod_ratio(candles, closed_idx=closed_idx)
    vol_label = volume_participation_label(volume_ratio, volume_tod_ratio)

    prev_idx = max(first_today, closed_idx - 1)
    st_prev_dir, _ = _supertrend_dir_last_two(highs[: closed_idx + 1], lows[: closed_idx + 1], closes[: closed_idx + 1])
    st_prev = None if st_prev_dir is None else (st_prev_dir > 0)
    macd_prev, sig_prev, _ = _macd_last(closes[: prev_idx + 1]) if prev_idx >= 26 else (macd, macd_signal, 0.0)
    vwap_series = cumulative_vwap(t_highs, t_lows, t_closes, t_vols) if t_closes else [vwap]
    regime = detect_market_regime(
        st_prev=st_prev,
        st_curr=st_curr,
        macd_prev=macd_prev,
        macd_sig_prev=sig_prev,
        macd_curr=macd,
        macd_sig_curr=macd_signal,
        ema5_prev=ema5_s[prev_idx] if prev_idx < len(ema5_s) else ema5,
        vwap_prev=vwap_series[-2] if len(vwap_series) >= 2 else vwap,
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
    grade, floor = compute_confidence_grade(trade_score, vol_label, 0.0, regime)
    purity_dir = "SHORT" if kav.state in BEARISH_STATES else "LONG"
    purity = compute_vwap_purity_pct(t_closes, vwap_series, direction=purity_dir)

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
        "vwap": vwap,
        "supertrend": (1.0 if st_curr else (-1.0 if st_curr is False else 0.0)),
        "macd": macd,
        "macd_signal": macd_signal,
        "macd_histogram": macd_hist,
        "adx": adx,
        "ranking_type": ranking_type,
        "scan_time": datetime.now(IST),
    }


def _latest_nifty_pct(db) -> float:
    row = db.execute(
        text(
            """
            SELECT nifty_percent FROM relative_strength_snapshot
            WHERE scan_time = (SELECT MAX(scan_time) FROM relative_strength_snapshot)
            LIMIT 1
            """
        )
    ).fetchone()
    if row and row.nifty_percent is not None:
        return float(row.nifty_percent)
    return 0.0


def recompute_locked_symbol(db, symbol: str, direction: str) -> Optional[Dict[str, Any]]:
    """Return checklist auto fields + indicator_as_of from live candle cache, or None."""
    sym = (symbol or "").strip().upper()
    if not sym:
        return None
    ikey_map, _ = load_instrument_atr_maps(db, {sym})
    ikey = ikey_map.get(sym)
    if not ikey:
        return None
    candles = candles_cache_only(ikey)
    if not candles:
        return None
    ranking = _ranking_for_direction(direction)
    nifty_pct = _latest_nifty_pct(db)
    metrics = _metrics_from_candles(candles, ranking_type=ranking, nifty_pct=nifty_pct)
    if not metrics:
        return None
    row = SimpleNamespace(**metrics, symbol=sym)
    fields = _auto_fields_from_rs(row, direction, live_map={})
    computed_at = metrics["scan_time"]
    return {"fields": fields, "indicator_as_of": computed_at, "source": "live_recompute"}


def is_indicator_stale(
    indicator_as_of: Optional[datetime],
    latest_rs_scan: Optional[datetime],
    *,
    stale_minutes: int = 10,
) -> bool:
    """True when indicator data is too old vs latest RS batch or wall clock."""
    now = datetime.now(IST)
    if indicator_as_of is None:
        return True
    ia = indicator_as_of.astimezone(IST) if indicator_as_of.tzinfo else indicator_as_of.replace(tzinfo=IST)
    age_min = (now - ia).total_seconds() / 60.0
    if age_min > stale_minutes:
        return True
    if latest_rs_scan is not None:
        ls = latest_rs_scan.astimezone(IST) if latest_rs_scan.tzinfo else latest_rs_scan.replace(tzinfo=IST)
        if ls - ia > timedelta(minutes=stale_minutes):
            return True
    return False

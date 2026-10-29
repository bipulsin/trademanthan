"""Stage-1 qualification filter (pure; unit-tested)."""
from __future__ import annotations

from datetime import date, datetime, time as dt_time
from typing import Any, Dict, List, Optional, Sequence

import pytz

from backend.services.momentx.candles import candle_date, candle_time, drop_forming_candle, splits_ohlcv
from backend.services.momentx.config import CONFIG, MomentXConfig
from backend.services.momentx.indicators import sma, wilder_adx, wilder_rsi, williams_r

IST = pytz.timezone("Asia/Kolkata")


def _completed_daily(daily: Sequence[Dict[str, Any]], today: date) -> List[Dict[str, Any]]:
    out = []
    for c in daily:
        d = candle_date(c)
        if d is None or d >= today:
            continue
        out.append(c)
    return out


def _today_volume(
    daily: Sequence[Dict[str, Any]],
    minutes15: Sequence[Dict[str, Any]],
    today: date,
    *,
    asof_time: Optional[dt_time],
    time_adjusted: bool,
) -> float:
    """Current-day cumulative volume.

    Prefer sum of today's 15m bars (optionally truncated to asof_time). Fall back
    to the partial daily candle for today when 15m volume is empty.
    """
    total = 0.0
    for c in minutes15:
        d = candle_date(c)
        if d != today:
            continue
        if time_adjusted and asof_time is not None:
            t = candle_time(c)
            if t is not None and t > asof_time:
                continue
        try:
            total += float(c.get("volume") or 0)
        except (TypeError, ValueError):
            continue
    if total > 0:
        return total
    for c in daily:
        if candle_date(c) == today:
            try:
                return float(c.get("volume") or 0)
            except (TypeError, ValueError):
                return 0.0
    return 0.0


def _avg_prior_volume(
    daily: Sequence[Dict[str, Any]],
    minutes15: Sequence[Dict[str, Any]],
    today: date,
    *,
    n_days: int,
    asof_time: Optional[dt_time],
    time_adjusted: bool,
) -> Optional[float]:
    completed = _completed_daily(daily, today)
    if len(completed) < n_days:
        return None
    prior = completed[-n_days:]
    if not time_adjusted:
        vols = []
        for c in prior:
            try:
                vols.append(float(c.get("volume") or 0))
            except (TypeError, ValueError):
                vols.append(0.0)
        return sum(vols) / float(n_days)

    # Time-adjusted: for each of the prior n days, sum 15m volume up to asof_time.
    day_set = {candle_date(c) for c in prior}
    by_day: Dict[date, float] = {d: 0.0 for d in day_set if d is not None}
    for c in minutes15:
        d = candle_date(c)
        if d not in by_day:
            continue
        if asof_time is not None:
            t = candle_time(c)
            if t is not None and t > asof_time:
                continue
        try:
            by_day[d] += float(c.get("volume") or 0)
        except (TypeError, ValueError):
            continue
    if len(by_day) < n_days:
        # Fall back to full-day average if 15m history is thin.
        vols = [float(c.get("volume") or 0) for c in prior]
        return sum(vols) / float(n_days)
    return sum(by_day.values()) / float(len(by_day))


def evaluate_stage1(
    *,
    daily: Sequence[Dict[str, Any]],
    minutes15: Sequence[Dict[str, Any]],
    price: Optional[float] = None,
    now: Optional[datetime] = None,
    cfg: Optional[MomentXConfig] = None,
) -> Dict[str, Any]:
    """Return qualification result + per-condition values for one equity.

    ``qualified`` is True only when every condition passes.
    """
    cfg = cfg or CONFIG
    now = now or datetime.now(IST)
    if now.tzinfo is None:
        now = IST.localize(now)
    else:
        now = now.astimezone(IST)
    today = now.date()
    asof_time = now.time()

    daily_rows = list(daily or [])
    m15 = drop_forming_candle(minutes15 or [], 15)
    completed_daily = _completed_daily(daily_rows, today)

    result: Dict[str, Any] = {
        "qualified": False,
        "skip_reason": None,
        "price": None,
        "sma200": None,
        "prev_close": None,
        "pct_change": None,
        "rsi": None,
        "williams_r": None,
        "adx": None,
        "today_volume": None,
        "volume_sma5": None,
        "conditions": {},
    }

    if len(completed_daily) < cfg.sma_period:
        result["skip_reason"] = "insufficient_daily"
        return result
    if len(m15) < max(cfg.minutes15_min_bars, cfg.rsi_period + 2, cfg.adx_period * 2):
        result["skip_reason"] = "insufficient_15m"
        return result

    _, _, _, daily_closes, _ = splits_ohlcv(completed_daily)
    sma200 = sma(daily_closes, cfg.sma_period)
    prev_close = float(daily_closes[-1]) if daily_closes else None
    if sma200 is None or prev_close is None or prev_close <= 0:
        result["skip_reason"] = "sma_or_prev_close"
        return result

    _, highs, lows, closes, _ = splits_ohlcv(m15)
    px = float(price) if price is not None else float(closes[-1])
    pct = ((px - prev_close) / prev_close) * 100.0
    rsi = wilder_rsi(closes, cfg.rsi_period)
    wr = williams_r(highs, lows, closes, cfg.williams_period)
    adx = wilder_adx(highs, lows, closes, cfg.adx_period)
    today_vol = _today_volume(
        daily_rows,
        m15,
        today,
        asof_time=asof_time,
        time_adjusted=cfg.time_adjusted_volume,
    )
    vol_sma = _avg_prior_volume(
        daily_rows,
        m15,
        today,
        n_days=cfg.volume_sma_days,
        asof_time=asof_time,
        time_adjusted=cfg.time_adjusted_volume,
    )

    result.update(
        {
            "price": px,
            "sma200": sma200,
            "prev_close": prev_close,
            "pct_change": pct,
            "rsi": rsi,
            "williams_r": wr,
            "adx": adx,
            "today_volume": today_vol,
            "volume_sma5": vol_sma,
        }
    )

    conds = {
        "below_sma200": px < float(sma200),
        "pct_in_band": cfg.pct_change_min <= pct <= cfg.pct_change_max,
        "rsi_ok": rsi is not None and float(rsi) > cfg.rsi_min,
        "williams_ok": wr is not None and float(wr) >= cfg.williams_min,
        "adx_ok": adx is not None and float(adx) > cfg.adx_min,
        "volume_ok": vol_sma is not None and today_vol > float(vol_sma),
    }
    result["conditions"] = conds
    result["qualified"] = all(conds.values())
    return result

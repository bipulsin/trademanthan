"""Stage-1 qualification filter (pure; unit-tested).

Supports bullish and bearish sides. Volume vs SMA(5) is always computed
(``volume_ok``) but only required for ``qualified`` when ``require_volume``
is True.
"""
from __future__ import annotations

from datetime import date, datetime, time as dt_time
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pytz

from backend.services.momentx.candles import candle_date, candle_time, drop_forming_candle, splits_ohlcv
from backend.services.momentx.config import CONFIG, MomentXConfig
from backend.services.momentx.indicators import sma, wilder_adx, wilder_rsi, williams_r

IST = pytz.timezone("Asia/Kolkata")

SIDE_BULLISH = "bullish"
SIDE_BEARISH = "bearish"


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


def _empty_result(*, side: str) -> Dict[str, Any]:
    return {
        "qualified": False,
        "core_qualified": False,
        "side": side,
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


def _bullish_conditions(
    *,
    px: float,
    sma200: float,
    pct: float,
    rsi: Optional[float],
    wr: Optional[float],
    adx: Optional[float],
    today_vol: float,
    vol_sma: Optional[float],
    cfg: MomentXConfig,
) -> Dict[str, bool]:
    return {
        "below_sma200": px < float(sma200),
        "pct_in_band": cfg.pct_change_min <= pct <= cfg.pct_change_max,
        "rsi_ok": rsi is not None and float(rsi) > cfg.rsi_min,
        "williams_ok": wr is not None and float(wr) >= cfg.williams_min,
        "adx_ok": adx is not None and float(adx) > cfg.adx_min,
        "volume_ok": vol_sma is not None and today_vol > float(vol_sma),
    }


def _bearish_conditions(
    *,
    px: float,
    sma200: float,
    pct: float,
    rsi: Optional[float],
    wr: Optional[float],
    adx: Optional[float],
    today_vol: float,
    vol_sma: Optional[float],
    cfg: MomentXConfig,
) -> Dict[str, bool]:
    return {
        "above_sma200": px > float(sma200),
        "pct_bearish": pct <= cfg.pct_change_bearish_max,
        "rsi_bearish": rsi is not None and float(rsi) <= cfg.rsi_bearish_max,
        "williams_bearish": wr is not None and float(wr) <= cfg.williams_bearish_max,
        "adx_ok": adx is not None and float(adx) > cfg.adx_min,
        "volume_ok": vol_sma is not None and today_vol > float(vol_sma),
    }


def _finalize(
    *,
    side: str,
    conds: Dict[str, bool],
    require_volume: bool,
    metrics: Dict[str, Any],
) -> Dict[str, Any]:
    core = {k: v for k, v in conds.items() if k != "volume_ok"}
    core_ok = all(core.values()) if core else False
    vol_ok = bool(conds.get("volume_ok"))
    qualified = core_ok and (vol_ok if require_volume else True)
    out = _empty_result(side=side)
    out.update(metrics)
    out["conditions"] = conds
    out["core_qualified"] = core_ok
    out["qualified"] = qualified
    return out


def evaluate_stage1(
    *,
    daily: Sequence[Dict[str, Any]],
    minutes15: Sequence[Dict[str, Any]],
    price: Optional[float] = None,
    now: Optional[datetime] = None,
    cfg: Optional[MomentXConfig] = None,
    side: str = SIDE_BULLISH,
    require_volume: bool = True,
) -> Dict[str, Any]:
    """Return qualification result + per-condition values for one equity.

    ``core_qualified`` ignores the volume gate. ``qualified`` also requires
    ``volume_ok`` when ``require_volume`` is True.
    """
    cfg = cfg or CONFIG
    side_norm = SIDE_BEARISH if str(side).lower() == SIDE_BEARISH else SIDE_BULLISH
    now = now or datetime.now(IST)
    if now.tzinfo is None:
        now = IST.localize(now)
    else:
        now = now.astimezone(IST)
    today = now.date()
    asof_time = now.time()

    daily_rows = list(daily or [])
    m15 = drop_forming_candle(minutes15 or [], 15, now=now)
    completed_daily = _completed_daily(daily_rows, today)

    result = _empty_result(side=side_norm)

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

    metrics = {
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

    if side_norm == SIDE_BEARISH:
        conds = _bearish_conditions(
            px=px,
            sma200=float(sma200),
            pct=pct,
            rsi=rsi,
            wr=wr,
            adx=adx,
            today_vol=today_vol,
            vol_sma=vol_sma,
            cfg=cfg,
        )
    else:
        conds = _bullish_conditions(
            px=px,
            sma200=float(sma200),
            pct=pct,
            rsi=rsi,
            wr=wr,
            adx=adx,
            today_vol=today_vol,
            vol_sma=vol_sma,
            cfg=cfg,
        )

    return _finalize(
        side=side_norm,
        conds=conds,
        require_volume=require_volume,
        metrics=metrics,
    )


def evaluate_best_side(
    *,
    daily: Sequence[Dict[str, Any]],
    minutes15: Sequence[Dict[str, Any]],
    price: Optional[float] = None,
    now: Optional[datetime] = None,
    cfg: Optional[MomentXConfig] = None,
    require_volume: bool = False,
) -> Tuple[Optional[str], Dict[str, Any]]:
    """Evaluate bullish then bearish; return ``(side, result)`` for first core hit.

    Prefer bullish when both core-qualify at the same clock (rare).
    """
    bull = evaluate_stage1(
        daily=daily,
        minutes15=minutes15,
        price=price,
        now=now,
        cfg=cfg,
        side=SIDE_BULLISH,
        require_volume=require_volume,
    )
    if bull.get("skip_reason"):
        return None, bull
    if bull.get("core_qualified"):
        return SIDE_BULLISH, bull

    bear = evaluate_stage1(
        daily=daily,
        minutes15=minutes15,
        price=price,
        now=now,
        cfg=cfg,
        side=SIDE_BEARISH,
        require_volume=require_volume,
    )
    if bear.get("core_qualified"):
        return SIDE_BEARISH, bear
    return None, bull

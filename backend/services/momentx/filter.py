"""Stage-1 qualification filter (pure; unit-tested).

Supports bullish and bearish sides. Volume vs SMA(5) is always computed
(``volume_ok``) but only required for ``qualified`` when ``require_volume``
is True.

Data quality: rejects stale / frozen bars (wrong last candle clock, flat
pct vs prev close, volume regression, duplicate metric fingerprints).
"""
from __future__ import annotations

from datetime import date, datetime, time as dt_time, timedelta
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

import pytz

from backend.services.momentx.candles import (
    candle_date,
    candle_time,
    drop_forming_candle,
    splits_ohlcv,
)
from backend.services.momentx.config import CONFIG, MomentXConfig
from backend.services.momentx.indicators import sma, wilder_adx, wilder_rsi, williams_r

IST = pytz.timezone("Asia/Kolkata")

SIDE_BULLISH = "bullish"
SIDE_BEARISH = "bearish"

# Metric fingerprint for duplicate / frozen-bar detection across clocks.
Fingerprint = Tuple[float, float, float, int]


def _completed_daily(daily: Sequence[Dict[str, Any]], today: date) -> List[Dict[str, Any]]:
    out = []
    for c in daily:
        d = candle_date(c)
        if d is None or d >= today:
            continue
        out.append(c)
    return out


def expected_closed_15m_start(now: datetime) -> datetime:
    """Start timestamp of the latest 15m bar that has fully closed by ``now``.

    Upstox bars are labeled by start; a bar starting at S closes at S+15m.
    """
    if now.tzinfo is None:
        now = IST.localize(now)
    else:
        now = now.astimezone(IST)
    # Latest start S with S + 15m <= now  ⇒  S <= now - 15m, floored to 15m grid.
    closed_by = now - timedelta(minutes=15)
    minute = (closed_by.minute // 15) * 15
    return closed_by.replace(minute=minute, second=0, microsecond=0)


def metric_fingerprint(
    *,
    price: Optional[float],
    rsi: Optional[float],
    adx: Optional[float],
    today_volume: Optional[float],
) -> Fingerprint:
    return (
        round(float(price or 0.0), 4),
        round(float(rsi or 0.0), 2),
        round(float(adx or 0.0), 2),
        int(float(today_volume or 0.0)),
    )


def _today_volume_since_open(
    minutes15: Sequence[Dict[str, Any]],
    today: date,
    *,
    asof_time: Optional[dt_time],
    market_open: dt_time,
) -> float:
    """Cumulative cash volume from session open through asof (numerator)."""
    total = 0.0
    for c in minutes15:
        d = candle_date(c)
        if d != today:
            continue
        t = candle_time(c)
        if t is not None and t < market_open:
            continue
        if asof_time is not None and t is not None and t > asof_time:
            continue
        try:
            total += float(c.get("volume") or 0)
        except (TypeError, ValueError):
            continue
    return total


def _avg_prior_full_day_volume(
    daily: Sequence[Dict[str, Any]],
    today: date,
    *,
    n_days: int,
) -> Optional[float]:
    """5-day average of full-day volume on prior completed sessions (Chartink-style)."""
    completed = _completed_daily(daily, today)
    if len(completed) < n_days:
        return None
    prior = completed[-n_days:]
    vols: List[float] = []
    for c in prior:
        try:
            vols.append(float(c.get("volume") or 0))
        except (TypeError, ValueError):
            vols.append(0.0)
    return sum(vols) / float(n_days)


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
        "fingerprint": None,
        "last_candle_ts": None,
        "expected_candle_ts": None,
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


def _stale_reason(
    *,
    m15: Sequence[Dict[str, Any]],
    today: date,
    now: datetime,
    px: float,
    prev_close: float,
    pct: float,
    rsi: Optional[float],
    adx: Optional[float],
    today_vol: float,
    prior_fingerprints: Optional[Set[Fingerprint]],
    prev_today_volume: Optional[float],
) -> Optional[str]:
    """Return a skip_reason if the bar looks stale / frozen."""
    expected = expected_closed_15m_start(now)
    last = m15[-1] if m15 else None
    last_ts = None
    if last is not None:
        try:
            from backend.services.upstox_service import _parse_ts_to_aware_ist

            last_ts = _parse_ts_to_aware_ist(last.get("timestamp"))
        except Exception:
            last_ts = None
    if last_ts is None:
        return "stale_missing_candle"
    last_ts = last_ts.astimezone(IST).replace(second=0, microsecond=0)
    exp = expected.replace(second=0, microsecond=0)
    if last_ts != exp:
        return "stale_candle_clock"

    # Flat day print equal to previous close — typical frozen quote at open.
    if abs(float(pct)) < 1e-12 and abs(float(px) - float(prev_close)) < 1e-9:
        return "stale_flat_prev_close"

    # Session volume must not go backwards as clocks advance.
    if prev_today_volume is not None and today_vol + 1e-9 < float(prev_today_volume):
        return "stale_volume_regression"

    # Latest 15m bar volume must not be below the prior bar's volume when the
    # session print is otherwise frozen (common stale-feed pattern).
    today_bars = [c for c in m15 if candle_date(c) == today]
    if len(today_bars) >= 2:
        try:
            v_last = float(today_bars[-1].get("volume") or 0)
            v_prev = float(today_bars[-2].get("volume") or 0)
            c_last = float(today_bars[-1].get("close") or 0)
            c_prev = float(today_bars[-2].get("close") or 0)
            if v_last + 1e-9 < v_prev and abs(c_last - c_prev) < 1e-9:
                return "stale_bar_volume_drop"
        except (TypeError, ValueError):
            pass

    fp = metric_fingerprint(price=px, rsi=rsi, adx=adx, today_volume=today_vol)
    if prior_fingerprints and fp in prior_fingerprints:
        return "stale_duplicate_metrics"
    return None


def evaluate_stage1(
    *,
    daily: Sequence[Dict[str, Any]],
    minutes15: Sequence[Dict[str, Any]],
    price: Optional[float] = None,  # ignored for live gates — cash 15m close only
    now: Optional[datetime] = None,
    cfg: Optional[MomentXConfig] = None,
    side: str = SIDE_BULLISH,
    require_volume: bool = True,
    prior_fingerprints: Optional[Set[Fingerprint]] = None,
    prev_today_volume: Optional[float] = None,
    enforce_stale_guards: bool = True,
    minutes15_oscillators: Optional[Sequence[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """Return qualification result + per-condition values for one equity.

    Price / SMA200 / prev_close / volume come from the **cash equity** series:
    SMA200 + prev_close from completed daily closes; price from the last
    closed cash 15m candle close (never a futures tick).

    RSI / Williams %R / ADX come from ``minutes15_oscillators`` when provided
    (live/backtest: current-month **futures** 15m OHLC — same series Kavach
    ``ta.dmi`` uses on the futures chart). When omitted, oscillators fall back
    to cash ``minutes15`` (unit tests / legacy callers).

    Volume: numerator = session cumulative cash 15m volume since open; baseline =
    average full-day volume over the prior ``volume_sma_days`` sessions.
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
    market_open = dt_time(int(cfg.market_open_hhmm[0]), int(cfg.market_open_hhmm[1]))

    daily_rows = list(daily or [])
    m15 = drop_forming_candle(minutes15 or [], 15, now=now)
    # Oscillators: futures 15m when supplied; else same cash series.
    osc_src = minutes15_oscillators if minutes15_oscillators is not None else minutes15
    m15_osc = drop_forming_candle(osc_src or [], 15, now=now)
    completed_daily = _completed_daily(daily_rows, today)

    result = _empty_result(side=side_norm)
    expected = expected_closed_15m_start(now)
    result["expected_candle_ts"] = expected.isoformat()

    if len(completed_daily) < cfg.sma_period:
        result["skip_reason"] = "insufficient_daily"
        return result
    # ADX/RSI need multi-session 15m history (not only today).
    min_bars = max(cfg.minutes15_min_bars, cfg.rsi_period + 2, cfg.adx_period * 3)
    if len(m15) < min_bars:
        result["skip_reason"] = "insufficient_15m"
        return result
    if len(m15_osc) < min_bars:
        result["skip_reason"] = "insufficient_15m_osc"
        return result
    prior_session_bars = sum(1 for c in m15_osc if candle_date(c) is not None and candle_date(c) < today)
    if prior_session_bars < cfg.adx_period * 2:
        result["skip_reason"] = "insufficient_prior_15m"
        return result

    _, _, _, daily_closes, _ = splits_ohlcv(completed_daily)
    sma200 = sma(daily_closes, cfg.sma_period)
    prev_close = float(daily_closes[-1]) if daily_closes else None
    if sma200 is None or prev_close is None or prev_close <= 0:
        result["skip_reason"] = "sma_or_prev_close"
        return result

    _, _, _, closes, _ = splits_ohlcv(m15)
    _, osc_highs, osc_lows, osc_closes, _ = splits_ohlcv(m15_osc)
    # Always cash 15m close — never a futures LTP / tick override.
    _ = price  # retained in signature for call-site compat; unused
    px = float(closes[-1])
    pct = ((px - prev_close) / prev_close) * 100.0
    # Match TradingView Kavach on the futures chart: ta.dmi / RSI / Williams
    # on the futures 15m series (not cash equity).
    rsi = wilder_rsi(osc_closes, cfg.rsi_period)
    wr = williams_r(osc_highs, osc_lows, osc_closes, cfg.williams_period)
    adx = wilder_adx(osc_highs, osc_lows, osc_closes, cfg.adx_period)

    today_vol = _today_volume_since_open(
        m15,
        today,
        asof_time=asof_time,
        market_open=market_open,
    )
    # Chartink-style: full-day average of prior sessions (not time-of-day matched).
    vol_sma = _avg_prior_full_day_volume(
        daily_rows,
        today,
        n_days=cfg.volume_sma_days,
    )

    last_ts_iso = None
    try:
        from backend.services.upstox_service import _parse_ts_to_aware_ist

        ts = _parse_ts_to_aware_ist(m15[-1].get("timestamp")) if m15 else None
        if ts is not None:
            last_ts_iso = ts.astimezone(IST).isoformat()
    except Exception:
        last_ts_iso = None

    fp = metric_fingerprint(price=px, rsi=rsi, adx=adx, today_volume=today_vol)
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
        "fingerprint": fp,
        "last_candle_ts": last_ts_iso,
        "expected_candle_ts": expected.isoformat(),
    }

    if enforce_stale_guards:
        stale = _stale_reason(
            m15=m15,
            today=today,
            now=now,
            px=px,
            prev_close=prev_close,
            pct=pct,
            rsi=rsi,
            adx=adx,
            today_vol=today_vol,
            prior_fingerprints=prior_fingerprints,
            prev_today_volume=prev_today_volume,
        )
        if stale:
            result.update(metrics)
            result["skip_reason"] = stale
            result["conditions"] = {}
            return result

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
    prior_fingerprints: Optional[Set[Fingerprint]] = None,
    prev_today_volume: Optional[float] = None,
    enforce_stale_guards: bool = True,
    minutes15_oscillators: Optional[Sequence[Dict[str, Any]]] = None,
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
        prior_fingerprints=prior_fingerprints,
        prev_today_volume=prev_today_volume,
        enforce_stale_guards=enforce_stale_guards,
        minutes15_oscillators=minutes15_oscillators,
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
        prior_fingerprints=prior_fingerprints,
        prev_today_volume=prev_today_volume,
        enforce_stale_guards=enforce_stale_guards,
        minutes15_oscillators=minutes15_oscillators,
    )
    if bear.get("skip_reason"):
        # Bull didn't core-qualify; bear failed data quality — surface bear skip.
        if not bull.get("core_qualified"):
            return None, bear
    if bear.get("core_qualified"):
        return SIDE_BEARISH, bear
    return None, bull

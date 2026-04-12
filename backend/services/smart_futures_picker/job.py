"""
Smart Futures picker job: arbitrage_master universe → CMS / Final_CMS → smart_futures_daily.

Schedules: 09:30 IST open; then 10:00–15:00 every 30 minutes (weekdays).
Off-cycle on Sat/Sun: set env SMART_FUTURES_PICKER_FORCE_WEEKEND=1 (manual only).
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional

from sqlalchemy.orm import Session

import pytz
from sqlalchemy import text

from backend.config import settings
from backend.database import SessionLocal
from backend.services.smart_futures_session_date import effective_session_date_ist_for_trend
from backend.services.smart_futures_picker.indicators import (
    adx_14_value,
    compute_cms_core,
    compute_obv_slope_daily,
    divergence_bundle,
    ha_trend_score,
    renko_momentum_score,
    session_vwap,
    true_range,
    volume_surge_ratio,
    wilder_atr_14,
)
from backend.services.smart_futures_picker.sector_score import compute_sector_score_for_stock
from backend.services.upstox_service import UpstoxService

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")
INDIA_VIX_KEY = "NSE_INDEX|India VIX"
SESSION_OPEN = (9, 15)
SESSION_END = (15, 30)
SESSION_MINUTES = 375.0  # 9:15–15:30


def _sort_candles(candles: Optional[List[dict]]) -> List[dict]:
    if not candles:
        return []
    return sorted(candles, key=lambda c: str(c.get("timestamp") or ""))


def _last_n_daily(candles: List[dict], n: int = 10) -> List[dict]:
    """Last n daily candles by date (oldest first within window)."""
    s = _sort_candles(candles)
    if len(s) < n:
        return []
    return s[-n:]


def _ist_date_from_ts(ts: str) -> Optional[datetime.date]:
    if not ts or len(ts) < 10:
        return None
    try:
        dt = datetime.strptime(ts[:19], "%Y-%m-%dT%H:%M:%S")
        return dt.replace(tzinfo=IST).date()
    except ValueError:
        try:
            return datetime.strptime(ts[:10], "%Y-%m-%d").date()
        except ValueError:
            return None


def _minutes_since_open_ist(ts: str, session_date: datetime.date) -> float:
    """Minutes from 9:15 IST on session_date to candle time (approx if parse fails)."""
    if not ts or len(ts) < 16:
        return SESSION_MINUTES / 2.0
    try:
        dt = datetime.strptime(ts[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=IST)
        sod = IST.localize(datetime.combine(session_date, datetime.min.time()).replace(hour=9, minute=15))
        return max(1.0, (dt - sod).total_seconds() / 60.0)
    except Exception:
        return SESSION_MINUTES / 2.0


def _session_elapsed_fraction(session_date: datetime.date, m5: List[dict]) -> float:
    if not m5:
        return 0.25
    last_ts = str(m5[-1].get("timestamp") or "")
    mins = _minutes_since_open_ist(last_ts, session_date)
    return max(0.05, min(1.0, mins / SESSION_MINUTES))


def _vix_last_close_5m(upstox: UpstoxService) -> Optional[float]:
    try:
        c = upstox.get_historical_candles_by_instrument_key(
            INDIA_VIX_KEY, interval="minutes/5", days_back=2
        )
        s = _sort_candles(c)
        if not s:
            q = upstox.get_market_quote_by_key(INDIA_VIX_KEY) or {}
            lp = float(q.get("last_price") or 0)
            return lp if lp > 0 else None
        return float(s[-1].get("close") or 0) or None
    except Exception as e:
        logger.warning("smart_futures_picker: VIX fetch failed: %s", e)
        return None


def _entry_price_1m_close(upstox: UpstoxService, fut_key: str, now_ist: datetime) -> Optional[float]:
    try:
        c = upstox.get_historical_candles_by_instrument_key(fut_key, interval="minutes/1", days_back=2)
        s = _sort_candles(c)
        if not s:
            return None
        for bar in reversed(s):
            ts = str(bar.get("timestamp") or "")
            if len(ts) < 19:
                continue
            try:
                t_end = datetime.strptime(ts[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=IST)
            except ValueError:
                continue
            if t_end <= now_ist:
                cl = float(bar.get("close") or 0)
                return cl if cl > 0 else None
        cl = float(s[-1].get("close") or 0)
        return cl if cl > 0 else None
    except Exception as e:
        logger.debug("1m entry for %s: %s", fut_key, e)
        return None


@dataclass
class ScoredPick:
    stock: str
    fut_symbol: str
    fut_instrument_key: str
    side: str
    obv_slope: float
    volume_surge: float
    adx_14: float
    atr_14: float
    renko_momentum: float
    ha_trend: float
    macd_div: float
    rsi_div: float
    stoch_div: float
    cms: float
    final_cms: float
    sector_score: float
    combined_sentiment: float


def _load_sentiment_map(db) -> Dict[str, float]:
    try:
        rows = db.execute(
            text(
                """
                SELECT stock,
                       COALESCE(current_combined_sentiment, combined_sentiment_avg,
                                last_combined_sentiment, api_sentiment_avg, 0.0) AS sc
                FROM stock_fin_sentiment
                """
            )
        ).fetchall()
    except Exception:
        return {}
    out: Dict[str, float] = {}
    for stock, sc in rows:
        if stock:
            try:
                out[str(stock).strip().upper()] = float(sc or 0.0)
            except (TypeError, ValueError):
                out[str(stock).strip().upper()] = 0.0
    return out


def _score_symbol(
    upstox: UpstoxService,
    stock: str,
    fut_sym: str,
    fut_key: str,
    session_date: datetime.date,
    sentiment_map: Dict[str, float],
    sector_index: Optional[str] = None,
) -> Optional[ScoredPick]:
    daily_raw = upstox.get_historical_candles_by_instrument_key(
        fut_key, interval="days/1", days_back=45
    )
    daily = _last_n_daily(_sort_candles(daily_raw), 10)
    if len(daily) < 10:
        return None
    closes_d = [float(x["close"]) for x in daily]
    vols_d = [float(x.get("volume") or 0) for x in daily]
    avg_daily_vol = sum(vols_d) / 10.0
    obv_slope = compute_obv_slope_daily(closes_d, vols_d)

    m5_raw = upstox.get_historical_candles_by_instrument_key(
        fut_key, interval="minutes/5", days_back=2
    )
    m5 = _sort_candles(m5_raw)
    m5_today = [b for b in m5 if _ist_date_from_ts(str(b.get("timestamp") or "")) == session_date]
    if len(m5_today) < 20:
        m5_today = m5[-max(20, len(m5)) :] if len(m5) >= 20 else []
    if len(m5_today) < 15:
        return None

    highs = [float(b["high"]) for b in m5_today]
    lows = [float(b["low"]) for b in m5_today]
    opens = [float(b["open"]) for b in m5_today]
    closes = [float(b["close"]) for b in m5_today]
    vols = [float(b.get("volume") or 0) for b in m5_today]

    atr = wilder_atr_14(highs, lows, closes)
    if atr is None or atr <= 0:
        return None
    adx = adx_14_value(highs, lows, closes)
    if adx is None:
        adx = 0.0

    vwap = session_vwap(highs, lows, closes, vols)
    last_close = closes[-1]
    cvatr = (last_close - vwap) / atr
    cvatr = max(-4.0, min(4.0, cvatr))

    frac = _session_elapsed_fraction(session_date, m5_today)
    vs = volume_surge_ratio(vols, avg_daily_vol, frac)
    if vs < 1.5:
        return None

    brick = max(atr * 0.1, last_close * 0.0005)
    rm = renko_momentum_score(closes, brick)
    ha = ha_trend_score(opens, highs, lows, closes)
    md, rd, sd = divergence_bundle(highs, lows, closes)

    cms = compute_cms_core(obv_slope, vs, adx, cvatr, rm, ha, md, rd, sd)

    sector_score = compute_sector_score_for_stock(stock, sector_instrument_key=sector_index)
    if sector_score < -0.6:
        return None

    raw_sent = sentiment_map.get(stock.upper())
    if raw_sent is not None:
        try:
            comb = float(raw_sent)
        except (TypeError, ValueError):
            comb = 0.0
        if -0.4 <= comb <= 0.3:
            return None
    else:
        comb = 0.0

    final_cms = cms * (1.0 + 0.5 * sector_score) * (1.0 + comb)
    if abs(final_cms) <= 2.5:
        return None

    side = "LONG" if final_cms > 0 else "SHORT"
    return ScoredPick(
        stock=stock,
        fut_symbol=fut_sym or "",
        fut_instrument_key=fut_key,
        side=side,
        obv_slope=obv_slope,
        volume_surge=vs,
        adx_14=float(adx),
        atr_14=float(atr),
        renko_momentum=rm,
        ha_trend=ha,
        macd_div=md,
        rsi_div=rd,
        stoch_div=sd,
        cms=float(cms),
        final_cms=float(final_cms),
        sector_score=float(sector_score),
        combined_sentiment=comb,
    )


def _persist_pick(
    db,
    pick: ScoredPick,
    session_date: datetime.date,
    scan_trigger: str,
    vix: Optional[float],
    entry_price: float,
    now_ist: datetime,
) -> None:
    atr = pick.atr_14
    hold_type = "positional" if abs(pick.final_cms) > 4.0 else "intraday"
    if pick.side == "LONG":
        sl = entry_price - atr * 1.5
        tgt = entry_price + atr * 3.0
    else:
        sl = entry_price + atr * 1.5
        tgt = entry_price - atr * 3.0

    params = {
        "session_date": session_date,
        "stock": pick.stock,
        "fut_symbol": pick.fut_symbol,
        "fut_instrument_key": pick.fut_instrument_key,
        "side": pick.side,
        "obv_slope": pick.obv_slope,
        "volume_surge": pick.volume_surge,
        "adx_14": pick.adx_14,
        "atr_14": pick.atr_14,
        "renko_momentum": pick.renko_momentum,
        "ha_trend": pick.ha_trend,
        "macd_div": pick.macd_div,
        "rsi_div": pick.rsi_div,
        "stoch_div": pick.stoch_div,
        "cms": pick.cms,
        "final_cms": pick.final_cms,
        "sector_score": pick.sector_score,
        "combined_sentiment": pick.combined_sentiment,
        "entry_price": entry_price,
        "sl_price": sl,
        "target_price": tgt,
        "hold_type": hold_type,
        "entry_at": now_ist,
        "scan_trigger": scan_trigger,
        "vix_at_scan": vix,
    }

    ex = db.execute(
        text(
            """
            SELECT 1 FROM smart_futures_daily
            WHERE session_date = :session_date AND fut_instrument_key = :fut_instrument_key
            LIMIT 1
            """
        ),
        {"session_date": session_date, "fut_instrument_key": pick.fut_instrument_key},
    ).first()

    if ex:
        db.execute(
            text(
                """
                UPDATE smart_futures_daily SET
                    stock = :stock,
                    fut_symbol = :fut_symbol,
                    side = :side,
                    obv_slope = :obv_slope,
                    volume_surge = :volume_surge,
                    adx_14 = :adx_14,
                    atr_14 = :atr_14,
                    renko_momentum = :renko_momentum,
                    ha_trend = :ha_trend,
                    macd_div = :macd_div,
                    rsi_div = :rsi_div,
                    stoch_div = :stoch_div,
                    cms = :cms,
                    final_cms = :final_cms,
                    sector_score = :sector_score,
                    combined_sentiment = :combined_sentiment,
                    entry_price = :entry_price,
                    sl_price = :sl_price,
                    target_price = :target_price,
                    hold_type = :hold_type,
                    entry_at = :entry_at,
                    trend_continuation = 'Yes',
                    scan_trigger = :scan_trigger,
                    vix_at_scan = :vix_at_scan,
                    updated_at = CURRENT_TIMESTAMP
                WHERE session_date = :session_date AND fut_instrument_key = :fut_instrument_key
                """
            ),
            params,
        )
    else:
        db.execute(
            text(
                """
                INSERT INTO smart_futures_daily (
                    session_date, stock, fut_symbol, fut_instrument_key, side,
                    obv_slope, volume_surge, adx_14, atr_14,
                    renko_momentum, ha_trend, macd_div, rsi_div, stoch_div,
                    cms, final_cms, sector_score, combined_sentiment,
                    entry_price, sl_price, target_price, hold_type,
                    entry_at, trend_continuation, scan_trigger, vix_at_scan
                ) VALUES (
                    :session_date, :stock, :fut_symbol, :fut_instrument_key, :side,
                    :obv_slope, :volume_surge, :adx_14, :atr_14,
                    :renko_momentum, :ha_trend, :macd_div, :rsi_div, :stoch_div,
                    :cms, :final_cms, :sector_score, :combined_sentiment,
                    :entry_price, :sl_price, :target_price, :hold_type,
                    :entry_at, NULL, :scan_trigger, :vix_at_scan
                )
                """
            ),
            params,
        )
    db.commit()


def run_smart_futures_picker_job(scan_trigger: str = "") -> Dict[str, Any]:
    """
    scan_trigger: e.g. '09:30', '10:00' (for logging / row metadata).
    """
    now_ist = datetime.now(IST)
    _force_weekend = os.environ.get("SMART_FUTURES_PICKER_FORCE_WEEKEND", "").strip().lower() in (
        "1",
        "true",
        "yes",
    )
    if now_ist.weekday() >= 5 and not _force_weekend:
        return {"skipped": "weekend", "scan_trigger": scan_trigger}
    # Align with GET /daily filter (weekend / pre-9:00 map to trading session date).
    session_date = effective_session_date_ist_for_trend(now_ist)

    db = SessionLocal()
    try:
        sentiment_map = _load_sentiment_map(db)
        rows = db.execute(
            text(
                """
                SELECT stock, currmth_future_symbol, currmth_future_instrument_key, sector_index
                FROM arbitrage_master
                WHERE currmth_future_instrument_key IS NOT NULL
                  AND TRIM(currmth_future_instrument_key) <> ''
                ORDER BY stock
                """
            )
        ).fetchall()
    finally:
        db.close()

    if not rows:
        logger.info("smart_futures_picker: no arbitrage_master futures keys")
        return {"skipped": "no_universe", "scan_trigger": scan_trigger}

    try:
        upstox = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    except Exception as e:
        logger.error("smart_futures_picker: Upstox init failed: %s", e)
        return {"error": str(e), "scan_trigger": scan_trigger}

    vix = _vix_last_close_5m(upstox)
    skip_long_vix = vix is not None and vix > 22.0

    longs: List[ScoredPick] = []
    shorts: List[ScoredPick] = []
    for stock, fut_sym, fut_key, sector_index in rows:
        if not fut_key:
            continue
        st = str(stock).strip().upper()
        try:
            sc = _score_symbol(
                upstox,
                st,
                str(fut_sym or ""),
                str(fut_key).strip(),
                session_date,
                sentiment_map,
                str(sector_index).strip() if sector_index else None,
            )
        except Exception as e:
            logger.debug("smart_futures_picker: skip %s: %s", st, e)
            continue
        if not sc:
            continue
        if sc.side == "LONG":
            if skip_long_vix:
                continue
            longs.append(sc)
        else:
            shorts.append(sc)

    longs.sort(key=lambda x: x.final_cms, reverse=True)
    shorts.sort(key=lambda x: x.final_cms)

    best_long = longs[0] if longs else None
    best_short = shorts[0] if shorts else None

    if not best_long and not best_short:
        logger.info(
            "smart_futures_picker [%s]: no qualifying picks (longs=%s shorts=%s vix=%s skip_long=%s)",
            scan_trigger,
            len(longs),
            len(shorts),
            vix,
            skip_long_vix,
        )
        return {
            "scan_trigger": scan_trigger,
            "picks": 0,
            "vix": vix,
            "skip_long_vix": skip_long_vix,
        }

    dbw: Optional[Session] = None
    try:
        dbw = SessionLocal()
        saved = 0
        for pick in (best_long, best_short):
            if not pick:
                continue
            entry = _entry_price_1m_close(upstox, pick.fut_instrument_key, now_ist)
            if entry is None:
                q = upstox.get_market_quote_by_key(pick.fut_instrument_key) or {}
                entry = float(q.get("last_price") or 0)
            if not entry or entry <= 0:
                logger.warning("smart_futures_picker: no entry for %s", pick.stock)
                continue
            _persist_pick(dbw, pick, session_date, scan_trigger or "", vix, entry, now_ist)
            saved += 1

        logger.info(
            "smart_futures_picker [%s]: saved=%s vix=%s (long=%s short=%s)",
            scan_trigger,
            saved,
            vix,
            best_long.stock if best_long else None,
            best_short.stock if best_short else None,
        )
        return {
            "scan_trigger": scan_trigger,
            "picks": saved,
            "vix": vix,
            "best_long": best_long.stock if best_long else None,
            "best_short": best_short.stock if best_short else None,
        }
    finally:
        if dbw is not None:
            dbw.close()

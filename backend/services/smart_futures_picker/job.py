"""
Smart Futures picker job: arbitrage_master universe → CMS / Final_CMS → smart_futures_daily.

Schedules: 09:15 and 09:30 IST; then 10:00–15:00 every 30 minutes (weekdays).
Off-cycle on Sat/Sun: set env SMART_FUTURES_PICKER_FORCE_WEEKEND=1 (manual only).
"""
from __future__ import annotations

import json
import logging
import os
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy.orm import Session

import pytz
from sqlalchemy import text

from backend.config import settings
from backend.database import SessionLocal
from backend.services.smart_futures_session_date import effective_session_date_ist_for_trend
from backend.services.smart_futures_session_utils import compute_atr5_14_ratio_for_session
from backend.services.oi_integration import (
    NSEOIFetcher,
    get_cached_oi_quote,
    oi_signal_from_quote,
    normalized_oi_score_for_side,
)
from backend.services.smart_futures_config import (
    ADX_LENGTH,
    ADX_THRESHOLD,
    CAPITAL,
    CMS_FINAL_ENTRY_THRESHOLD,
    SMART_FUTURES_MAX_PUBLISH_PER_SCAN,
    SMART_FUTURES_PICK_SELECTION_TOP_N,
    buildup_selection_long_short_caps,
    NEUTRAL_BAND,
    OI_BLOCK_ON_CONFLICT,
    OI_GATE_ENABLED,
    OI_IN_CMS_ENABLED,
    REENTRY_COOLDOWN_MINUTES,
    REENTRY_ENABLED,
    REENTRY_MAX_PER_SESSION,
    REENTRY_NEUTRAL_RESET_THRESHOLD,
    RISK_PCT,
    SECTOR_ALIGN_MIN,
    TIER1_THRESHOLD,
    TIER2_THRESHOLD,
    TIME_FILTER_ENABLED,
    TRADE_WINDOWS,
    cms_weights_active,
    CMS_PRIORITY_FILTER_ENABLED,
    CMS_PRIORITY_OI_MOVER_MAX_RANK,
)
# Index NIFTY/BANKNIFTY alignment was optional; disabled — see run_smart_futures_picker_job.
from backend.services.smart_futures_picker.indicators import (
    adx_last_two,
    breakout_volume_spike,
    compute_cms_core,
    compute_cms_final_multiplier,
    compute_obv_slope_daily,
    divergence_bundle,
    ema_slope_norm_m5,
    ha_trend_score,
    market_regime_ok,
    renko_momentum_score,
    session_vwap,
    volume_surge_ratio,
    vwap_deviation_atr_norm,
    wilder_atr,
    wilder_atr_14,
)
from backend.services.smart_futures_picker.position_sizing import (
    calculate_position_size,
    get_futures_lot_size_by_instrument_key,
)
from backend.services.smart_futures_picker.sector_score import compute_sector_score_for_stock
from backend.services.upstox_service import UpstoxService

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")
INDIA_VIX_KEY = "NSE_INDEX|India VIX"
SESSION_OPEN = (9, 15)
SESSION_END = (15, 30)
SESSION_MINUTES = 375.0  # 9:15–15:30
STANDARD_REQUIRED_DAYS = 10
MINIMUM_ABSOLUTE_DAYS = 5

# Re-entry / CMS trace (per process; reset from DB at job start)
_reentry_track: Dict[str, Dict[str, Any]] = {}
_oi_fetcher_singleton: Optional[NSEOIFetcher] = None


def _log_sf_event(payload: Dict[str, Any]) -> None:
    logger.info("smart_futures_signal %s", json.dumps(payload, default=str))


def _reentry_rk(session_date: date, stock: str) -> str:
    return f"{session_date.isoformat()}_{stock.strip().upper()}"


def _bootstrap_reentry_track(db, session_date: date) -> None:
    try:
        rows = db.execute(
            text(
                """
                SELECT stock, sell_time, reentry_consumed
                FROM smart_futures_daily
                WHERE session_date = :sd
                """
            ),
            {"sd": session_date},
        ).fetchall()
    except Exception as e:
        logger.debug("reentry bootstrap: %s", e)
        return
    for stock, sell_time, rcons in rows:
        if not stock:
            continue
        rk = _reentry_rk(session_date, str(stock))
        st = _reentry_track.setdefault(rk, {})
        if sell_time is not None and rcons:
            st["consumed"] = True
        if sell_time is not None:
            try:
                cu = sell_time + timedelta(minutes=int(REENTRY_COOLDOWN_MINUTES))
                st["cooldown_until"] = cu
            except Exception:
                pass


def _reentry_update_scan(stock: str, session_date: date, final_cms: Optional[float]) -> None:
    if final_cms is None or not REENTRY_ENABLED:
        return
    rk = _reentry_rk(session_date, stock)
    st = _reentry_track.setdefault(rk, {})
    if abs(float(final_cms)) < float(REENTRY_NEUTRAL_RESET_THRESHOLD):
        st["saw_below"] = True


def _reentry_block_reason(
    db,
    stock: str,
    session_date: date,
    now_ist: datetime,
    final_cms: float,
) -> Optional[str]:
    """If re-entry rules block a new pick for a symbol that already had a closed trade today, return reason."""
    if not REENTRY_ENABLED:
        return None
    try:
        row = db.execute(
            text(
                """
                SELECT sell_time, reentry_consumed
                FROM smart_futures_daily
                WHERE session_date = :sd AND UPPER(TRIM(stock)) = :st
                LIMIT 1
                """
            ),
            {"sd": session_date, "st": stock.strip().upper()},
        ).mappings().first()
    except Exception as e:
        logger.debug("reentry check: %s", e)
        return None
    if not row or row.get("sell_time") is None:
        return None
    if bool(row.get("reentry_consumed")):
        return "BLOCKED: re-entry exhausted"
    rk = _reentry_rk(session_date, stock)
    st = _reentry_track.setdefault(rk, {})
    cu = st.get("cooldown_until")
    if cu is not None and now_ist < cu:
        return "BLOCKED: Cooldown active"
    if not st.get("saw_below"):
        return "BLOCKED: neutral reset not confirmed"
    return None


def _count_open_smart_futures(db, session_date: date) -> int:
    try:
        r = db.execute(
            text(
                """
                SELECT COUNT(*) FROM smart_futures_daily
                WHERE session_date = :sd
                  AND LOWER(TRIM(COALESCE(order_status, ''))) = 'bought'
                  AND sell_time IS NULL
                """
            ),
            {"sd": session_date},
        ).scalar()
        return int(r or 0)
    except Exception:
        return 0


def _bar_end_ist_from_ts(ts: str) -> Optional[datetime]:
    if not ts or len(ts) < 19:
        return None
    try:
        return datetime.strptime(ts[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=IST)
    except ValueError:
        return None


def time_of_day_filter(ist_dt: datetime) -> Tuple[bool, str]:
    if not TIME_FILTER_ENABLED:
        return True, ""
    t = ist_dt.time()
    for a, b in TRADE_WINDOWS:
        if a <= t <= b:
            return True, ""
    return False, "BLOCKED: Outside trade window"


def _tier_long(final_cms: float) -> Tuple[str, float]:
    if final_cms > TIER1_THRESHOLD:
        return "TIER1", 1.0
    if TIER2_THRESHOLD <= final_cms <= TIER1_THRESHOLD:
        return "TIER2", 0.5
    return "NO_SIGNAL", 0.0


def _tier_short(final_cms: float) -> Tuple[str, float]:
    if final_cms < -TIER1_THRESHOLD:
        return "TIER1", 1.0
    if -TIER1_THRESHOLD <= final_cms <= -TIER2_THRESHOLD:
        return "TIER2", 0.5
    return "NO_SIGNAL", 0.0


def _oi_gate_ok(side: str, oi_sig: str) -> Tuple[bool, str]:
    s = str(side or "").strip().upper()
    if s == "LONG":
        if oi_sig == "LONG_BUILDUP":
            return True, ""
        if oi_sig == "LONG_UNWINDING":
            return False, "LONG_UNWINDING blocks LONG"
        if oi_sig in ("SHORT_COVERING", "NEUTRAL"):
            return True, "OI caution: " + oi_sig
        return True, "OI caution: " + oi_sig
    if s == "SHORT":
        if oi_sig == "SHORT_BUILDUP":
            return True, ""
        if oi_sig == "SHORT_COVERING":
            return False, "SHORT_COVERING blocks SHORT"
        if oi_sig in ("LONG_BUILDUP", "NEUTRAL"):
            return True, "OI caution: " + oi_sig
        return True, "OI caution: " + oi_sig
    return False, "invalid side"


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


def _ist_date_from_ts(ts: str) -> Optional[date]:
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


def _minutes_since_open_ist(ts: str, session_date: date) -> float:
    """Minutes from 9:15 IST on session_date to candle time (approx if parse fails)."""
    if not ts or len(ts) < 16:
        return SESSION_MINUTES / 2.0
    try:
        dt = datetime.strptime(ts[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=IST)
        sod = IST.localize(datetime.combine(session_date, datetime.min.time()).replace(hour=9, minute=15))
        return max(1.0, (dt - sod).total_seconds() / 60.0)
    except Exception:
        return SESSION_MINUTES / 2.0


def _session_elapsed_fraction(session_date: date, m5: List[dict]) -> float:
    if not m5:
        return 0.25
    last_ts = str(m5[-1].get("timestamp") or "")
    mins = _minutes_since_open_ist(last_ts, session_date)
    return max(0.05, min(1.0, mins / SESSION_MINUTES))


def _vix_last_close_5m(upstox: UpstoxService) -> Optional[float]:
    try:
        c = upstox.get_historical_candles_by_instrument_key(
            INDIA_VIX_KEY, interval="minutes/5", days_back=6
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
class SymbolScoreOutcome:
    """Per-symbol Smart Futures scan result (for diagnostics when nothing qualifies)."""

    stock: str
    pick: Optional["ScoredPick"]
    reject_code: str
    reject_note: Optional[str] = None
    final_cms: Optional[float] = None
    cms: Optional[float] = None
    volume_surge: Optional[float] = None
    sector_score: Optional[float] = None
    combined_sentiment: Optional[float] = None
    gate_long: Optional[Dict[str, bool]] = None
    gate_short: Optional[Dict[str, bool]] = None


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
    atr5_14_ratio: float
    renko_momentum: float
    ha_trend: float
    macd_div: float
    rsi_div: float
    stoch_div: float
    cms: float
    final_cms: float
    sector_score: float
    combined_sentiment: float
    signal_tier: str = "NO_SIGNAL"
    tier_multiplier: float = 1.0
    ema_slope_norm: float = 0.0
    time_filter_passed: bool = True
    time_filter_reason: str = ""
    regime_filter_passed: bool = True
    regime_filter_reason: str = ""
    oi_value: Optional[int] = None
    oi_change: Optional[int] = None
    oi_signal: str = ""
    oi_gate_passed: Optional[bool] = None
    oi_gate_reason: str = ""
    premkt_rank: Optional[int] = None
    oi_heat_rank: Optional[int] = None
    calculated_lots: Optional[int] = None
    stop_loss_price: Optional[float] = None
    tier_sizing_mult: float = 1.0
    history_days_used: int = STANDARD_REQUIRED_DAYS
    history_status: str = "STANDARD"


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


def _score_symbol_outcome(
    upstox: UpstoxService,
    stock: str,
    fut_sym: str,
    fut_key: str,
    session_date: date,
    sentiment_map: Dict[str, float],
    sector_index: Optional[str] = None,
    *,
    index_long_ok: bool = False,
    index_short_ok: bool = False,
) -> SymbolScoreOutcome:
    global _oi_fetcher_singleton

    def _fail(code: str, reject_note: Optional[str] = None, **metrics: Any) -> SymbolScoreOutcome:
        return SymbolScoreOutcome(
            stock=stock,
            pick=None,
            reject_code=code,
            reject_note=reject_note,
            final_cms=metrics.get("final_cms"),
            cms=metrics.get("cms"),
            volume_surge=metrics.get("volume_surge"),
            sector_score=metrics.get("sector_score"),
            combined_sentiment=metrics.get("combined_sentiment"),
            gate_long=metrics.get("gate_long"),
            gate_short=metrics.get("gate_short"),
        )

    daily_raw = upstox.get_historical_candles_by_instrument_key(
        fut_key, interval="days/1", days_back=45
    )
    daily_all = _sort_candles(daily_raw)
    daily_count = len(daily_all)
    if daily_count < MINIMUM_ABSOLUTE_DAYS:
        return _fail(
            "insufficient_daily",
            reject_note=f"New listing with only {daily_count} days; need at least {MINIMUM_ABSOLUTE_DAYS}",
        )
    history_status = "STANDARD"
    if daily_count >= STANDARD_REQUIRED_DAYS:
        daily = daily_all[-STANDARD_REQUIRED_DAYS:]
    else:
        daily = daily_all
        history_status = "NEW_FO_LISTING"
        logger.info(
            "smart_futures_picker: %s: Using adaptive history (%s days). New F&O Entry detected: Using %s days of history instead of %s.",
            stock,
            daily_count,
            daily_count,
            STANDARD_REQUIRED_DAYS,
        )

    closes_d = [float(x["close"]) for x in daily]
    vols_d = [float(x.get("volume") or 0) for x in daily]
    avg_daily_vol = sum(vols_d) / float(len(vols_d))
    obv_slope = compute_obv_slope_daily(closes_d, vols_d)

    m5_raw = upstox.get_historical_candles_by_instrument_key(
        fut_key, interval="minutes/5", days_back=6
    )
    m5 = _sort_candles(m5_raw)
    m5_today = [b for b in m5 if _ist_date_from_ts(str(b.get("timestamp") or "")) == session_date]
    if len(m5_today) < 20:
        m5_today = m5[-max(20, len(m5)) :] if len(m5) >= 20 else []
    if len(m5_today) < 15:
        return _fail("insufficient_m5", reject_note=f"need=15 have={len(m5_today)}")

    last_ts = str(m5_today[-1].get("timestamp") or "")
    bar_end = _bar_end_ist_from_ts(last_ts)
    if bar_end is None:
        return _fail("bad_bar_ts", reject_note=last_ts[:32])
    tf_ok, tf_reason = time_of_day_filter(bar_end)

    highs = [float(b["high"]) for b in m5_today]
    lows = [float(b["low"]) for b in m5_today]
    opens = [float(b["open"]) for b in m5_today]
    closes = [float(b["close"]) for b in m5_today]
    vols = [float(b.get("volume") or 0) for b in m5_today]

    atr = wilder_atr_14(highs, lows, closes)
    if atr is None or atr <= 0:
        return _fail("atr14_invalid")
    atr5 = wilder_atr(highs, lows, closes, 5)
    if atr5 is None or atr5 <= 0:
        return _fail("atr5_invalid")
    atr5_14_ratio = float(atr5) / float(atr)
    adx_curr, adx_prev = adx_last_two(highs, lows, closes, ADX_LENGTH)
    if adx_curr is None:
        return _fail("adx_missing")
    if not market_regime_ok(
        float(atr5), float(atr), float(adx_curr), adx_prev, adx_threshold=ADX_THRESHOLD
    ):
        _log_sf_event(
            {
                "timestamp": bar_end.isoformat(),
                "symbol": stock,
                "action": "BLOCKED",
                "block_reason": "BLOCKED: regime filter failed",
                "regime_filter_passed": False,
                "adx": float(adx_curr),
            }
        )
        return _fail("regime_fail", reject_note="BLOCKED: regime filter failed", final_cms=None)

    vwap = session_vwap(highs, lows, closes, vols)
    last_close = closes[-1]
    vwap_dev = vwap_deviation_atr_norm(last_close, vwap, float(atr))

    frac = _session_elapsed_fraction(session_date, m5_today)
    vs = volume_surge_ratio(vols, avg_daily_vol, frac)
    if vs < 1.5:
        return _fail("volume_surge_low", volume_surge=float(vs))

    brick = max(atr * 0.1, last_close * 0.0005)
    rm = renko_momentum_score(closes, brick)
    ha = ha_trend_score(opens, highs, lows, closes)
    md, rd, sd = divergence_bundle(highs, lows, closes)
    ema_n = ema_slope_norm_m5(closes)

    boost = breakout_volume_spike(highs, lows, closes, vs)
    wts = cms_weights_active()
    oi_score_norm = 0.0
    if OI_IN_CMS_ENABLED:
        if _oi_fetcher_singleton is None:
            try:
                _oi_fetcher_singleton = NSEOIFetcher()
            except Exception:
                _oi_fetcher_singleton = None
        oq = get_cached_oi_quote(stock, _oi_fetcher_singleton)
        if oq is not None:
            prov_side = "LONG" if last_close >= vwap else "SHORT"
            oi_score_norm = normalized_oi_score_for_side(oq, prov_side)

    cms = compute_cms_core(
        obv_slope,
        vs,
        vwap_dev,
        rm,
        ha,
        ema_n,
        breakout_boost=boost,
        oi_score_norm=oi_score_norm,
        weights=wts,
    )
    final_cms = compute_cms_final_multiplier(cms, float(atr5), float(atr), float(adx_curr))

    rk = f"{session_date.isoformat()}_{stock.upper()}"
    st = _reentry_track.setdefault(rk, {})
    st["last_final_cms"] = float(final_cms)

    sector_score = compute_sector_score_for_stock(stock, sector_instrument_key=sector_index)

    raw_sent = sentiment_map.get(stock.upper())
    if raw_sent is not None:
        try:
            comb = float(raw_sent)
        except (TypeError, ValueError):
            comb = 0.0
        if -0.4 <= comb <= 0.3:
            return _fail(
                "sentiment_neutral_band",
                final_cms=float(final_cms),
                cms=float(cms),
                volume_surge=float(vs),
                sector_score=float(sector_score),
                combined_sentiment=float(comb),
            )
    else:
        comb = 0.0

    if abs(float(final_cms)) < float(NEUTRAL_BAND):
        return _fail(
            "neutral_band",
            final_cms=float(final_cms),
            cms=float(cms),
            volume_surge=float(vs),
            sector_score=float(sector_score),
            combined_sentiment=float(comb),
        )

    th = TIER2_THRESHOLD
    t_long, _ = _tier_long(final_cms)
    t_short, _ = _tier_short(final_cms)
    gl = {
        "tier_ok_long": bool(t_long != "NO_SIGNAL"),
        "final_cms_ge_th": bool(final_cms >= th - 1e-12),
        "close_gt_vwap": bool(last_close > vwap),
        "sector_gt_min": bool(sector_score > SECTOR_ALIGN_MIN),
        "index_long_ok": bool(index_long_ok),
    }
    gs = {
        "tier_ok_short": bool(t_short != "NO_SIGNAL"),
        "final_cms_le_neg_th": bool(final_cms <= -th + 1e-12),
        "close_lt_vwap": bool(last_close < vwap),
        "sector_lt_neg_min": bool(sector_score < -SECTOR_ALIGN_MIN),
        "index_short_ok": bool(index_short_ok),
    }
    long_ok = all(gl.values())
    short_ok = all(gs.values())
    if long_ok and not short_ok:
        side = "LONG"
    elif short_ok and not long_ok:
        side = "SHORT"
    elif long_ok and short_ok:
        side = "LONG" if final_cms >= 0 else "SHORT"
    else:
        return _fail(
            "no_entry_signal",
            final_cms=float(final_cms),
            cms=float(cms),
            volume_surge=float(vs),
            sector_score=float(sector_score),
            combined_sentiment=float(comb),
            gate_long=gl,
            gate_short=gs,
        )

    if side == "LONG":
        sig_tier, tier_mult = _tier_long(final_cms)
    else:
        sig_tier, tier_mult = _tier_short(final_cms)

    premkt_r: Optional[int] = None
    oi_heat_r: Optional[int] = None
    try:
        from backend.services.oi_heatmap import oi_heat_rank_for_underlying, premkt_rank_for_stock

        premkt_r = premkt_rank_for_stock(stock, session_date)
        oi_heat_r = oi_heat_rank_for_underlying(stock)
    except Exception:
        pass

    if CMS_PRIORITY_FILTER_ENABLED:
        top_n = int(getattr(settings, "PREMKET_TOP_N", 10))
        in_premkt = premkt_r is not None and 1 <= premkt_r <= top_n
        in_oi = oi_heat_r is not None and 1 <= oi_heat_r <= int(CMS_PRIORITY_OI_MOVER_MAX_RANK)
        if not in_premkt and not in_oi:
            _log_sf_event(
                {
                    "symbol": stock,
                    "action": "BLOCKED",
                    "block_reason": "priority_filter: not in premarket top %s or OI movers top %s"
                    % (top_n, CMS_PRIORITY_OI_MOVER_MAX_RANK),
                    "premkt_rank": premkt_r,
                    "oi_heat_rank": oi_heat_r,
                }
            )
            return _fail(
                "priority_filter",
                reject_note="Not in premarket watchlist or top OI movers",
                final_cms=float(final_cms),
                cms=float(cms),
                volume_surge=float(vs),
                sector_score=float(sector_score),
                combined_sentiment=float(comb),
            )

    oi_val: Optional[int] = None
    oi_chg: Optional[int] = None
    oi_sig = ""
    oi_gate_passed: Optional[bool] = True
    oi_gate_reason = ""
    if OI_GATE_ENABLED:
        if _oi_fetcher_singleton is None:
            try:
                _oi_fetcher_singleton = NSEOIFetcher()
            except Exception:
                _oi_fetcher_singleton = None
        oq = None
        heatmap_sig: Optional[str] = None
        if getattr(settings, "UPSTOX_OI_ENABLED", True):
            try:
                from backend.services.oi_heatmap import (
                    try_oiquote_from_heatmap_for_gate,
                    try_oi_signal_from_heatmap_for_gate,
                )

                oq = try_oiquote_from_heatmap_for_gate(stock)
                heatmap_sig = try_oi_signal_from_heatmap_for_gate(stock)
            except Exception:
                oq = None
                heatmap_sig = None
            if (oq is None or (getattr(oq, "oi", 0) or 0) <= 0) and fut_key:
                try:
                    from backend.services.upstox_market_feed import try_oiquote_from_feed_for_gate

                    oq_feed = try_oiquote_from_feed_for_gate(stock, fut_key)
                    if oq_feed is not None:
                        oq = oq_feed
                except Exception:
                    pass
        if oq is None:
            oq = get_cached_oi_quote(stock, _oi_fetcher_singleton)
        if oq is None:
            oi_gate_passed = None
            oi_gate_reason = "OI unavailable"
            logger.warning("smart_futures_picker: OI unavailable for %s", stock)
            if OI_BLOCK_ON_CONFLICT:
                _log_sf_event(
                    {
                        "symbol": stock,
                        "action": "BLOCKED",
                        "block_reason": "OI gate: data unavailable",
                        "oi_gate_passed": None,
                    }
                )
                return _fail("oi_unavailable", reject_note="OI gate: data unavailable")
        else:
            import time as time_mod

            if time_mod.time() - oq.fetched_at > 300:
                logger.warning("smart_futures_picker: OI stale for %s", stock)
                oi_gate_reason = "OI stale (>5m)"
                if OI_BLOCK_ON_CONFLICT:
                    return _fail("oi_stale", reject_note=oi_gate_reason)
            oi_val = int(oq.oi)
            oi_chg = int(oq.change_in_oi)
            oi_sig = str(heatmap_sig or oi_signal_from_quote(oq) or "").strip().upper()
            ok_oi, note_oi = _oi_gate_ok(side, oi_sig)
            oi_gate_passed = ok_oi
            oi_gate_reason = note_oi
            if not ok_oi and OI_BLOCK_ON_CONFLICT:
                _log_sf_event(
                    {
                        "symbol": stock,
                        "action": "BLOCKED",
                        "block_reason": note_oi,
                        "oi_signal": oi_sig,
                        "oi_gate_passed": False,
                    }
                )
                return _fail("oi_gate", reject_note=note_oi)
            if ok_oi and note_oi.startswith("OI caution"):
                logger.warning("smart_futures_picker: %s %s", stock, note_oi)

    pick = ScoredPick(
        stock=stock,
        fut_symbol=fut_sym or "",
        fut_instrument_key=fut_key,
        side=side,
        obv_slope=obv_slope,
        volume_surge=vs,
        adx_14=float(adx_curr),
        atr_14=float(atr),
        atr5_14_ratio=atr5_14_ratio,
        renko_momentum=rm,
        ha_trend=ha,
        macd_div=md,
        rsi_div=rd,
        stoch_div=sd,
        cms=float(cms),
        final_cms=float(final_cms),
        sector_score=float(sector_score),
        combined_sentiment=comb,
        signal_tier=sig_tier,
        tier_multiplier=float(tier_mult),
        ema_slope_norm=float(ema_n),
        time_filter_passed=bool(tf_ok),
        time_filter_reason="" if tf_ok else tf_reason,
        regime_filter_passed=True,
        regime_filter_reason="",
        oi_value=oi_val,
        oi_change=oi_chg,
        oi_signal=oi_sig,
        oi_gate_passed=oi_gate_passed,
        oi_gate_reason=oi_gate_reason,
        premkt_rank=premkt_r,
        oi_heat_rank=oi_heat_r,
        history_days_used=int(len(daily)),
        history_status=history_status,
    )
    _log_sf_event(
        {
            "timestamp": bar_end.isoformat(),
            "symbol": stock,
            "cms_score_raw": float(cms),
            "cms_final": float(final_cms),
            "signal_tier": sig_tier,
            "time_filter_passed": bool(tf_ok),
            "regime_filter_passed": True,
            "oi_signal": oi_sig,
            "oi_gate_passed": oi_gate_passed,
            "action": "QUALIFIED",
            "block_reason": "",
            "history_days_used": int(len(daily)),
            "history_status": history_status,
        }
    )
    return SymbolScoreOutcome(
        stock=stock,
        pick=pick,
        reject_code="ok",
        final_cms=float(final_cms),
        cms=float(cms),
        volume_surge=float(vs),
        sector_score=float(sector_score),
        combined_sentiment=float(comb),
    )


def _persist_pick(
    db,
    pick: ScoredPick,
    session_date: date,
    scan_trigger: str,
    vix: Optional[float],
    entry_price: float,
    now_ist: datetime,
) -> bool:
    atr = pick.atr_14
    hold_type = "positional" if abs(pick.final_cms) > 1.2 else "intraday"
    if pick.side == "LONG":
        sl = entry_price - atr * 1.2
        tgt = entry_price + atr * 3.0
    else:
        sl = entry_price + atr * 1.2
        tgt = entry_price - atr * 3.0

    sl_dist = float(atr) * 1.2
    lot = get_futures_lot_size_by_instrument_key(pick.fut_instrument_key)
    if lot <= 0:
        logger.warning(
            "smart_futures_signal %s",
            json.dumps(
                {
                    "symbol": pick.stock,
                    "action": "BLOCKED",
                    "block_reason": "BLOCKED: lot size unknown",
                },
                default=str,
            ),
        )
        return False
    psz = calculate_position_size(
        capital=CAPITAL,
        risk_pct=RISK_PCT,
        stop_loss_distance=sl_dist,
        lot_size=lot,
        signal_tier=pick.signal_tier,
    )
    pick.tier_sizing_mult = float(psz.tier_sizing_mult)
    if psz.skipped_reason:
        logger.warning(
            "smart_futures_signal %s",
            json.dumps(
                {
                    "symbol": pick.stock,
                    "action": "BLOCKED",
                    "block_reason": psz.skipped_reason or "SIZE ZERO — SKIPPED",
                    "calculated_lots": 0,
                },
                default=str,
            ),
        )
        return False
    pick.calculated_lots = int(psz.final_lots)
    pick.stop_loss_price = float(sl)

    existing = db.execute(
        text(
            """
            SELECT order_status, sell_time, reentry_consumed
            FROM smart_futures_daily
            WHERE session_date = :session_date AND fut_instrument_key = :fut_instrument_key
            LIMIT 1
            """
        ),
        {"session_date": session_date, "fut_instrument_key": pick.fut_instrument_key},
    ).mappings().first()

    if existing:
        ost = str(existing.get("order_status") or "").strip().lower()
        sold = existing.get("sell_time") is not None
        if ost == "bought" and not sold:
            logger.info(
                "smart_futures_signal %s",
                json.dumps(
                    {
                        "symbol": pick.stock,
                        "action": "BLOCKED",
                        "block_reason": "BLOCKED: open position exists for symbol",
                    },
                    default=str,
                ),
            )
            return False
        if sold and bool(existing.get("reentry_consumed")):
            logger.info(
                "smart_futures_signal %s",
                json.dumps(
                    {
                        "symbol": pick.stock,
                        "action": "BLOCKED",
                        "block_reason": "BLOCKED: re-entry exhausted for session",
                    },
                    default=str,
                ),
            )
            return False

    reentry_flag = bool(existing and existing.get("sell_time") is not None)

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
        "atr5_14_ratio": pick.atr5_14_ratio,
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
        "signal_tier": pick.signal_tier,
        "tier_multiplier": pick.tier_multiplier,
        "sizing_tier_mult": pick.tier_sizing_mult,
        "calculated_lots": pick.calculated_lots,
        "stop_loss_price": pick.stop_loss_price,
        "stop_stage": "INITIAL",
        "current_stop_price": float(sl),
        "time_filter_passed": pick.time_filter_passed,
        "regime_filter_passed": pick.regime_filter_passed,
        "regime_filter_reason": pick.regime_filter_reason or "",
        "oi_value": pick.oi_value,
        "oi_change": pick.oi_change,
        "oi_signal": pick.oi_signal or "",
        "oi_gate_passed": pick.oi_gate_passed,
        "oi_gate_reason": pick.oi_gate_reason or "",
        "ema_slope_norm": pick.ema_slope_norm,
        "cms_score_raw": float(pick.cms),
        "cms_final": float(pick.final_cms),
        "reentry_apply": bool(reentry_flag),
        "premkt_rank": pick.premkt_rank,
        "oi_heat_rank": pick.oi_heat_rank,
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
                    atr5_14_ratio = :atr5_14_ratio,
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
                    signal_tier = :signal_tier,
                    tier_multiplier = :tier_multiplier,
                    sizing_tier_mult = :sizing_tier_mult,
                    calculated_lots = :calculated_lots,
                    stop_loss_price = :stop_loss_price,
                    stop_stage = :stop_stage,
                    current_stop_price = :current_stop_price,
                    time_filter_passed = :time_filter_passed,
                    regime_filter_passed = :regime_filter_passed,
                    regime_filter_reason = :regime_filter_reason,
                    oi_value = :oi_value,
                    oi_change = :oi_change,
                    oi_signal = :oi_signal,
                    oi_gate_passed = :oi_gate_passed,
                    oi_gate_reason = :oi_gate_reason,
                    ema_slope_norm = :ema_slope_norm,
                    cms_score_raw = :cms_score_raw,
                    cms_final = :cms_final,
                    premkt_rank = :premkt_rank,
                    oi_heat_rank = :oi_heat_rank,
                    reentry_consumed = CASE WHEN :reentry_apply THEN TRUE ELSE reentry_consumed END,
                    order_status = CASE WHEN :reentry_apply THEN NULL ELSE order_status END,
                    buy_price = CASE WHEN :reentry_apply THEN NULL ELSE buy_price END,
                    sell_price = CASE WHEN :reentry_apply THEN NULL ELSE sell_price END,
                    sell_time = CASE WHEN :reentry_apply THEN NULL ELSE sell_time END,
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
                    obv_slope, volume_surge, adx_14, atr_14, atr5_14_ratio,
                    renko_momentum, ha_trend, macd_div, rsi_div, stoch_div,
                    cms, final_cms, sector_score, combined_sentiment,
                    entry_price, sl_price, target_price, hold_type,
                    entry_at, trend_continuation, scan_trigger, vix_at_scan,
                    signal_tier, tier_multiplier, sizing_tier_mult, calculated_lots,
                    stop_loss_price, stop_stage, current_stop_price,
                    time_filter_passed, regime_filter_passed, regime_filter_reason,
                    oi_value, oi_change, oi_signal, oi_gate_passed, oi_gate_reason,
                    ema_slope_norm, cms_score_raw, cms_final, reentry_consumed,
                    premkt_rank, oi_heat_rank
                ) VALUES (
                    :session_date, :stock, :fut_symbol, :fut_instrument_key, :side,
                    :obv_slope, :volume_surge, :adx_14, :atr_14, :atr5_14_ratio,
                    :renko_momentum, :ha_trend, :macd_div, :rsi_div, :stoch_div,
                    :cms, :final_cms, :sector_score, :combined_sentiment,
                    :entry_price, :sl_price, :target_price, :hold_type,
                    :entry_at, NULL, :scan_trigger, :vix_at_scan,
                    :signal_tier, :tier_multiplier, :sizing_tier_mult, :calculated_lots,
                    :stop_loss_price, :stop_stage, :current_stop_price,
                    :time_filter_passed, :regime_filter_passed, :regime_filter_reason,
                    :oi_value, :oi_change, :oi_signal, :oi_gate_passed, :oi_gate_reason,
                    :ema_slope_norm, :cms_score_raw, :cms_final, FALSE,
                    :premkt_rank, :oi_heat_rank
                )
                """
            ),
            params,
        )
    db.commit()
    _log_sf_event(
        {
            "symbol": pick.stock,
            "action": "ENTER_" + str(pick.side),
            "calculated_lots": pick.calculated_lots,
            "stop_loss_price": pick.stop_loss_price,
            "stop_stage": "INITIAL",
            "signal_tier": pick.signal_tier,
            "cms_final": float(pick.final_cms),
            "cms_score_raw": float(pick.cms),
        }
    )
    return True


def _log_smart_futures_diagnostics(
    scan_trigger: str,
    session_date: date,
    universe_size: int,
    reject_counts: Counter,
    idx_long_ok: bool,
    idx_short_ok: bool,
    vix: Optional[float],
    skip_long_vix: bool,
    no_entry_outcomes: List[SymbolScoreOutcome],
    long_candidates_after_vix: int,
    short_candidates: int,
    vix_skipped_long_candidates: int,
) -> None:
    """INFO log explaining why the universe produced few or no picks."""
    th = CMS_FINAL_ENTRY_THRESHOLD
    ranked = sorted(
        no_entry_outcomes,
        key=lambda o: abs(o.final_cms or 0.0),
        reverse=True,
    )[:15]
    near: List[Dict[str, Any]] = []
    for o in ranked:
        near.append(
            {
                "stock": o.stock,
                "final_cms": round(o.final_cms, 4) if o.final_cms is not None else None,
                "cms": round(o.cms, 4) if o.cms is not None else None,
                "vs": round(o.volume_surge, 3) if o.volume_surge is not None else None,
                "sector": round(o.sector_score, 4) if o.sector_score is not None else None,
                "sent": round(o.combined_sentiment, 3) if o.combined_sentiment is not None else None,
                "long_gates": o.gate_long,
                "short_gates": o.gate_short,
            }
        )
    index_only_long = 0
    index_only_short = 0
    for o in no_entry_outcomes:
        gl, gs = o.gate_long, o.gate_short
        if gl and all(
            [gl["final_cms_ge_th"], gl["close_gt_vwap"], gl["sector_gt_min"]]
        ) and not gl["index_long_ok"]:
            index_only_long += 1
        if gs and all(
            [gs["final_cms_le_neg_th"], gs["close_lt_vwap"], gs["sector_lt_neg_min"]]
        ) and not gs["index_short_ok"]:
            index_only_short += 1

    logger.info(
        "smart_futures_picker [%s] diagnostics session_date=%s universe=%s vix=%s "
        "idx_long_ok=%s idx_short_ok=%s skip_long_vix=%s cms_th=%s "
        "reject_hist=%s longs_after_vix=%s shorts=%s vix_skipped_long_candidates=%s "
        "index_block_long=%s index_block_short=%s near_miss=%s",
        scan_trigger,
        session_date.isoformat(),
        universe_size,
        vix,
        idx_long_ok,
        idx_short_ok,
        skip_long_vix,
        th,
        dict(reject_counts),
        long_candidates_after_vix,
        short_candidates,
        vix_skipped_long_candidates,
        index_only_long,
        index_only_short,
        json.dumps(near, default=str),
    )


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
    # Align with GET /daily filter (weekend / pre-9:15 map to trading session date).
    session_date = effective_session_date_ist_for_trend(now_ist)

    db = SessionLocal()
    try:
        sentiment_map = _load_sentiment_map(db)
        prior_rows = db.execute(
            text(
                """
                SELECT stock
                FROM smart_futures_daily
                WHERE session_date = :sd
                  AND LOWER(TRIM(COALESCE(order_status, ''))) = 'bought'
                  AND sell_time IS NULL
                """
            ),
            {"sd": session_date},
        ).fetchall()
        already_selected = {
            str(r[0]).strip().upper()
            for r in prior_rows
            if r and r[0] is not None and str(r[0]).strip()
        }
        _bootstrap_reentry_track(db, session_date)
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

    # Index long/short gates disabled: do not fetch NIFTY/BANKNIFTY 5m for alignment.
    idx_long_ok, idx_short_ok = True, True

    reject_counts: Counter[str] = Counter()
    no_entry_outcomes: List[SymbolScoreOutcome] = []
    vix_skipped_long_candidates = 0
    excluded_already_selected = 0

    longs: List[ScoredPick] = []
    shorts: List[ScoredPick] = []
    for stock, fut_sym, fut_key, sector_index in rows:
        if not fut_key:
            continue
        st = str(stock).strip().upper()
        if st in already_selected:
            excluded_already_selected += 1
            continue
        try:
            oc = _score_symbol_outcome(
                upstox,
                st,
                str(fut_sym or ""),
                str(fut_key).strip(),
                session_date,
                sentiment_map,
                str(sector_index).strip() if sector_index else None,
                index_long_ok=idx_long_ok,
                index_short_ok=idx_short_ok,
            )
        except Exception as e:
            reject_counts["exception"] += 1
            logger.debug("smart_futures_picker: skip %s: %s", st, e)
            continue
        if oc.final_cms is not None:
            _reentry_update_scan(st, session_date, float(oc.final_cms))
        if oc.reject_code != "ok":
            reject_counts[oc.reject_code] += 1
            if oc.reject_code == "no_entry_signal":
                no_entry_outcomes.append(oc)
        if not oc.pick:
            continue
        rdb = SessionLocal()
        try:
            rr = _reentry_block_reason(rdb, st, session_date, now_ist, float(oc.pick.final_cms))
        finally:
            rdb.close()
        if rr:
            logger.info("smart_futures_signal %s", json.dumps({"symbol": st, "action": "BLOCKED", "block_reason": rr}, default=str))
            reject_counts["reentry_block"] += 1
            continue
        if oc.pick.side == "LONG":
            if skip_long_vix:
                vix_skipped_long_candidates += 1
                continue
            longs.append(oc.pick)
        else:
            shorts.append(oc.pick)

    longs.sort(key=lambda x: x.final_cms, reverse=True)
    shorts.sort(key=lambda x: x.final_cms)

    tn = max(0, int(SMART_FUTURES_PICK_SELECTION_TOP_N))
    n_long_cap, n_short_cap = buildup_selection_long_short_caps(tn)
    picked_longs = longs[:n_long_cap] if n_long_cap else []
    picked_shorts = shorts[:n_short_cap] if n_short_cap else []
    merged_picks: List[ScoredPick] = picked_longs + picked_shorts
    if not merged_picks and (longs or shorts) and tn >= 1:
        pool = longs + shorts
        merged_picks = pool[: min(tn, len(pool))]

    best_long = longs[0] if longs else None
    best_short = shorts[0] if shorts else None

    if not merged_picks:
        logger.info(
            "smart_futures_picker [%s]: no qualifying picks (longs=%s shorts=%s vix=%s skip_long=%s excluded_already_selected=%s)",
            scan_trigger,
            len(longs),
            len(shorts),
            vix,
            skip_long_vix,
            excluded_already_selected,
        )
        _log_smart_futures_diagnostics(
            scan_trigger,
            session_date,
            len(rows),
            reject_counts,
            idx_long_ok,
            idx_short_ok,
            vix,
            skip_long_vix,
            no_entry_outcomes,
            len(longs),
            len(shorts),
            vix_skipped_long_candidates,
        )
        return {
            "scan_trigger": scan_trigger,
            "picks": 0,
            "vix": vix,
            "skip_long_vix": skip_long_vix,
            "excluded_already_selected": excluded_already_selected,
            "reject_histogram": dict(reject_counts),
            "index_long_ok": idx_long_ok,
            "index_short_ok": idx_short_ok,
            "vix_skipped_long_candidates": vix_skipped_long_candidates,
        }

    dbw: Optional[Session] = None
    try:
        dbw = SessionLocal()
        publish_cap = max(1, int(SMART_FUTURES_MAX_PUBLISH_PER_SCAN))
        saved = 0
        for pick in merged_picks[:publish_cap]:
            tf_ok, tf_reason = time_of_day_filter(now_ist)
            if not tf_ok:
                reject_counts["time_window_order"] += 1
                logger.info(
                    "smart_futures_signal %s",
                    json.dumps(
                        {
                            "symbol": pick.stock,
                            "action": "BLOCKED",
                            "block_reason": tf_reason,
                            "time_filter_passed": False,
                            "block_stage": "order_execution",
                        },
                        default=str,
                    ),
                )
                continue
            entry = _entry_price_1m_close(upstox, pick.fut_instrument_key, now_ist)
            if entry is None:
                q = upstox.get_market_quote_by_key(pick.fut_instrument_key) or {}
                entry = float(q.get("last_price") or 0)
            if not entry or entry <= 0:
                logger.warning("smart_futures_picker: no entry for %s", pick.stock)
                continue
            if _persist_pick(dbw, pick, session_date, scan_trigger or "", vix, entry, now_ist):
                saved += 1

        picked_long_syms = [p.stock for p in picked_longs]
        picked_short_syms = [p.stock for p in picked_shorts]
        logger.info(
            "smart_futures_picker [%s]: saved=%s publish_cap=%s vix=%s excluded_already_selected=%s "
            "caps long=%s short=%s tn=%s picked_long=%s picked_short=%s merged=%s",
            scan_trigger,
            saved,
            publish_cap,
            vix,
            excluded_already_selected,
            n_long_cap,
            n_short_cap,
            tn,
            picked_long_syms,
            picked_short_syms,
            [p.stock for p in merged_picks],
        )
        logger.debug(
            "smart_futures_picker [%s] reject_hist=%s no_entry_count=%s",
            scan_trigger,
            dict(reject_counts),
            len(no_entry_outcomes),
        )
        return {
            "scan_trigger": scan_trigger,
            "picks": saved,
            "vix": vix,
            "excluded_already_selected": excluded_already_selected,
            "best_long": best_long.stock if best_long else None,
            "best_short": best_short.stock if best_short else None,
            "pick_selection_top_n": tn,
            "pick_selection_long_cap": n_long_cap,
            "pick_selection_short_cap": n_short_cap,
            "picked_long": picked_long_syms,
            "picked_short": picked_short_syms,
            "merged_pick_symbols": [p.stock for p in merged_picks],
            "merged_pick_details": [
                {
                    "stock": p.stock,
                    "side": p.side,
                    "history_days_used": int(p.history_days_used),
                    "history_status": p.history_status,
                }
                for p in merged_picks
            ],
            "publish_cap": publish_cap,
            "reject_histogram": dict(reject_counts),
        }
    finally:
        if dbw is not None:
            dbw.close()

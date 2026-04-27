"""
Daily Futures — ChartInk webhook → ``arbitrage_master`` **current** future columns
(``currmth_future_symbol`` / ``currmth_future_instrument_key``), Upstox LTP + conviction.
"""
from __future__ import annotations

import logging
import os
import re
import time
import json
import uuid
import threading
from datetime import date, datetime, time as dt_time, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import quote

import pytz
from sqlalchemy import text
from sqlalchemy.orm import Session

from backend.config import settings
from backend.database import engine
from backend.services.fno_bullish_backtest import (
    TradeRow,
    _fill_conviction_raw_metrics,
    finalize_conviction_scores,
)
from backend.services.nks_intraday_backtest import (
    _bucket_candles_by_hhmm,
    _index_instruments,
    _load_instruments,
    fetch_intraday_1m_candles,
)
from backend.services.upstox_service import UpstoxService, _candles_rows_to_structured

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")

_INSTRUMENT_CACHE: Optional[Tuple[Any, Any]] = None
_DF_INTRADAY_1M_CACHE: Dict[Tuple[str, str], Dict[str, Any]] = {}
_DF_PREV_CLOSE_CACHE: Dict[str, Any] = {"trade_date": None, "stock": {}, "nifty": None}
NIFTY50_INDEX_KEY = "NSE_INDEX|Nifty 50"


def _bearish_index_gate_enabled() -> bool:
    """
    When True (default), bearish pick visibility and SHORT order entry require NIFTY LTP
    below the NIFTY session day open. Set DAILY_FUTURES_BEARISH_INDEX_GATE_ENABLED=0|off|no|false to disable.
    """
    raw = os.getenv("DAILY_FUTURES_BEARISH_INDEX_GATE_ENABLED")
    if raw is None or (isinstance(raw, str) and raw.strip() == ""):
        return True
    v = str(raw).strip().lower()
    if v in ("0", "false", "no", "off"):
        return False
    return True
_DF_TABLES_READY = False
_DF_TABLES_LOCK = threading.Lock()


def _instruments_index():
    global _INSTRUMENT_CACHE
    if _INSTRUMENT_CACHE is None:
        instruments = _load_instruments()
        _INSTRUMENT_CACHE = _index_instruments(instruments)
    return _INSTRUMENT_CACHE


def fut_lot_for_key(instrument_key: str) -> Optional[int]:
    fut_by_und, _eq = _instruments_index()
    for _sym, lst in fut_by_und.items():
        for inst in lst or []:
            if (inst.get("instrument_key") or "").strip() == instrument_key:
                ls = inst.get("lot_size")
                try:
                    return int(ls) if ls is not None else None
                except (TypeError, ValueError):
                    return None
    return None


def ensure_daily_futures_tables() -> None:
    global _DF_TABLES_READY
    if _DF_TABLES_READY:
        return
    with _DF_TABLES_LOCK:
        if _DF_TABLES_READY:
            return
        ddl = """
    CREATE TABLE IF NOT EXISTS daily_futures_screening (
        id SERIAL PRIMARY KEY,
        trade_date DATE NOT NULL,
        underlying VARCHAR(64) NOT NULL,
        direction_type VARCHAR(16) NOT NULL DEFAULT 'LONG',
        future_symbol TEXT,
        instrument_key TEXT NOT NULL,
        lot_size INTEGER,
        scan_count INTEGER NOT NULL DEFAULT 1,
        first_hit_at TIMESTAMPTZ,
        last_hit_at TIMESTAMPTZ,
        conviction_score NUMERIC(8,2) NOT NULL DEFAULT 0,
        conviction_oi_leg NUMERIC(8,2),
        conviction_vwap_leg NUMERIC(8,2),
        ltp NUMERIC(18,4),
        session_vwap NUMERIC(18,4),
        total_oi BIGINT,
        oi_change_pct NUMERIC(18,6),
        nifty_ltp NUMERIC(18,4),
        nifty_session_vwap NUMERIC(18,4),
        stock_prev_close NUMERIC(18,4),
        nifty_prev_close NUMERIC(18,4),
        stock_change_pct NUMERIC(18,6),
        nifty_change_pct NUMERIC(18,6),
        snapshotted_at TIMESTAMPTZ,
        second_scan_time TIMESTAMPTZ,
        second_scan_conviction_score NUMERIC(8,2),
        second_scan_stock_change_pct NUMERIC(18,6),
        second_scan_nifty_change_pct NUMERIC(18,6),
        candle_is_green BOOLEAN,
        candle_higher_high BOOLEAN,
        candle_higher_low BOOLEAN,
        conviction_breakdown_json JSONB,
        updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (trade_date, underlying)
    );
    CREATE INDEX IF NOT EXISTS idx_dfs_trade_date ON daily_futures_screening (trade_date);

    CREATE TABLE IF NOT EXISTS daily_futures_user_trade (
        id SERIAL PRIMARY KEY,
        user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
        screening_id INTEGER NOT NULL REFERENCES daily_futures_screening(id) ON DELETE CASCADE,
        underlying VARCHAR(64) NOT NULL,
        direction_type VARCHAR(16) NOT NULL DEFAULT 'LONG',
        future_symbol TEXT,
        instrument_key TEXT,
        lot_size INTEGER,
        order_status VARCHAR(16) NOT NULL DEFAULT 'bought',
        entry_time VARCHAR(16),
        entry_price NUMERIC(18,4),
        exit_time VARCHAR(16),
        exit_price NUMERIC(18,4),
        pnl_points NUMERIC(18,4),
        pnl_rupees NUMERIC(18,4),
        consecutive_webhook_misses INTEGER NOT NULL DEFAULT 0,
        position_atr NUMERIC(18,4),
        profit_trail_armed BOOLEAN NOT NULL DEFAULT FALSE,
        nifty_structure_weakening BOOLEAN NOT NULL DEFAULT FALSE,
        trail_stop_hit BOOLEAN NOT NULL DEFAULT FALSE,
        momentum_exhausting BOOLEAN NOT NULL DEFAULT FALSE,
        created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
    );
    CREATE INDEX IF NOT EXISTS idx_dfut_user_status ON daily_futures_user_trade (user_id, order_status);
    """
        with engine.begin() as conn:
            conn.execute(text(ddl))
            # Safe additive migrations for existing databases.
            conn.execute(text("ALTER TABLE daily_futures_screening ADD COLUMN IF NOT EXISTS conviction_oi_leg NUMERIC(8,2)"))
            conn.execute(text("ALTER TABLE daily_futures_screening ADD COLUMN IF NOT EXISTS conviction_vwap_leg NUMERIC(8,2)"))
            conn.execute(text("ALTER TABLE daily_futures_screening ADD COLUMN IF NOT EXISTS session_vwap NUMERIC(18,4)"))
            conn.execute(text("ALTER TABLE daily_futures_screening ADD COLUMN IF NOT EXISTS total_oi BIGINT"))
            conn.execute(text("ALTER TABLE daily_futures_screening ADD COLUMN IF NOT EXISTS oi_change_pct NUMERIC(18,6)"))
            conn.execute(text("ALTER TABLE daily_futures_screening ADD COLUMN IF NOT EXISTS nifty_ltp NUMERIC(18,4)"))
            conn.execute(text("ALTER TABLE daily_futures_screening ADD COLUMN IF NOT EXISTS nifty_session_vwap NUMERIC(18,4)"))
            conn.execute(text("ALTER TABLE daily_futures_screening ADD COLUMN IF NOT EXISTS stock_prev_close NUMERIC(18,4)"))
            conn.execute(text("ALTER TABLE daily_futures_screening ADD COLUMN IF NOT EXISTS nifty_prev_close NUMERIC(18,4)"))
            conn.execute(text("ALTER TABLE daily_futures_screening ADD COLUMN IF NOT EXISTS stock_change_pct NUMERIC(18,6)"))
            conn.execute(text("ALTER TABLE daily_futures_screening ADD COLUMN IF NOT EXISTS nifty_change_pct NUMERIC(18,6)"))
            conn.execute(text("ALTER TABLE daily_futures_screening ADD COLUMN IF NOT EXISTS snapshotted_at TIMESTAMPTZ"))
            conn.execute(text("ALTER TABLE daily_futures_screening ADD COLUMN IF NOT EXISTS second_scan_time TIMESTAMPTZ"))
            conn.execute(text("ALTER TABLE daily_futures_screening ADD COLUMN IF NOT EXISTS second_scan_conviction_score NUMERIC(8,2)"))
            conn.execute(text("ALTER TABLE daily_futures_screening ADD COLUMN IF NOT EXISTS second_scan_stock_change_pct NUMERIC(18,6)"))
            conn.execute(text("ALTER TABLE daily_futures_screening ADD COLUMN IF NOT EXISTS second_scan_nifty_change_pct NUMERIC(18,6)"))
            conn.execute(text("ALTER TABLE daily_futures_screening ADD COLUMN IF NOT EXISTS candle_is_green BOOLEAN"))
            conn.execute(text("ALTER TABLE daily_futures_screening ADD COLUMN IF NOT EXISTS candle_higher_high BOOLEAN"))
            conn.execute(text("ALTER TABLE daily_futures_screening ADD COLUMN IF NOT EXISTS candle_higher_low BOOLEAN"))
            conn.execute(text("ALTER TABLE daily_futures_screening ADD COLUMN IF NOT EXISTS conviction_breakdown_json JSONB"))
            conn.execute(text("ALTER TABLE daily_futures_screening ADD COLUMN IF NOT EXISTS direction_type VARCHAR(16)"))
            conn.execute(text("UPDATE daily_futures_screening SET direction_type = 'LONG' WHERE direction_type IS NULL OR TRIM(direction_type) = ''"))
            conn.execute(text("ALTER TABLE daily_futures_screening ALTER COLUMN direction_type SET DEFAULT 'LONG'"))
            conn.execute(text("UPDATE daily_futures_screening SET conviction_score = 0 WHERE conviction_score IS NULL"))
            conn.execute(text("ALTER TABLE daily_futures_screening ALTER COLUMN conviction_score SET DEFAULT 0"))
            conn.execute(text("ALTER TABLE daily_futures_screening ALTER COLUMN conviction_score SET NOT NULL"))
            conn.execute(
                text("ALTER TABLE daily_futures_user_trade ADD COLUMN IF NOT EXISTS position_atr NUMERIC(18,4)")
            )
            conn.execute(
                text(
                    "ALTER TABLE daily_futures_user_trade ADD COLUMN IF NOT EXISTS profit_trail_armed BOOLEAN NOT NULL DEFAULT FALSE"
                )
            )
            conn.execute(
                text(
                    "ALTER TABLE daily_futures_user_trade ADD COLUMN IF NOT EXISTS nifty_structure_weakening BOOLEAN NOT NULL DEFAULT FALSE"
                )
            )
            conn.execute(
                text("ALTER TABLE daily_futures_user_trade ADD COLUMN IF NOT EXISTS trail_stop_hit BOOLEAN NOT NULL DEFAULT FALSE")
            )
            conn.execute(
                text(
                    "ALTER TABLE daily_futures_user_trade ADD COLUMN IF NOT EXISTS momentum_exhausting BOOLEAN NOT NULL DEFAULT FALSE"
                )
            )
            conn.execute(text("ALTER TABLE daily_futures_user_trade ADD COLUMN IF NOT EXISTS direction_type VARCHAR(16)"))
            conn.execute(text("UPDATE daily_futures_user_trade SET direction_type = 'LONG' WHERE direction_type IS NULL OR TRIM(direction_type) = ''"))
            conn.execute(text("ALTER TABLE daily_futures_user_trade ALTER COLUMN direction_type SET DEFAULT 'LONG'"))
            # SHORT leg prices (optional; LONG uses entry_ / exit_ as today)
            conn.execute(text("ALTER TABLE daily_futures_user_trade ADD COLUMN IF NOT EXISTS sell_price NUMERIC(18,4)"))
            conn.execute(text("ALTER TABLE daily_futures_user_trade ADD COLUMN IF NOT EXISTS sell_time VARCHAR(16)"))
            conn.execute(text("ALTER TABLE daily_futures_user_trade ADD COLUMN IF NOT EXISTS buy_price NUMERIC(18,4)"))
            conn.execute(text("ALTER TABLE daily_futures_user_trade ADD COLUMN IF NOT EXISTS buy_time VARCHAR(16)"))
            # Allow same underlying on one day for LONG vs SHORT parallel screeners
            try:
                conn.execute(
                    text(
                        "ALTER TABLE daily_futures_screening DROP CONSTRAINT IF EXISTS daily_futures_screening_trade_date_underlying_key"
                    )
                )
            except Exception:
                pass
            conn.execute(
                text(
                    "CREATE UNIQUE INDEX IF NOT EXISTS uq_daily_futures_screening_td_und_dir "
                    "ON daily_futures_screening (trade_date, (UPPER(TRIM(underlying))), (UPPER(TRIM(direction_type))))"
                )
            )
        _DF_TABLES_READY = True


def _safe_float(v: Any) -> Optional[float]:
    try:
        if v is None or v == "":
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def _vwap_leg_score_reason(
    price_vs_vwap_pct: Optional[float],
    candle_is_green: bool = False,
    candle_higher_high: bool = False,
    candle_higher_low: bool = False,
) -> Tuple[float, str]:
    p = _safe_float(price_vs_vwap_pct)
    if p is None:
        return 12.0, "no_price_vs_vwap"
    if p < -0.5:
        return 5.0, "below_vwap_gt_0.5pct"
    if p < 0:
        return 15.0, "below_vwap_lt_0.5pct"
    if 0 <= p <= 0.8:
        return 30.0 + (p / 0.8) * 20.0, "sweet_spot_0_to_0.8"
    trend_score = int(bool(candle_is_green)) + int(bool(candle_higher_high)) + int(bool(candle_higher_low))
    if trend_score == 3:
        return max(50.0 - min(p - 0.8, 3.0) * 2.0, 42.0), "above_0.8_all_three_trend"
    if trend_score == 2:
        return 35.0, "above_0.8_two_trend"
    if trend_score == 1:
        return 22.0, "above_0.8_one_trend"
    return 12.0, "above_0.8_no_trend"


def _vwap_leg_score_reason_bearish(
    price_vs_vwap_pct: Optional[float],
    nifty15_close_below_sv: bool,
    stock15_close_below_sv: bool,
    candle_is_red: bool = False,
    candle_lower_low: bool = False,
    candle_lower_high: bool = False,
) -> Tuple[float, str]:
    """
    Bearish VWAP leg: reward negative pvp (price below session VWAP), and require
    Nifty 15m + stock 15m closes below their session VWAPs when data exists.
    """
    if not nifty15_close_below_sv or not stock15_close_below_sv:
        return 8.0, "nifty_or_stock_15m_not_below_session_vwap"
    p = _safe_float(price_vs_vwap_pct)
    if p is None:
        return 10.0, "no_price_vs_vwap_bear"
    if p > 0.5:
        return 6.0, "above_vwap_gt_0.5pct_bear"
    if p > 0:
        return 14.0, "above_vwap_small_bear"
    if -0.8 <= p <= 0:
        return 30.0 + (abs(p) / 0.8) * 20.0, "bear_sweet_spot_0_to_0.8"
    trend_score = int(bool(candle_is_red)) + int(bool(candle_lower_low)) + int(bool(candle_lower_high))
    if trend_score == 3:
        return max(50.0 - min(abs(p) - 0.8, 3.0) * 2.0, 42.0), "deep_below_vwap_three"
    if trend_score == 2:
        return 35.0, "deep_below_vwap_two"
    if trend_score == 1:
        return 22.0, "deep_below_vwap_one"
    return 12.0, "deep_below_vwap_none"


def _momentum_bounce_fading_for_short(cands: List[Dict[str, Any]]) -> bool:
    """SHORT position: intraday bounce losing steam (mirror of long momentum fade)."""
    if len(cands) < 2:
        return False
    a, b = cands[-2], cands[-1]
    try:
        body_a = abs(float(a.get("close")) - float(a.get("open")))
        body_b = abs(float(b.get("close")) - float(b.get("open")))
        hi = float(b.get("high"))
        lo = float(b.get("low"))
        cl = float(b.get("close"))
    except (TypeError, ValueError):
        return False
    rng = hi - lo
    if rng <= 0:
        return False
    close_from_top = (hi - cl) / rng
    return bool(body_b < body_a and close_from_top < 0.30)


def _vwap_proximity_score_0_50(
    price_vs_vwap_pct: Optional[float],
    candle_is_green: bool = False,
    candle_higher_high: bool = False,
    candle_higher_low: bool = False,
) -> float:
    score, _reason = _vwap_leg_score_reason(
        price_vs_vwap_pct,
        candle_is_green=candle_is_green,
        candle_higher_high=candle_higher_high,
        candle_higher_low=candle_higher_low,
    )
    return float(score)


def _quote_session_vwap(snapshot: Dict[str, Any]) -> Optional[float]:
    if not isinstance(snapshot, dict):
        return None
    for k in ("vwap", "session_vwap", "average_price", "avg_price", "average_traded_price", "atp"):
        fv = _safe_float(snapshot.get(k))
        if fv is not None and fv > 0:
            return fv
    ohlc = snapshot.get("ohlc") if isinstance(snapshot.get("ohlc"), dict) else {}
    for k in ("vwap", "average_price", "avg_price", "average_traded_price", "atp"):
        fv = _safe_float(ohlc.get(k))
        if fv is not None and fv > 0:
            return fv
    return None


def _typical_price_ohlc_fallback(snapshot: Dict[str, Any]) -> Optional[float]:
    """
    When Upstox does not return session VWAP/ATP, approximate fair value from OHLC
    so the VWAP leg is not stuck at a neutral 25 for every symbol.
    """
    if not isinstance(snapshot, dict):
        return None
    ohlc = snapshot.get("ohlc") if isinstance(snapshot.get("ohlc"), dict) else {}
    h = _safe_float(ohlc.get("high"))
    low = _safe_float(ohlc.get("low"))
    c = _safe_float(ohlc.get("close"))
    lp = _safe_float(snapshot.get("last_price"))
    close = c if c is not None and c > 0 else (lp if lp is not None and lp > 0 else None)
    if h and low and close and h > 0 and low > 0 and close > 0:
        return (h + low + close) / 3.0
    return None


def _session_vwap_for_conviction(quote: Dict[str, Any]) -> Optional[float]:
    v = _quote_session_vwap(quote) or _typical_price_ohlc_fallback(quote)
    return v


def _floor_ist_to_15m(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        dt = IST.localize(dt)
    else:
        dt = dt.astimezone(IST)
    mm = (dt.minute // 15) * 15
    return dt.replace(minute=mm, second=0, microsecond=0)


def _prev_15m_close_for_instrument(
    upstox: UpstoxService,
    instrument_key: str,
    ref_dt_ist: datetime,
    cache: Dict[str, Optional[float]],
) -> Optional[float]:
    ik = str(instrument_key or "").strip()
    if not ik:
        return None
    if ik in cache:
        return cache[ik]
    try:
        raw = upstox.get_historical_candles_by_instrument_key(
            ik,
            interval="minutes/15",
            days_back=2,
            range_end_date=ref_dt_ist.date(),
        ) or []
    except Exception as e:
        logger.debug("daily_futures: prev15 fetch failed for %s: %s", ik, e)
        cache[ik] = None
        return None

    cutoff = _floor_ist_to_15m(ref_dt_ist)
    candles: List[Tuple[datetime, float]] = []
    for c in raw:
        ts = _parse_iso_ist(c.get("timestamp"))
        if ts is None:
            continue
        cl = _safe_float(c.get("close"))
        if cl is None or cl <= 0:
            continue
        candles.append((ts, float(cl)))
    candles.sort(key=lambda x: x[0])
    if not candles:
        cache[ik] = None
        return None

    prev_candidates = [cl for ts, cl in candles if ts < cutoff]
    val = prev_candidates[-1] if prev_candidates else candles[-1][1]
    cache[ik] = float(val) if val and val > 0 else None
    return cache[ik]


def _last_completed_15m_candles_for_instrument(
    upstox: UpstoxService,
    instrument_key: str,
    session_date: date,
    now_ist: datetime,
) -> List[Dict[str, Any]]:
    ik = str(instrument_key or "").strip()
    if not ik:
        return []
    try:
        raw = upstox.get_historical_candles_by_instrument_key(
            ik, interval="minutes/15", days_back=3, range_end_date=session_date
        ) or []
    except Exception as e:
        logger.debug("daily_futures: 15m candle fetch failed for %s: %s", ik, e)
        raw = []

    cutoff = _floor_ist_to_15m(now_ist)

    def _rows_to_completed(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        out_local: List[Dict[str, Any]] = []
        for c in rows or []:
            ts = _parse_iso_ist(c.get("timestamp"))
            if ts is None or ts.date() != session_date or ts >= cutoff:
                continue
            op = _safe_float(c.get("open"))
            hi = _safe_float(c.get("high"))
            lo = _safe_float(c.get("low"))
            cl = _safe_float(c.get("close"))
            if op is None or hi is None or lo is None or cl is None:
                continue
            out_local.append({"timestamp": ts, "open": op, "high": hi, "low": lo, "close": cl})
        out_local.sort(key=lambda x: x["timestamp"])
        return out_local

    out = _rows_to_completed(raw)

    # Upstox historical 15m can return prior session intraday while market is open.
    # For today's strip (L1/L3), fallback to intraday 15m endpoint to ensure current-session bars.
    if not out and session_date == ist_today():
        try:
            key_enc = quote(ik, safe="")
            intraday_url = f"{upstox.base_url}/historical-candle/intraday/{key_enc}/minutes/15"
            raw_i = upstox.make_api_request(intraday_url, method="GET", timeout=15, max_retries=2) or {}
            if isinstance(raw_i, dict) and raw_i.get("status") == "success":
                rows_i = ((raw_i.get("data") or {}).get("candles")) or []
                structured_i = _candles_rows_to_structured(rows_i) or []
                out = _rows_to_completed(structured_i)
                if out:
                    logger.info("daily_futures: 15m fallback intraday used for %s bars=%d", ik, len(out))
        except Exception as e:
            logger.debug("daily_futures: 15m intraday fallback failed for %s: %s", ik, e)

    return out[-5:]


def _entry_datetime_ist(trade_d: date, entry_time: Optional[str]) -> Optional[datetime]:
    if not entry_time or not str(entry_time).strip():
        return None
    parts = str(entry_time).strip().split(":")
    if len(parts) < 2:
        return None
    try:
        h = int(parts[0].strip())
        m = int(parts[1].strip()[:2])
    except (TypeError, ValueError):
        return None
    return IST.localize(datetime.combine(trade_d, dt_time(h, m)))


def _compute_position_atr_15m_5d(
    upstox: UpstoxService,
    instrument_key: str,
    as_of: date,
) -> Optional[float]:
    """
    Mean of (15m high − low) across all 15m bars in the last five distinct
    session dates present in the returned history.
    """
    ik = str(instrument_key or "").strip()
    if not ik:
        return None
    try:
        raw = upstox.get_historical_candles_by_instrument_key(
            ik, interval="minutes/15", days_back=20, range_end_date=as_of
        ) or []
    except Exception as e:
        logger.debug("daily_futures: ATR 15m fetch failed for %s: %s", ik, e)
        return None
    bars: List[Tuple[date, float]] = []
    for c in raw:
        ts = _parse_iso_ist(c.get("timestamp"))
        if ts is None:
            continue
        d = ts.astimezone(IST).date()
        hi = _safe_float(c.get("high"))
        lo = _safe_float(c.get("low"))
        if hi is None or lo is None:
            continue
        rng = float(hi) - float(lo)
        if rng < 0:
            continue
        bars.append((d, rng))
    if not bars:
        return None
    uniq = sorted({d for d, _ in bars})
    use_dates: Set[date] = set(uniq[-5:]) if len(uniq) >= 5 else set(uniq)
    rs = [r for d, r in bars if d in use_dates]
    if not rs:
        return None
    return float(sum(rs)) / float(len(rs))


def _momentum_exhausting_last_two(cands: List[Dict[str, Any]]) -> bool:
    """
    For the last two completed 15m candles (prev, last): body shrinks and weak close
    in the latest bar. close_pct = (close-low)/(high-low), skip if range is zero.
    """
    if len(cands) < 2:
        return False
    a, b = cands[-2], cands[-1]
    try:
        body_a = abs(float(a.get("close")) - float(a.get("open")))
        body_b = abs(float(b.get("close")) - float(b.get("open")))
        hi = float(b.get("high"))
        lo = float(b.get("low"))
        cl = float(b.get("close"))
    except (TypeError, ValueError):
        return False
    rng = hi - lo
    if rng <= 0:
        return False
    close_pct = (cl - lo) / rng
    return bool(body_b < body_a and close_pct < 0.30)


def _nifty_momentum_state_last_two_closes(cands: List[Dict[str, Any]], thr_pct: float) -> str:
    """
    Return one of:
      - nifty_higher_high      (close-to-close change > +threshold)
      - nifty_lower_low        (close-to-close change < -threshold)
      - nifty_no_higher_high   (0 <= change <= +threshold)
      - nifty_no_lower_low     (-threshold <= change < 0)
    """
    if len(cands) < 2:
        return "nifty_no_higher_high"
    try:
        prev_close = float(cands[-2].get("close"))
        curr_close = float(cands[-1].get("close"))
    except (TypeError, ValueError):
        return "nifty_no_higher_high"
    if prev_close <= 0:
        return "nifty_no_higher_high"

    thr_pct = max(0.0, float(thr_pct))
    move_pct = ((curr_close - prev_close) / prev_close) * 100.0
    if move_pct > thr_pct:
        return "nifty_higher_high"
    if move_pct < -thr_pct:
        return "nifty_lower_low"
    if move_pct >= 0:
        return "nifty_no_higher_high"
    return "nifty_no_lower_low"


def _resolve_nifty_momentum_threshold_pct(
    upstox: UpstoxService,
    trade_date: date,
    nifty_cands: List[Dict[str, Any]],
) -> float:
    """
    Resolve threshold pct used for Nifty momentum classification.
    - fixed mode: direct config pct
    - atr mode: (ATR multiplier * Nifty 15m ATR(5d)) as percent of latest close
    Falls back to fixed pct on any missing data.
    """
    fixed_thr_pct = max(0.0, float(settings.DAILY_FUTURES_NIFTY_MOMENTUM_THRESHOLD_PCT))
    mode = str(getattr(settings, "DAILY_FUTURES_NIFTY_MOMENTUM_MODE", "fixed") or "fixed").lower().strip()
    if mode != "atr":
        return fixed_thr_pct

    atr_mult = max(0.0, float(getattr(settings, "DAILY_FUTURES_NIFTY_MOMENTUM_ATR_MULTIPLIER", 0.25)))
    if atr_mult <= 0:
        return fixed_thr_pct
    # Use already-fetched completed 15m Nifty candles for ATR-like range estimate.
    # This avoids an additional historical API round-trip during workspace polling.
    if not nifty_cands:
        return fixed_thr_pct
    ranges: List[float] = []
    for c in nifty_cands:
        try:
            hi = float(c.get("high"))
            lo = float(c.get("low"))
        except (TypeError, ValueError):
            continue
        rng = hi - lo
        if rng > 0:
            ranges.append(rng)
    if not ranges:
        return fixed_thr_pct
    atr_abs = float(sum(ranges)) / float(len(ranges))
    try:
        close_ref = float((nifty_cands[-1] or {}).get("close")) if nifty_cands else 0.0
    except (TypeError, ValueError):
        close_ref = 0.0
    if close_ref <= 0:
        return fixed_thr_pct
    dyn_pct = (atr_mult * float(atr_abs) / float(close_ref)) * 100.0
    if dyn_pct <= 0:
        return fixed_thr_pct
    return float(dyn_pct)


def _apply_exit_alerts_to_running(
    db: Session,
    running: List[Dict[str, Any]],
    trade_date: date,
) -> Dict[str, Any]:
    if not running:
        return {}
    now_ist = datetime.now(IST)
    try:
        upstox = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    except Exception as e:
        logger.warning("daily_futures: exit alerts Upstox init failed: %s", e)
        for r in running:
            r["nifty_structure_weakening"] = bool(r.get("nifty_structure_weakening"))
            r["trail_stop_hit"] = bool(r.get("trail_stop_hit"))
            r["momentum_exhausting"] = bool(r.get("momentum_exhausting"))
            r["exit_review"] = bool(r.get("trail_stop_hit")) or (
                bool(r.get("nifty_structure_weakening")) and bool(r.get("momentum_exhausting"))
            )
            r["alert_strip"] = {
                "l1": "nifty_no_higher_high",
                "l2": "building",
                "l3": "strong",
                "decision": "hold",
            }
        return {}

    nifty_cands: List[Dict[str, Any]] = []
    try:
        nifty_cands = _last_completed_15m_candles_for_instrument(
            upstox, NIFTY50_INDEX_KEY, trade_date, now_ist
        )
    except Exception as e:
        logger.debug("daily_futures: exit alerts nifty 15m: %s", e)

    nifty_thr_pct = _resolve_nifty_momentum_threshold_pct(upstox, trade_date, nifty_cands)
    nifty_l1_state = _nifty_momentum_state_last_two_closes(nifty_cands, nifty_thr_pct)
    nifty_weakening = nifty_l1_state == "nifty_lower_low"
    nifty_against_short = nifty_l1_state == "nifty_higher_high"
    stock_candle_cache: Dict[str, List[Dict[str, Any]]] = {}

    for r in running:
        tid = int(r["trade_id"])
        ikey = str(r.get("instrument_key") or "").strip()
        dirt = str(r.get("direction_type") or "LONG").strip().upper()
        if dirt == "SHORT":
            ep = _safe_float(r.get("sell_price"))
            tstr = r.get("sell_time") or r.get("entry_time")
            entry_dt = _entry_datetime_ist(trade_date, tstr)
        else:
            ep = _safe_float(r.get("entry_price"))
            entry_dt = _entry_datetime_ist(trade_date, r.get("entry_time"))
        ltp = _safe_float(r.get("ltp"))
        pos_age_s = (now_ist - entry_dt).total_seconds() if entry_dt else 0.0
        pos_age_ok = pos_age_s > 45.0 * 60.0

        atr_v = r.get("position_atr")
        atr: Optional[float] = float(atr_v) if atr_v is not None else None
        if ikey and atr is None:
            atr = _compute_position_atr_15m_5d(upstox, ikey, trade_date)
            if atr is not None and atr > 0:
                r["position_atr"] = round(float(atr), 4)

        old_n = bool(r.get("nifty_structure_weakening"))
        if dirt == "SHORT":
            n_sig = bool(nifty_against_short and pos_age_ok and entry_dt is not None)
        else:
            n_sig = bool(nifty_weakening and pos_age_ok and entry_dt is not None)
        new_n = n_sig
        merged_n = old_n or new_n

        if ikey and ikey not in stock_candle_cache:
            try:
                stock_candle_cache[ikey] = _last_completed_15m_candles_for_instrument(
                    upstox, ikey, trade_date, now_ist
                )
            except Exception as e:
                logger.debug("daily_futures: exit alerts stock 15m %s: %s", ikey, e)
                stock_candle_cache[ikey] = []
        stk = stock_candle_cache.get(ikey) or []
        old_m = bool(r.get("momentum_exhausting"))
        if dirt == "SHORT":
            new_m = _momentum_bounce_fading_for_short(stk)
        else:
            new_m = _momentum_exhausting_last_two(stk)
        merged_m = old_m or new_m

        old_armed = bool(r.get("profit_trail_armed"))
        if dirt == "SHORT":
            new_armed = bool(
                atr is not None
                and atr > 0
                and ep is not None
                and ltp is not None
                and (float(ep) - float(ltp)) >= 1.5 * float(atr)
            )
        else:
            new_armed = bool(
                atr is not None
                and atr > 0
                and ep is not None
                and ltp is not None
                and (ltp - ep) >= 1.5 * float(atr)
            )
        merged_armed = old_armed or new_armed
        old_hit = bool(r.get("trail_stop_hit"))
        if dirt == "SHORT":
            new_hit = bool(
                merged_armed
                and ep is not None
                and ltp is not None
                and atr is not None
                and atr > 0
                and float(ltp) > float(ep) - 0.8 * float(atr)
            )
        else:
            new_hit = bool(
                merged_armed
                and ep is not None
                and ltp is not None
                and atr is not None
                and atr > 0
                and ltp < ep + 0.8 * float(atr)
            )
        merged_hit = old_hit or new_hit

        if dirt == "SHORT":
            drawdown_15atr_breach = bool(
                entry_dt is not None
                and pos_age_s > 45.0 * 60.0
                and ep is not None
                and ltp is not None
                and atr is not None
                and float(atr) > 0.0
                and float(ltp) > float(ep)
                and (float(ltp) - float(ep)) >= 1.5 * float(atr)
            )
        else:
            # Long underwater…
            drawdown_15atr_breach = bool(
                entry_dt is not None
                and pos_age_s > 45.0 * 60.0
                and ep is not None
                and ltp is not None
                and atr is not None
                and float(atr) > 0.0
                and float(ltp) < float(ep)
                and (float(ep) - float(ltp)) >= 1.5 * float(atr)
            )

        exit_review = bool(merged_hit or (merged_n and merged_m) or drawdown_15atr_breach)

        l1_amber = bool(nifty_weakening) if dirt != "SHORT" else bool(nifty_against_short)
        l3_fading = bool(new_m)
        if merged_hit:
            l2k = "hit"
        elif merged_armed:
            l2k = "active"
        else:
            l2k = "building"
        if merged_hit:
            as_dec = "lock_profit"
        elif l1_amber and l3_fading:
            as_dec = "dual_exit"
        elif l1_amber or l3_fading:
            as_dec = "watch"
        else:
            as_dec = "hold"
        r["alert_strip"] = {
            "l1": nifty_l1_state,
            "l2": l2k,
            "l3": "fading" if l3_fading else "strong",
            "decision": as_dec,
        }

        try:
            db.execute(
                text(
                    """
                    UPDATE daily_futures_user_trade SET
                      position_atr = COALESCE(CAST(:atr AS NUMERIC), position_atr),
                      profit_trail_armed = CAST(:ar AS BOOLEAN),
                      nifty_structure_weakening = CAST(:ns AS BOOLEAN),
                      trail_stop_hit = CAST(:th AS BOOLEAN),
                      momentum_exhausting = CAST(:me AS BOOLEAN),
                      updated_at = CURRENT_TIMESTAMP
                    WHERE id = :id
                    """
                ),
                {
                    "atr": float(atr) if atr is not None else None,
                    "ar": merged_armed,
                    "ns": merged_n,
                    "th": merged_hit,
                    "me": merged_m,
                    "id": tid,
                },
            )
        except Exception as e:
            logger.warning("daily_futures: exit alert persist failed trade_id=%s: %s", tid, e)

        r["nifty_structure_weakening"] = merged_n
        r["momentum_exhausting"] = merged_m
        r["trail_stop_hit"] = merged_hit
        r["profit_trail_armed"] = merged_armed
        r["exit_review"] = exit_review
        r["drawdown_15atr_breach"] = drawdown_15atr_breach
        r["position_atr"] = round(float(atr), 4) if atr is not None else r.get("position_atr")

    try:
        db.commit()
    except Exception as e:
        logger.warning("daily_futures: exit alerts commit: %s", e)
        db.rollback()
    return {
        "nifty_momentum_mode": str(getattr(settings, "DAILY_FUTURES_NIFTY_MOMENTUM_MODE", "fixed")),
        "nifty_momentum_threshold_pct_effective": round(float(nifty_thr_pct), 6),
    }


def _prev_close_from_snapshot(snapshot: Dict[str, Any]) -> Optional[float]:
    if not isinstance(snapshot, dict):
        return None
    ohlc = snapshot.get("ohlc") if isinstance(snapshot.get("ohlc"), dict) else {}
    # Preferred: broker previous close from OHLC payload.
    pc = _safe_float(ohlc.get("close"))
    if pc is not None and pc > 0:
        return pc

    # Some quote payloads expose close directly (without nested OHLC close).
    pc2 = _safe_float(snapshot.get("close_price")) or _safe_float(snapshot.get("close"))
    if pc2 is not None and pc2 > 0:
        return pc2

    # Last-resort derivation: previous close ~= last_price - net_change.
    # Upstox net_change is vs previous close for the session.
    lp = _safe_float(snapshot.get("last_price"))
    nc = _safe_float(snapshot.get("net_change"))
    if lp is not None and nc is not None:
        d = lp - nc
        if d > 0:
            return d
    return None


def _get_or_init_prev_close_cache(
    trade_date: date,
    upstox: UpstoxService,
    symbol_to_key: Dict[str, str],
) -> Dict[str, Any]:
    td = str(trade_date)
    global _DF_PREV_CLOSE_CACHE
    cache_same_day = _DF_PREV_CLOSE_CACHE.get("trade_date") == td
    cached_stock = dict(_DF_PREV_CLOSE_CACHE.get("stock") or {}) if cache_same_day else {}
    cached_nifty = _DF_PREV_CLOSE_CACHE.get("nifty") if cache_same_day else None

    # Refresh only missing symbols for the current day cache.
    missing_symbols: Dict[str, str] = {}
    for sym, ik in symbol_to_key.items():
        s = str(sym or "").strip().upper()
        k = str(ik or "").strip()
        if not s or not k:
            continue
        if s not in cached_stock:
            missing_symbols[s] = k

    need_nifty = (cached_nifty is None)
    if cache_same_day and not missing_symbols and not need_nifty:
        return _DF_PREV_CLOSE_CACHE

    keys = [k for k in missing_symbols.values() if k]
    req = list(dict.fromkeys(keys + ([NIFTY50_INDEX_KEY] if need_nifty else [])))
    if not req:
        _DF_PREV_CLOSE_CACHE = {"trade_date": td, "stock": cached_stock, "nifty": cached_nifty}
        return _DF_PREV_CLOSE_CACHE
    snap = {}
    try:
        snap = upstox.get_market_quote_snapshots_batch(req)
    except Exception as e:
        logger.warning("daily_futures: prev-close batch fetch failed: %s", e)
    stock_pc: Dict[str, Optional[float]] = dict(cached_stock)
    for sym, ik in missing_symbols.items():
        stock_pc[sym] = _prev_close_from_snapshot(snap.get(ik) or {})
    nifty_pc = (
        _prev_close_from_snapshot(snap.get(NIFTY50_INDEX_KEY) or {}) if need_nifty else cached_nifty
    )
    _DF_PREV_CLOSE_CACHE = {"trade_date": td, "stock": stock_pc, "nifty": nifty_pc}
    return _DF_PREV_CLOSE_CACHE


def ist_today() -> date:
    return datetime.now(IST).date()


def _is_holiday_date(d: date) -> bool:
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT 1 FROM holiday WHERE holiday_date = CAST(:d AS DATE) LIMIT 1"),
                {"d": str(d)},
            ).fetchone()
            return row is not None
    except Exception:
        # If holiday table is unavailable, keep weekend checks as the minimum guard.
        return False


def _is_trading_day(d: date) -> bool:
    if d.weekday() >= 5:
        return False
    return not _is_holiday_date(d)


def _prev_trading_day(d: date) -> date:
    x = d - timedelta(days=1)
    for _ in range(15):
        if _is_trading_day(x):
            return x
        x -= timedelta(days=1)
    return d - timedelta(days=1)


def _workspace_trade_date_ist(now: Optional[datetime] = None) -> date:
    """
    Daily Futures display date policy:
    - Trading day before 09:00 IST: show previous trading day
    - Trading day from 09:00 IST onward: show current day
    - Non-trading day (weekend/holiday): show previous trading day
    """
    dt = now or datetime.now(IST)
    if dt.tzinfo is None:
        dt = IST.localize(dt)
    else:
        dt = dt.astimezone(IST)
    d = dt.date()
    if not _is_trading_day(d):
        return _prev_trading_day(d)
    if dt.time() < dt_time(9, 0):
        return _prev_trading_day(d)
    return d


def is_daily_futures_session_open_ist(now: Optional[datetime] = None) -> bool:
    """
    Workspace shows only the current IST calendar session from 09:00 onward.
    Before 09:00 IST all sections are empty (clean slate until the day session starts).
    """
    dt = now or datetime.now(IST)
    if dt.tzinfo is None:
        dt = IST.localize(dt)
    else:
        dt = dt.astimezone(IST)
    return dt.time() >= dt_time(9, 0)


def _empty_daily_futures_workspace(trade_date: date, *, session_before_open: bool) -> Dict[str, Any]:
    msg = (
        "Daily Futures shows only the current IST session from 09:00 onward. "
        "Sections stay empty before 09:00 IST."
    )
    return {
        "trade_date": str(trade_date),
        "session_before_open": session_before_open,
        "session_message": msg if session_before_open else None,
        "picks": [],
        "picks_mixed": [],
        "picks_bearish": [],
        "picks_low_conv_bull": [],
        "picks_low_conv_bear": [],
        "index_bearish_gate": {
            "ok": False,
            "nifty_ltp": None,
            "nifty_open": None,
            "nifty_bearish": None,
            "nifty_bullish": None,
        },
        "picks_diagnostics": {
            "screening_count": 0,
            "hidden_because_bought": 0,
            "hidden_because_sold_today": 0,
        },
        "running": [],
        "closed": [],
        "trade_if_could_have_done": [],
        "summary": {
            "cumulative_pnl_rupees": 0.0,
            "wins": 0,
            "losses": 0,
            "win_rate_pct": None,
        },
    }


def _parse_iso_ist(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    try:
        raw = str(ts).strip()
        if raw.isdigit():
            v = float(raw)
            if v > 1_000_000_000_000:
                v /= 1000.0
            return datetime.fromtimestamp(v, tz=IST)
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            return IST.localize(dt)
        return dt.astimezone(IST)
    except Exception:
        return None


def _fmt_hm(dt: Optional[datetime]) -> str:
    return dt.strftime("%H:%M") if dt else "—"


def _floor_to_15m_ist(dt: datetime) -> datetime:
    """Normalize to scan slot boundary: 11:00, 11:15, 11:30, 11:45, ..."""
    if dt.tzinfo is None:
        dt = IST.localize(dt)
    else:
        dt = dt.astimezone(IST)
    mm = (dt.minute // 15) * 15
    return dt.replace(minute=mm, second=0, microsecond=0)


def _fetch_intraday_1m_cached(upstox: UpstoxService, instrument_key: str, trade_date: date) -> List[Dict[str, Any]]:
    if not instrument_key:
        return []
    ck = (instrument_key.strip(), str(trade_date))
    now = time.time()
    hit = _DF_INTRADAY_1M_CACHE.get(ck)
    if hit and (now - float(hit.get("ts") or 0)) < 180.0:
        return list(hit.get("candles") or [])
    try:
        candles = upstox.get_historical_candles_by_instrument_key(
            instrument_key,
            interval="minutes/1",
            days_back=2,
            range_end_date=trade_date,
        ) or []
    except Exception:
        candles = []
    # Fallback for current-day snapshots: historical endpoint can be empty intraday.
    # Use Upstox intraday-candle endpoint for minute bars when needed.
    if not candles and trade_date == ist_today():
        try:
            key_enc = quote(instrument_key, safe="")
            intraday_url = f"{upstox.base_url}/historical-candle/intraday/{key_enc}/minutes/1"
            raw = upstox.make_api_request(intraday_url, method="GET", timeout=15, max_retries=2) or {}
            if isinstance(raw, dict) and raw.get("status") == "success":
                rows = ((raw.get("data") or {}).get("candles")) or []
                candles = _candles_rows_to_structured(rows) or []
        except Exception:
            candles = candles or []
    _DF_INTRADAY_1M_CACHE[ck] = {"ts": now, "candles": candles}
    return list(candles)


def _ltp_asof_ist(candles: List[Dict[str, Any]], target_dt_ist: datetime) -> Optional[float]:
    best_ts: Optional[datetime] = None
    best_close: Optional[float] = None
    for c in candles or []:
        ts = c.get("timestamp")
        dt = None
        try:
            if isinstance(ts, (int, float)):
                v = float(ts)
                if v > 1_000_000_000_000:
                    v /= 1000.0
                dt = datetime.fromtimestamp(v, tz=IST)
            else:
                dt = _parse_iso_ist(str(ts))
        except Exception:
            dt = None
        if not dt:
            continue
        if dt > target_dt_ist:
            continue
        close_px = c.get("close")
        try:
            cp = float(close_px)
        except (TypeError, ValueError):
            continue
        if cp <= 0:
            continue
        if best_ts is None or dt > best_ts:
            best_ts = dt
            best_close = cp
    return round(float(best_close), 4) if best_close is not None else None


def _second_scan_consecutive_15m_ist(first_hit: datetime, second_hit: datetime) -> bool:
    """
    True if the 2nd screening webhook lands on the next ~15m run after the 1st
    (one cadence, not a later catch-up after a miss).
    """
    a = first_hit.astimezone(IST) if first_hit.tzinfo else IST.localize(first_hit)
    b = second_hit.astimezone(IST) if second_hit.tzinfo else IST.localize(second_hit)
    delta = (b - a).total_seconds()
    if delta <= 0:
        return False
    # One ~15m ChartInk step: 8–32 min slack; wider gaps imply a missed intermediate run
    return 8 * 60 <= delta <= 32 * 60


def _build_trade_if_could_rows(
    picks: List[Dict[str, Any]],
    closed: List[Dict[str, Any]],
    trade_date: date,
) -> List[Dict[str, Any]]:
    closed_sids = {
        int(r.get("screening_id"))
        for r in (closed or [])
        if r.get("screening_id") is not None
    }
    candidates = [
        p for p in (picks or [])
        if p.get("screening_id") is not None and int(p.get("screening_id")) not in closed_sids
    ]
    if not candidates:
        return []

    upstox = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    out: List[Dict[str, Any]] = []
    close_1515 = IST.localize(datetime.combine(trade_date, datetime.min.time()).replace(hour=15, minute=15))
    now_ist = datetime.now(IST)

    for p in candidates:
        first_hit = _parse_iso_ist(p.get("first_hit_at"))
        if not first_hit:
            continue
        last_hit = _parse_iso_ist(p.get("last_hit_at")) or first_hit
        last_hit_slot = _floor_to_15m_ist(last_hit)
        second_hit = _parse_iso_ist(p.get("second_scan_time"))
        # Only symbols with a 2nd scan on the immediate next ~15m run (see webhook cadence).
        if not second_hit:
            continue
        if not _second_scan_consecutive_15m_ist(first_hit, second_hit):
            continue
        entry_dt = second_hit + timedelta(minutes=5)
        ikey = (p.get("instrument_key") or "").strip()
        candles = _fetch_intraday_1m_cached(upstox, ikey, trade_date)
        entry_ltp = _ltp_asof_ist(candles, entry_dt)
        scan_ltp = _ltp_asof_ist(candles, last_hit_slot)
        if scan_ltp is None:
            try:
                pv = p.get("ltp")
                if pv is not None:
                    x = float(pv)
                    if x > 0:
                        scan_ltp = round(x, 4)
            except Exception:
                scan_ltp = None
        qty = p.get("lot_size")
        try:
            qty_num = float(qty) if qty is not None else None
        except Exception:
            qty_num = None

        row: Dict[str, Any] = {
            "screening_id": p.get("screening_id"),
            "underlying": p.get("underlying"),
            "direction_type": str(p.get("direction_type") or "LONG").strip().upper(),
            "future_symbol": p.get("future_symbol"),
            "instrument_key": ikey,
            "qty": int(qty_num) if qty_num is not None else None,
            "first_scan_time": _fmt_hm(first_hit),
            "second_scan_hm": _fmt_hm(second_hit) if second_hit else None,
            "entry_time": _fmt_hm(entry_dt),
            "entry_ltp": entry_ltp,
            "exit_scan_time": _fmt_hm(last_hit_slot),
            "exit_scan_ltp": scan_ltp,
            "current_ltp": None,
            "pnl_scan_rupees": None,
            "exit_1515_time": "15:15",
            "exit_1515_ltp": None,
            "pnl_1515_rupees": None,
        }

        try:
            pv = p.get("ltp")
            if pv is not None:
                cur = float(pv)
                if cur > 0:
                    row["current_ltp"] = round(cur, 4)
        except Exception:
            row["current_ltp"] = None

        pnl_ref_ltp = row.get("current_ltp")
        if pnl_ref_ltp is None:
            pnl_ref_ltp = scan_ltp
        pdir = row.get("direction_type") or "LONG"
        if entry_ltp is not None and pnl_ref_ltp is not None and qty_num is not None:
            if str(pdir).strip().upper() == "SHORT":
                row["pnl_scan_rupees"] = round((entry_ltp - float(pnl_ref_ltp)) * qty_num, 2)
            else:
                row["pnl_scan_rupees"] = round((float(pnl_ref_ltp) - entry_ltp) * qty_num, 2)

        ltp_1515 = _ltp_asof_ist(candles, close_1515)
        if ltp_1515 is None:
            # Avoid blank 15:15 projection when historical candle fetch is incomplete.
            ltp_1515 = row.get("current_ltp") if row.get("current_ltp") is not None else scan_ltp
        row["exit_1515_ltp"] = ltp_1515
        if entry_ltp is not None and ltp_1515 is not None and qty_num is not None:
            if str(pdir).strip().upper() == "SHORT":
                row["pnl_1515_rupees"] = round((entry_ltp - ltp_1515) * qty_num, 2)
            else:
                row["pnl_1515_rupees"] = round((ltp_1515 - entry_ltp) * qty_num, 2)

        out.append(row)

    out.sort(key=lambda r: (r.get("future_symbol") or r.get("underlying") or ""))
    return out


def normalize_symbols_from_payload(payload: Any) -> List[str]:
    out: List[str] = []
    if payload is None:
        return out
    if isinstance(payload, str):
        return sorted({s.strip().upper() for s in re.split(r"[\s,;\n]+", payload) if s.strip()})
    if isinstance(payload, list):
        return sorted({str(x).strip().upper() for x in payload if str(x).strip()})
    if isinstance(payload, dict):
        for key in ("symbols", "symbol", "stocks", "tickers", "data", "alert_symbols"):
            v = payload.get(key)
            if isinstance(v, list):
                out.extend(str(x).strip().upper() for x in v if x)
            elif isinstance(v, str):
                out.extend(normalize_symbols_from_payload(v))
        if not out and payload.get("text"):
            out.extend(normalize_symbols_from_payload(str(payload["text"])))
    return sorted(set(out))


def load_arbitrage_future_row(conn, underlying: str) -> Optional[Dict[str, Any]]:
    """
    Resolve the tradeable FUT for Daily Futures: ``arbitrage_master.currmth_future_*``
    (serial front contract as maintained by the arbitrage daily setup job; roll window may shift
    which underlying expiry is stored there).
    """
    row = conn.execute(
        text(
            """
            SELECT stock,
                   currmth_future_symbol,
                   currmth_future_instrument_key
            FROM arbitrage_master
            WHERE UPPER(TRIM(stock)) = :u
              AND currmth_future_instrument_key IS NOT NULL
              AND TRIM(currmth_future_instrument_key) <> ''
            LIMIT 1
            """
        ),
        {"u": underlying.strip().upper()},
    ).fetchone()
    if not row:
        return None
    return {
        "underlying": row[0],
        "future_symbol": row[1],
        "instrument_key": str(row[2]).strip(),
    }


def retarget_daily_futures_to_next_month_for_date(
    trade_date: date, underlying: Optional[str] = None
) -> Dict[str, Any]:
    """
    Re-point that day's screening rows and open user trades to **current** FUT
    (``arbitrage_master.currmth_*``) via :func:`load_arbitrage_future_row`.

    Kept for one-off admin sync; name is historical. Does not alter historical report rows
    by itself—only updates ``daily_futures_*`` for the given ``trade_date`` when run.

    If ``underlying`` is set (e.g. ``ADANIPORTS``), only that symbol's screening row and its
    open bought trades are updated—useful when one pick was on the wrong FUT (e.g. nxtmth).
    """
    ensure_daily_futures_tables()
    und_filter: Optional[str] = str(underlying).strip().upper() if (underlying and str(underlying).strip()) else None
    out: Dict[str, Any] = {
        "trade_date": str(trade_date),
        "underlying": und_filter,
        "screening_updated": 0,
        "trades_synced": 0,
        "skipped": [],
    }
    screening_n = 0
    with engine.begin() as conn:
        if und_filter:
            srows = conn.execute(
                text(
                    """
                SELECT id, UPPER(TRIM(underlying)) AS u
                FROM daily_futures_screening
                WHERE trade_date = CAST(:d AS DATE)
                  AND UPPER(TRIM(underlying)) = :u
                """
                ),
                {"d": str(trade_date), "u": und_filter},
            ).fetchall()
        else:
            srows = conn.execute(
                text(
                    """
                SELECT id, UPPER(TRIM(underlying)) AS u
                FROM daily_futures_screening
                WHERE trade_date = CAST(:d AS DATE)
                """
                ),
                {"d": str(trade_date)},
            ).fetchall()
        for rid, u in srows:
            u = str(u or "").strip().upper()
            if not u:
                continue
            row = load_arbitrage_future_row(conn, u)
            if not row:
                out["skipped"].append({"underlying": u, "reason": "no_currmth_in_arbitrage_master"})
                continue
            lot = fut_lot_for_key(str(row["instrument_key"]))
            conn.execute(
                text(
                    """
                    UPDATE daily_futures_screening SET
                      future_symbol = :fs,
                      instrument_key = :ik,
                      lot_size = :ls,
                      updated_at = CURRENT_TIMESTAMP
                    WHERE id = :id
                    """
                ),
                {
                    "id": int(rid),
                    "fs": row["future_symbol"],
                    "ik": row["instrument_key"],
                    "ls": lot,
                },
            )
            screening_n += 1

        if und_filter:
            r2 = conn.execute(
                text(
                    """
                UPDATE daily_futures_user_trade t SET
                  direction_type = s.direction_type,
                  future_symbol = s.future_symbol,
                  instrument_key = s.instrument_key,
                  lot_size = s.lot_size,
                  updated_at = CURRENT_TIMESTAMP
                FROM daily_futures_screening s
                WHERE t.screening_id = s.id
                  AND s.trade_date = CAST(:d AS DATE)
                  AND t.order_status = 'bought'
                  AND UPPER(TRIM(s.underlying)) = :u
                """
                ),
                {"d": str(trade_date), "u": und_filter},
            )
        else:
            r2 = conn.execute(
                text(
                    """
                UPDATE daily_futures_user_trade t SET
                  direction_type = s.direction_type,
                  future_symbol = s.future_symbol,
                  instrument_key = s.instrument_key,
                  lot_size = s.lot_size,
                  updated_at = CURRENT_TIMESTAMP
                FROM daily_futures_screening s
                WHERE t.screening_id = s.id
                  AND s.trade_date = CAST(:d AS DATE)
                  AND t.order_status = 'bought'
                """
                ),
                {"d": str(trade_date)},
            )
        try:
            out["trades_synced"] = int(r2.rowcount or 0)
        except (TypeError, ValueError):
            out["trades_synced"] = 0
    out["screening_updated"] = screening_n
    return out


def _recompute_conviction_all_today(upstox: UpstoxService, trade_date: date) -> None:
    """Recompute conviction_score + LTP for every screening row on trade_date."""
    cand_cache: Dict[str, Dict[Tuple[int, int], Dict[str, Any]]] = {}

    with engine.connect() as conn:
        rows_db = conn.execute(
            text(
                """
                SELECT id, underlying, instrument_key FROM daily_futures_screening
                WHERE trade_date = CAST(:d AS DATE)
                """
            ),
            {"d": str(trade_date)},
        ).fetchall()

    finals: List[Dict[str, Any]] = []
    row_ids: List[int] = []

    for rid, und, ik in rows_db:
        ik = str(ik).strip()
        if ik not in cand_cache:
            cand_cache[ik] = fetch_intraday_1m_candles(upstox, ik, trade_date) or {}
        buckets = _bucket_candles_by_hhmm(cand_cache[ik])
        # Fallback: if today's 1m candles are unavailable, use previous trading day candles
        # from broker API so conviction can still be computed.
        if not buckets:
            try:
                prev_dt = upstox.get_last_trading_date(datetime.combine(trade_date, dt_time(12, 0)))
                prev_date = prev_dt.date() if prev_dt else None
                if prev_date and prev_date != trade_date:
                    if ik not in cand_cache:
                        cand_cache[ik] = fetch_intraday_1m_candles(upstox, ik, prev_date) or {}
                    else:
                        cand_cache[ik] = fetch_intraday_1m_candles(upstox, ik, prev_date) or cand_cache[ik]
                    buckets = _bucket_candles_by_hhmm(cand_cache[ik])
            except Exception:
                pass
        row_ids.append(int(rid))
        if not buckets:
            finals.append(
                {
                    "trade_date": str(trade_date),
                    "symbol": str(und),
                    "underlying": str(und),
                    "instrument_key": ik,
                    "conviction_oi_change_pct": None,
                    "conviction_price_vs_vwap_pct": None,
                    "conviction_score": 50.0,
                    "conviction_score_breakdown": {"oi": 25.0, "vwap": 25.0},
                    "_rid": rid,
                }
            )
            continue
        keys = sorted(buckets.keys())
        tr = TradeRow(trade_date=str(trade_date), symbol=str(und))
        _fill_conviction_raw_metrics(tr, buckets, [keys[-1]], conviction_scan_index=0)
        d = tr.to_dict()
        d["trade_date"] = str(trade_date)
        d["underlying"] = str(und)
        d["instrument_key"] = ik
        d["_rid"] = rid
        finals.append(d)

    finalize_conviction_scores(finals)

    with engine.begin() as conn:
        for d in finals:
            rid = int(d["_rid"])
            cs = d.get("conviction_score")
            if cs is None:
                cs = 50.0
            ik = str(d.get("instrument_key") or "").strip()
            q = upstox.get_market_quote_by_key(ik) if ik else {}
            lp = (q or {}).get("last_price") or (q or {}).get("close")
            try:
                ltp = float(lp) if lp is not None else None
            except (TypeError, ValueError):
                ltp = None
            conn.execute(
                text(
                    """
                    UPDATE daily_futures_screening SET
                      conviction_score = :cs,
                      ltp = COALESCE(:ltp, ltp),
                      updated_at = CURRENT_TIMESTAMP
                    WHERE id = :id
                    """
                ),
                {"cs": cs, "ltp": ltp, "id": rid},
            )
            br = d.get("conviction_score_breakdown") or {}
            logger.info(
                "daily_futures conviction: symbol=%s score=%s oi=%s vwap=%s",
                d.get("underlying") or d.get("symbol"),
                cs,
                br.get("oi"),
                br.get("vwap"),
            )


def _ingest_df_webhook(symbols: List[str], direction: str) -> Dict[str, Any]:
    ensure_daily_futures_tables()
    dir_key = str(direction or "LONG").strip().upper()
    if dir_key not in ("LONG", "SHORT"):
        dir_key = "LONG"
    trade_date = ist_today()
    sym_set: Set[str] = {s.strip().upper() for s in symbols if s and str(s).strip()}
    logger.info(
        "daily_futures webhook ingest: direction=%s unique_symbols=%d trade_date=%s",
        dir_key,
        len(sym_set),
        trade_date,
    )

    upstox = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    summary: Dict[str, Any] = {"trade_date": str(trade_date), "processed": 0, "skipped": []}

    touched_ids: List[int] = []
    symbol_rows: Dict[str, Dict[str, Any]] = {}
    for u in sorted(sym_set):
        with engine.connect() as conn:
            row = load_arbitrage_future_row(conn, u)
        if row:
            symbol_rows[u] = row
        else:
            summary["skipped"].append({"underlying": u, "reason": "not_in_arbitrage_master"})

    # Batch snapshot fetch for all futures + Nifty benchmark.
    symbol_to_key = {u: str(r["instrument_key"]).strip() for u, r in symbol_rows.items()}
    quote_keys = list(dict.fromkeys([k for k in symbol_to_key.values() if k] + [NIFTY50_INDEX_KEY]))
    batch_quotes: Dict[str, Dict[str, Any]] = {}
    try:
        batch_quotes = upstox.get_market_quote_snapshots_batch(quote_keys)
    except Exception as e:
        logger.warning("daily_futures: ingestion batch quote fetch failed: %s", e)
    prev_close_cache = _get_or_init_prev_close_cache(trade_date, upstox, symbol_to_key)
    nifty_quote = batch_quotes.get(NIFTY50_INDEX_KEY) or {}
    nifty_ltp = _safe_float(nifty_quote.get("last_price"))
    nifty_vwap = _session_vwap_for_conviction(nifty_quote)
    nifty_prev_close = _safe_float((prev_close_cache.get("nifty")))
    nifty_change_pct = (
        round(((nifty_ltp - nifty_prev_close) / nifty_prev_close) * 100.0, 6)
        if nifty_ltp is not None and nifty_prev_close and nifty_prev_close > 0
        else None
    )

    ingest_now = datetime.now(IST)
    nifty_15m_bars = _last_completed_15m_candles_for_instrument(
        upstox, NIFTY50_INDEX_KEY, trade_date, ingest_now
    )
    nifty15_for_bear = _safe_float((nifty_15m_bars or [])[-1].get("close")) if len(nifty_15m_bars or []) else None
    nifty15_bear_ok = (
        (nifty15_for_bear is not None and nifty_vwap is not None and float(nifty15_for_bear) < float(nifty_vwap))
        if (nifty15_for_bear is not None and nifty_vwap is not None)
        else (nifty_ltp is not None and nifty_vwap is not None and float(nifty_ltp) < float(nifty_vwap))
    )

    scan_inputs: List[Dict[str, Any]] = []

    with engine.begin() as conn:
        for u, row in sorted(symbol_rows.items(), key=lambda x: x[0]):
            ik = row["instrument_key"]
            lot = fut_lot_for_key(ik)
            q = batch_quotes.get(ik) or {}
            ltp = _safe_float(q.get("last_price"))
            session_vwap = _session_vwap_for_conviction(q)
            total_oi = _safe_float(q.get("oi"))
            prev_close = _safe_float((prev_close_cache.get("stock") or {}).get(u))
            stock_change_pct = (
                round(((ltp - prev_close) / prev_close) * 100.0, 6)
                if ltp is not None and prev_close and prev_close > 0
                else None
            )
            if not q:
                logger.warning("daily_futures: quote snapshot missing for %s (%s)", u, ik)
            last5_15m = _last_completed_15m_candles_for_instrument(upstox, ik, trade_date, ingest_now)
            candle_is_green = False
            candle_higher_high = False
            candle_higher_low = False
            if len(last5_15m) >= 2:
                c_prev = last5_15m[-2]
                c_last = last5_15m[-1]
                if dir_key == "LONG":
                    candle_is_green = bool(float(c_last["close"]) > float(c_last["open"]))
                    candle_higher_high = bool(float(c_last["high"]) > float(c_prev["high"]))
                    candle_higher_low = bool(float(c_last["low"]) > float(c_prev["low"]))
                else:
                    is_red = bool(float(c_last["close"]) < float(c_last["open"]))
                    lo = bool(float(c_last["low"]) < float(c_prev["low"]))
                    hi = bool(float(c_last["high"]) < float(c_prev["high"]))
                    candle_is_green = not is_red
                    candle_higher_high = hi
                    candle_higher_low = lo
            stock15_close = _safe_float(last5_15m[-1].get("close")) if len(last5_15m) else None
            stock15_bear_ok = (
                stock15_close is not None
                and session_vwap is not None
                and float(stock15_close) < float(session_vwap)
            )
            candle_is_red = False
            if dir_key == "SHORT" and len(last5_15m) >= 1:
                cl0 = last5_15m[-1]
                candle_is_red = bool(float(cl0["close"]) < float(cl0["open"]))
            ex = conn.execute(
                text(
                    """
                    SELECT id, scan_count, total_oi, second_scan_time,
                           second_scan_conviction_score, second_scan_stock_change_pct, second_scan_nifty_change_pct,
                           candle_is_green, candle_higher_high, candle_higher_low
                    FROM daily_futures_screening
                    WHERE trade_date = CAST(:d AS DATE) AND UPPER(TRIM(underlying)) = :u
                      AND UPPER(TRIM(COALESCE(direction_type, 'LONG'))) = :dfdir
                    """
                ),
                {"d": str(trade_date), "u": u, "dfdir": dir_key},
            ).fetchone()
            prev_oi = _safe_float(ex[2]) if ex else None
            oi_change_pct: Optional[float] = None
            if total_oi is not None and prev_oi is not None:
                if prev_oi > 0:
                    oi_change_pct = round(((total_oi - prev_oi) / prev_oi) * 100.0, 6)
                elif float(total_oi) > 0 and float(prev_oi) == 0:
                    oi_change_pct = 100.0
                else:
                    oi_change_pct = 0.0
            scan_inputs.append(
                {
                    "underlying": u,
                    "instrument_key": ik,
                    "ltp": ltp,
                    "session_vwap": session_vwap,
                    "total_oi": int(total_oi) if total_oi is not None else None,
                    "change_in_oi": (int(q["change_in_oi"]) if q.get("change_in_oi", None) is not None else None),
                    "oi_change_pct": oi_change_pct,
                    "nifty_ltp": nifty_ltp,
                    "nifty_session_vwap": nifty_vwap,
                    "stock_prev_close": prev_close,
                    "nifty_prev_close": nifty_prev_close,
                    "stock_change_pct": stock_change_pct,
                    "nifty_change_pct": nifty_change_pct,
                    "candle_is_green": candle_is_green,
                    "candle_higher_high": candle_higher_high,
                    "candle_higher_low": candle_higher_low,
                    "lot_size": lot,
                    "future_symbol": row["future_symbol"],
                    "existing": ex,
                    "nifty15_bear_ok": bool(nifty15_bear_ok),
                    "stock15_bear_ok": bool(stock15_bear_ok),
                    "candle_is_red": bool(candle_is_red),
                    "dir_key": dir_key,
                }
            )
        # OI-leg ranking within this batch: prefer % vs previous scan, else Upstox change_in_oi, else total_oi.
        def _oi_rank_value(x: Dict[str, Any]) -> float:
            p = x.get("oi_change_pct")
            if isinstance(p, (int, float)):
                return float(p)
            c = x.get("change_in_oi")
            if isinstance(c, (int, float)):
                return float(c)
            toi = x.get("total_oi")
            if isinstance(toi, int) and toi > 0:
                return float(toi) / 1_000_000.0
            return -1.0e18

        ranked = sorted(scan_inputs, key=_oi_rank_value, reverse=True)
        rank_map = {id(x): idx for idx, x in enumerate(ranked)}
        n_rank = len(ranked)

        for x in scan_inputs:
            pvp = (
                ((x["ltp"] - x["session_vwap"]) / x["session_vwap"] * 100.0)
                if x.get("ltp") is not None and x.get("session_vwap") not in (None, 0)
                else None
            )
            xdir = str(x.get("dir_key") or "LONG").strip().upper()
            if xdir == "SHORT":
                vw, vw_reason = _vwap_leg_score_reason_bearish(
                    pvp,
                    bool(x.get("nifty15_bear_ok")),
                    bool(x.get("stock15_bear_ok")),
                    candle_is_red=bool(x.get("candle_is_red")),
                    candle_lower_low=bool(x.get("candle_higher_low")),
                    candle_lower_high=bool(x.get("candle_higher_high")),
                )
            else:
                vw, vw_reason = _vwap_leg_score_reason(
                    pvp,
                    candle_is_green=bool(x.get("candle_is_green")),
                    candle_higher_high=bool(x.get("candle_higher_high")),
                    candle_higher_low=bool(x.get("candle_higher_low")),
                )
            if id(x) in rank_map and n_rank > 1:
                oi_leg = (n_rank - 1 - rank_map[id(x)]) / (n_rank - 1) * 50.0
            elif id(x) in rank_map and n_rank == 1:
                oi_leg = 50.0
            else:
                oi_leg = 25.0
            conviction = round(max(0.0, min(100.0, oi_leg + vw)), 1)
            x["conviction_oi_leg"] = round(oi_leg, 1)
            x["conviction_vwap_leg"] = round(vw, 1)
            x["conviction_score"] = conviction
            cj: Dict[str, Any] = {
                "oi_leg": round(oi_leg, 1),
                "vwap_leg": round(vw, 1),
                "vwap_leg_reason": vw_reason,
                "price_vs_vwap_pct": round(float(pvp), 6) if pvp is not None else None,
                "candle_is_green": bool(x.get("candle_is_green")),
                "candle_higher_high": bool(x.get("candle_higher_high")),
                "candle_higher_low": bool(x.get("candle_higher_low")),
                "ltp": float(x["ltp"]) if x.get("ltp") is not None else None,
                "session_vwap": float(x["session_vwap"]) if x.get("session_vwap") is not None else None,
                "timestamp": ingest_now.isoformat(),
                "direction": xdir,
            }
            if xdir == "SHORT":
                cj["nifty15_bear_ok"] = bool(x.get("nifty15_bear_ok"))
                cj["stock15_bear_ok"] = bool(x.get("stock15_bear_ok"))
            x["conviction_breakdown_json"] = cj

        for x in scan_inputs:
            u = x["underlying"]
            ex = x["existing"]
            if ex:
                sid = int(ex[0])
                prior_scan_count = int(ex[1] or 0)
                second_scan_time = ex[3]
                second_scan_conv = ex[4]
                second_scan_stock = ex[5]
                second_scan_nifty = ex[6]
                row_candle_is_green = bool(ex[7]) if ex[7] is not None else bool(x.get("candle_is_green"))
                row_candle_higher_high = bool(ex[8]) if ex[8] is not None else bool(x.get("candle_higher_high"))
                row_candle_higher_low = bool(ex[9]) if ex[9] is not None else bool(x.get("candle_higher_low"))
                next_scan_count = prior_scan_count + 1
                if prior_scan_count < 1:
                    next_scan_count = 1
                if next_scan_count >= 2 and second_scan_time is None:
                    second_scan_time = ingest_now
                    second_scan_conv = x["conviction_score"]
                    second_scan_stock = x["stock_change_pct"]
                    second_scan_nifty = x["nifty_change_pct"]
                conn.execute(
                    text(
                        """
                        UPDATE daily_futures_screening SET
                          scan_count = scan_count + 1,
                          last_hit_at = :lh,
                          direction_type = :dtd,
                          lot_size = COALESCE(:lot, lot_size),
                          future_symbol = :fs,
                          instrument_key = :ik,
                          conviction_score = :cs,
                          conviction_oi_leg = :oi_leg,
                          conviction_vwap_leg = :vw_leg,
                          ltp = :ltp,
                          session_vwap = :svwap,
                          total_oi = :toi,
                          oi_change_pct = :oi_chg,
                          nifty_ltp = :nltp,
                          nifty_session_vwap = :nsvwap,
                          stock_prev_close = :spc,
                          nifty_prev_close = :npc,
                          stock_change_pct = :scp,
                          nifty_change_pct = :ncp,
                          snapshotted_at = :snap,
                          second_scan_time = :sst,
                          second_scan_conviction_score = :ssc,
                          second_scan_stock_change_pct = :ss_stock,
                          second_scan_nifty_change_pct = :ss_nifty,
                          candle_is_green = :cig,
                          candle_higher_high = :chh,
                          candle_higher_low = :chl,
                          conviction_breakdown_json = CAST(:cbj AS JSONB),
                          updated_at = CURRENT_TIMESTAMP
                        WHERE id = :id
                        """
                    ),
                    {
                        "id": sid,
                        "lh": ingest_now,
                        "dtd": str(x.get("dir_key") or dir_key),
                        "lot": x["lot_size"],
                        "fs": x["future_symbol"],
                        "ik": x["instrument_key"],
                        "cs": x["conviction_score"],
                        "oi_leg": x["conviction_oi_leg"],
                        "vw_leg": x["conviction_vwap_leg"],
                        "ltp": x["ltp"],
                        "svwap": x["session_vwap"],
                        "toi": x["total_oi"],
                        "oi_chg": x["oi_change_pct"],
                        "nltp": x["nifty_ltp"],
                        "nsvwap": x["nifty_session_vwap"],
                        "spc": x["stock_prev_close"],
                        "npc": x["nifty_prev_close"],
                        "scp": x["stock_change_pct"],
                        "ncp": x["nifty_change_pct"],
                        "snap": ingest_now,
                        "sst": second_scan_time,
                        "ssc": second_scan_conv,
                        "ss_stock": second_scan_stock,
                        "ss_nifty": second_scan_nifty,
                        "cig": row_candle_is_green,
                        "chh": row_candle_higher_high,
                        "chl": row_candle_higher_low,
                        "cbj": json.dumps(x.get("conviction_breakdown_json") or {}),
                    },
                )
                touched_ids.append(sid)
            else:
                r = conn.execute(
                    text(
                        """
                        INSERT INTO daily_futures_screening (
                          trade_date, underlying, direction_type, future_symbol, instrument_key, lot_size,
                          scan_count, first_hit_at, last_hit_at, conviction_score,
                          conviction_oi_leg, conviction_vwap_leg, ltp, session_vwap, total_oi, oi_change_pct,
                          nifty_ltp, nifty_session_vwap, stock_prev_close, nifty_prev_close,
                          stock_change_pct, nifty_change_pct, snapshotted_at,
                          candle_is_green, candle_higher_high, candle_higher_low, conviction_breakdown_json
                        ) VALUES (
                          CAST(:d AS DATE), :u, :dtd, :fs, :ik, :lot, 1, :fh, :lh, :cs,
                          :oi_leg, :vw_leg, :ltp, :svwap, :toi, :oi_chg,
                          :nltp, :nsvwap, :spc, :npc, :scp, :ncp, :snap,
                          :cig, :chh, :chl, CAST(:cbj AS JSONB)
                        )
                        RETURNING id
                        """
                    ),
                    {
                        "d": str(trade_date),
                        "u": u,
                        "dtd": str(x.get("dir_key") or dir_key),
                        "fs": x["future_symbol"],
                        "ik": x["instrument_key"],
                        "lot": x["lot_size"],
                        "fh": ingest_now,
                        "lh": ingest_now,
                        "cs": x["conviction_score"],
                        "oi_leg": x["conviction_oi_leg"],
                        "vw_leg": x["conviction_vwap_leg"],
                        "ltp": x["ltp"],
                        "svwap": x["session_vwap"],
                        "toi": x["total_oi"],
                        "oi_chg": x["oi_change_pct"],
                        "nltp": x["nifty_ltp"],
                        "nsvwap": x["nifty_session_vwap"],
                        "spc": x["stock_prev_close"],
                        "npc": x["nifty_prev_close"],
                        "scp": x["stock_change_pct"],
                        "ncp": x["nifty_change_pct"],
                        "snap": ingest_now,
                        "cig": bool(x.get("candle_is_green")),
                        "chh": bool(x.get("candle_higher_high")),
                        "chl": bool(x.get("candle_higher_low")),
                        "cbj": json.dumps(x.get("conviction_breakdown_json") or {}),
                    },
                ).fetchone()
                touched_ids.append(int(r[0]))
            summary["processed"] += 1

        miss_rows = conn.execute(
            text(
                """
                SELECT id, UPPER(TRIM(underlying)) FROM daily_futures_user_trade
                WHERE order_status = 'bought'
                """
            ),
        ).fetchall()

        present = set(sym_set)
        for mid, und in miss_rows:
            und = (und or "").strip().upper()
            if und in present:
                conn.execute(
                    text(
                        """
                        UPDATE daily_futures_user_trade SET
                          consecutive_webhook_misses = 0,
                          updated_at = CURRENT_TIMESTAMP
                        WHERE id = :id
                        """
                    ),
                    {"id": mid},
                )
            else:
                conn.execute(
                    text(
                        """
                        UPDATE daily_futures_user_trade SET
                          consecutive_webhook_misses = consecutive_webhook_misses + 1,
                          updated_at = CURRENT_TIMESTAMP
                        WHERE id = :id
                        """
                    ),
                    {"id": mid},
                )

    summary["touched_screening_ids"] = touched_ids
    return summary


def process_chartink_webhook(symbols: List[str]) -> Dict[str, Any]:
    """Bullish ChartInk screener → direction LONG."""
    return _ingest_df_webhook(symbols, "LONG")


def process_chartink_webhook_bearish(symbols: List[str]) -> Dict[str, Any]:
    """Bearish ChartInk screener → direction SHORT."""
    return _ingest_df_webhook(symbols, "SHORT")


def _fetch_screening_dicts(conn: Any, trade_date: date) -> List[Dict[str, Any]]:
    res = conn.execute(
        text(
            """
            SELECT id, underlying, direction_type, future_symbol, instrument_key, lot_size,
                   scan_count, first_hit_at, last_hit_at, conviction_score, ltp,
                   second_scan_time, second_scan_conviction_score, second_scan_stock_change_pct, second_scan_nifty_change_pct,
                   stock_change_pct, nifty_change_pct,
                   candle_is_green, candle_higher_high, candle_higher_low, conviction_breakdown_json
            FROM daily_futures_screening
            WHERE trade_date = CAST(:d AS DATE)
            ORDER BY conviction_score DESC NULLS LAST, underlying
            """
        ),
        {"d": str(trade_date)},
    ).fetchall()
    out = []
    for row in res:
        out.append(
            {
                "screening_id": row[0],
                "underlying": row[1],
                "direction_type": str(row[2] or "LONG").strip().upper(),
                "future_symbol": row[3],
                "instrument_key": row[4],
                "lot_size": int(row[5]) if row[5] is not None else None,
                "scan_count": int(row[6] or 0),
                "first_hit_at": row[7].isoformat() if row[7] else None,
                "last_hit_at": row[8].isoformat() if row[8] else None,
                "conviction_score": float(row[9]) if row[9] is not None else None,
                "ltp": float(row[10]) if row[10] is not None else None,
                "second_scan_time": row[11].isoformat() if row[11] else None,
                "second_scan_conviction_score": float(row[12]) if row[12] is not None else None,
                "second_scan_stock_change_pct": float(row[13]) if row[13] is not None else None,
                "second_scan_nifty_change_pct": float(row[14]) if row[14] is not None else None,
                "stock_change_pct": float(row[15]) if row[15] is not None else None,
                "nifty_change_pct": float(row[16]) if row[16] is not None else None,
                "candle_is_green": bool(row[17]) if row[17] is not None else None,
                "candle_higher_high": bool(row[18]) if row[18] is not None else None,
                "candle_higher_low": bool(row[19]) if row[19] is not None else None,
                "conviction_breakdown_json": row[20] if row[20] is not None else None,
            }
        )
    return out


def _apply_live_ltps_to_picks_and_running(
    picks: List[Dict[str, Any]],
    running: List[Dict[str, Any]],
    closed: Optional[List[Dict[str, Any]]] = None,
) -> None:
    """
    Refresh LTP from Upstox for every row (batch + per-key fallback), update dicts in place,
    and persist ltp on daily_futures_screening so 15-min webhook runs and page loads stay aligned.
    """
    # Prioritize running rows first so LTP for open positions is refreshed even
    # when fallback budget is exhausted.
    combined: List[Dict[str, Any]] = list(running) + list(picks) + list(closed or [])
    uniq_keys: List[str] = []
    seen: Set[str] = set()
    for r in combined:
        ik = (r.get("instrument_key") or "").strip()
        if ik and ik not in seen:
            seen.add(ik)
            uniq_keys.append(ik)
    if not uniq_keys:
        return

    upstox = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    batch: Dict[str, float] = {}
    try:
        batch = upstox.get_market_quotes_batch_by_keys(uniq_keys)
    except Exception as e:
        logger.warning("daily_futures: batch LTP failed: %s", e)

    def _norm(k: str) -> str:
        return k.replace(":", "|").replace(" ", "").upper()

    # Keep workspace responsive when broker quote API is degraded.
    # Batch is preferred; single-key fallback is capped.
    single_fallback_budget = max(8, min(30, len(running) * 3))
    single_fallback_used = 0

    def _resolve_ltp(ik: str) -> Optional[float]:
        nonlocal single_fallback_used
        if not ik:
            return None
        if ik in batch:
            lp = batch[ik]
            if lp and float(lp) > 0:
                return float(lp)
        nk = _norm(ik)
        for bk, lp in batch.items():
            if _norm(bk) == nk and lp and float(lp) > 0:
                return float(lp)
        if single_fallback_used >= single_fallback_budget:
            return None
        try:
            single_fallback_used += 1
            q = upstox.get_market_quote_by_key(ik)
            if q and q.get("last_price"):
                v = float(q["last_price"])
                return v if v > 0 else None
        except Exception as ex:
            logger.debug("daily_futures: single-quote LTP failed for %s: %s", ik, ex)
        return None

    by_screening: Dict[int, float] = {}
    for r in combined:
        ik = (r.get("instrument_key") or "").strip()
        lp = _resolve_ltp(ik)
        if lp is None:
            continue
        lp_r = round(lp, 4)
        r["ltp"] = lp_r
        sid = r.get("screening_id")
        if sid is not None:
            by_screening[int(sid)] = lp_r

    if not by_screening:
        return
    try:
        with engine.begin() as conn:
            for sid, ltp in by_screening.items():
                conn.execute(
                    text(
                        """
                        UPDATE daily_futures_screening SET
                          ltp = :ltp,
                          updated_at = CURRENT_TIMESTAMP
                        WHERE id = :id
                        """
                    ),
                    {"ltp": ltp, "id": sid},
                )
    except Exception as e:
        logger.warning("daily_futures: persist LTP to screening failed: %s", e)


def _apply_live_rel_strength_to_picks_and_running(
    picks: List[Dict[str, Any]],
    running: List[Dict[str, Any]],
    trade_date: date,
) -> None:
    """
    Refresh stock_change_pct and nifty_change_pct for the Rel. str. column (FUT % vs Nifty 50 %).
    The webhook path already writes these; workspace polls must recompute or the UI stays em dash.
    """
    combined = list(picks) + list(running)
    if not combined:
        return
    symbol_to_key: Dict[str, str] = {}
    for r in combined:
        u = str(r.get("underlying") or "").strip().upper()
        ik = str(r.get("instrument_key") or "").strip()
        if u and ik and u not in symbol_to_key:
            symbol_to_key[u] = ik
    if not symbol_to_key:
        return
    try:
        upstox = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    except Exception as e:
        logger.warning("daily_futures: rel-strength Upstox init failed: %s", e)
        return
    now_ist = datetime.now(IST)
    prev15_cache: Dict[str, Optional[float]] = {}

    all_keys: List[str] = list(dict.fromkeys(list(symbol_to_key.values()) + [NIFTY50_INDEX_KEY]))
    ltp_by_key: Dict[str, float] = {}
    snap_by_key: Dict[str, Dict[str, Any]] = {}
    try:
        ltp_by_key = upstox.get_market_quotes_batch_by_keys(all_keys) or {}
    except Exception as e:
        logger.warning("daily_futures: rel-strength batch LTP failed: %s", e)
    try:
        snap_by_key = upstox.get_market_quote_snapshots_batch(all_keys) or {}
    except Exception as e:
        logger.warning("daily_futures: rel-strength batch snapshots failed: %s", e)

    def _ltp_for_instrument(ik: str) -> Optional[float]:
        if not ik or not ltp_by_key:
            return None
        if ik in ltp_by_key:
            v = ltp_by_key[ik]
            if v and float(v) > 0:
                return float(v)
        nk = ik.replace(":", "|").replace(" ", "").upper()
        for bk, lp in ltp_by_key.items():
            if not bk or not lp:
                continue
            if bk.replace(":", "|").replace(" ", "").upper() == nk and float(lp) > 0:
                return float(lp)
        return None

    nifty_ltp = _ltp_for_instrument(NIFTY50_INDEX_KEY)
    if nifty_ltp is None:
        try:
            q = upstox.get_market_quote_by_key(NIFTY50_INDEX_KEY) or {}
            lp = _safe_float(q.get("last_price"))
            if lp and lp > 0:
                nifty_ltp = lp
        except Exception as e:
            logger.debug("daily_futures: rel-strength Nifty single quote failed: %s", e)

    nifty_prev = _prev_15m_close_for_instrument(upstox, NIFTY50_INDEX_KEY, now_ist, prev15_cache)
    if nifty_prev is None:
        nifty_prev = _prev_close_from_snapshot(snap_by_key.get(NIFTY50_INDEX_KEY) or {})
    if nifty_prev is None:
        # Final fallback: use session open so rel-strength is still available intraday.
        nifty_prev = _quote_session_open_from_snapshot(snap_by_key.get(NIFTY50_INDEX_KEY) or {})
    nifty_change_pct: Optional[float] = None
    if nifty_ltp is not None and nifty_prev and nifty_prev > 0:
        nifty_change_pct = round(((nifty_ltp - nifty_prev) / nifty_prev) * 100.0, 6)

    by_screening: Dict[int, Tuple[Optional[float], Optional[float]]] = {}
    for r in combined:
        u = str(r.get("underlying") or "").strip().upper()
        if not u:
            continue
        ik = str(r.get("instrument_key") or "").strip()
        stock_prev = _prev_15m_close_for_instrument(upstox, ik, now_ist, prev15_cache)
        if stock_prev is None:
            stock_prev = _prev_close_from_snapshot(snap_by_key.get(ik) or {})
        if stock_prev is None:
            # Final fallback: use session open when previous close is unavailable.
            stock_prev = _quote_session_open_from_snapshot(snap_by_key.get(ik) or {})
        stock_ltp = _safe_float(r.get("ltp"))
        if stock_ltp is None:
            stock_ltp = _ltp_for_instrument(ik)
        stock_change_pct: Optional[float] = None
        if stock_ltp is not None and stock_prev and stock_prev > 0:
            stock_change_pct = round(((stock_ltp - stock_prev) / stock_prev) * 100.0, 6)
        # Never blank out existing values in-memory when live recompute is unavailable.
        if stock_change_pct is not None:
            r["stock_change_pct"] = stock_change_pct
        if nifty_change_pct is not None:
            r["nifty_change_pct"] = nifty_change_pct
        sc_use = _safe_float(r.get("stock_change_pct"))
        nc_use = _safe_float(r.get("nifty_change_pct"))
        r["relative_strength_vs_nifty"] = (
            round(float(sc_use) - float(nc_use), 6)
            if sc_use is not None and nc_use is not None
            else None
        )
        sid = r.get("screening_id")
        if sid is not None and (stock_change_pct is not None or nifty_change_pct is not None):
            by_screening[int(sid)] = (stock_change_pct, nifty_change_pct)
    if not by_screening:
        return
    try:
        with engine.begin() as conn:
            for sid, (scp, ncp) in by_screening.items():
                conn.execute(
                    text(
                        """
                        UPDATE daily_futures_screening SET
                          stock_change_pct = COALESCE(CAST(:scp AS NUMERIC), stock_change_pct),
                          nifty_change_pct = COALESCE(CAST(:ncp AS NUMERIC), nifty_change_pct),
                          updated_at = CURRENT_TIMESTAMP
                        WHERE id = :id
                        """
                    ),
                    {"id": sid, "scp": scp, "ncp": ncp},
                )
    except Exception as e:
        logger.warning("daily_futures: persist rel-strength to screening failed: %s", e)


def _quote_session_open_from_snapshot(snapshot: Dict[str, Any]) -> Optional[float]:
    ohlc = snapshot.get("ohlc") if isinstance(snapshot.get("ohlc"), dict) else {}
    o = _safe_float(ohlc.get("open"))
    if o is not None and o > 0:
        return o
    return _safe_float(snapshot.get("open"))


def index_bearish_gate_from_quotes() -> Dict[str, Any]:
    """
    NIFTY-only gate for bearish: allow when NIFTY LTP is below the session day open
    (index down from the open). If LTP is at or above the open, the index is treated as bullish; ok is False.
    Used for Today’s pick — Bearish visibility and SHORT order entry.
    """
    out: Dict[str, Any] = {
        "ok": False,
        "nifty_ltp": None,
        "nifty_open": None,
        "nifty_bearish": False,
        "nifty_bullish": False,
    }
    try:
        ux = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
        b = ux.get_market_quote_snapshots_batch([NIFTY50_INDEX_KEY]) or {}
    except Exception as e:
        out["error"] = str(e)
        return out
    nq = b.get(NIFTY50_INDEX_KEY) or {}
    nlp = _safe_float(nq.get("last_price"))
    nop = _quote_session_open_from_snapshot(nq)
    out["nifty_ltp"] = nlp
    out["nifty_open"] = nop
    if nlp is None or nop is None or float(nop) <= 0:
        out["nifty_quote_incomplete"] = True
        return out
    bearish = float(nlp) < float(nop)
    out["nifty_bearish"] = bool(bearish)
    out["nifty_bullish"] = not bearish
    out["ok"] = bool(bearish)
    return out


def get_workspace(db: Session, user_id: int, lite_mode: bool = False) -> Dict[str, Any]:
    ensure_daily_futures_tables()
    td = _workspace_trade_date_ist()
    now_ist = datetime.now(IST)

    with engine.connect() as conn:
        screenings = _fetch_screening_dicts(conn, td)

    br = db.execute(
        text(
            """
            SELECT t.screening_id
            FROM daily_futures_user_trade t
            JOIN daily_futures_screening s ON s.id = t.screening_id
            WHERE t.user_id = :u
              AND t.order_status = 'bought'
              AND s.trade_date = CAST(:td AS DATE)
            """
        ),
        {"u": user_id, "td": str(td)},
    ).fetchall()
    bought_sids = {int(r[0]) for r in br}

    screening_total = len(screenings)
    n_hidden_bought = len(
        [s for s in screenings if int(s.get("screening_id") or 0) in bought_sids]
    )
    not_bought = [s for s in screenings if s["screening_id"] not in bought_sids]
    # Today's pick should surface only symbols with sufficient LIVE conviction.
    # A symbol appears automatically once live conviction reaches the threshold.
    picks = [
        s
        for s in not_bought
        if s.get("conviction_score") is not None and float(s.get("conviction_score")) >= 50.0
    ]
    picks_before_closed_filter = list(picks)

    running_rows = db.execute(
        text(
            """
            SELECT t.id, t.screening_id, t.underlying, COALESCE(t.direction_type, s.direction_type, 'LONG') AS direction_type, t.future_symbol, t.instrument_key,
                   t.lot_size, t.entry_time, t.entry_price, t.sell_price, t.sell_time, t.buy_price,
                   t.consecutive_webhook_misses,
                   t.position_atr, t.profit_trail_armed, t.nifty_structure_weakening,
                   t.trail_stop_hit, t.momentum_exhausting,
                   s.scan_count, s.first_hit_at, s.last_hit_at, s.conviction_score,
                   s.second_scan_conviction_score, s.ltp,
                   s.stock_change_pct, s.nifty_change_pct
            FROM daily_futures_user_trade t
            JOIN daily_futures_screening s ON s.id = t.screening_id
            WHERE t.user_id = :u
              AND t.order_status = 'bought'
              AND s.trade_date = CAST(:td AS DATE)
            ORDER BY t.updated_at DESC
            """
        ),
        {"u": user_id, "td": str(td)},
    ).fetchall()

    running = []
    for row in running_rows:
        miss = int(row[12] or 0)
        pos_atr = float(row[13]) if row[13] is not None else None
        dtx = str(row[3] or "LONG").strip().upper()
        stx = str(row[10]).strip() if row[10] is not None else None
        running.append(
            {
                "trade_id": row[0],
                "screening_id": row[1],
                "underlying": row[2],
                "direction_type": dtx,
                "future_symbol": row[4],
                "instrument_key": row[5],
                "lot_size": int(row[6]) if row[6] is not None else None,
                "entry_time": (row[7] if dtx != "SHORT" else stx),
                "entry_price": (float(row[8]) if row[8] is not None else None) if dtx != "SHORT" else None,
                "sell_price": (float(row[9]) if row[9] is not None else None) if dtx == "SHORT" else None,
                "sell_time": stx if dtx == "SHORT" else None,
                "consecutive_webhook_misses": miss,
                "position_atr": pos_atr,
                "profit_trail_armed": bool(row[14]) if row[14] is not None else False,
                "nifty_structure_weakening": bool(row[15]) if row[15] is not None else False,
                "trail_stop_hit": bool(row[16]) if row[16] is not None else False,
                "momentum_exhausting": bool(row[17]) if row[17] is not None else False,
                "scan_count": int(row[18] or 0),
                "first_hit_at": row[19].isoformat() if row[19] else None,
                "last_hit_at": row[20].isoformat() if row[20] else None,
                "conviction_score": float(row[21]) if row[21] is not None else None,
                "second_scan_conviction_score": float(row[22]) if row[22] is not None else None,
                "ltp": float(row[23]) if row[23] is not None else None,
                "stock_change_pct": float(row[24]) if row[24] is not None else None,
                "nifty_change_pct": float(row[25]) if row[25] is not None else None,
                "warn_two_misses": miss >= 2,
            }
        )

    closed_rows = db.execute(
        text(
            """
            SELECT t.id, t.screening_id, t.underlying, COALESCE(t.direction_type, s.direction_type, 'LONG') AS direction_type, t.future_symbol, t.instrument_key, t.lot_size,
                   t.entry_time, t.entry_price, t.sell_time, t.sell_price, t.exit_time, t.exit_price, t.buy_time, t.buy_price,
                   t.pnl_points, t.pnl_rupees,
                   s.first_hit_at,
                   s.ltp
            FROM daily_futures_user_trade t
            JOIN daily_futures_screening s ON s.id = t.screening_id
            WHERE t.user_id = :u
              AND t.order_status = 'sold'
              AND s.trade_date = CAST(:td AS DATE)
            ORDER BY t.updated_at DESC
            LIMIT 200
            """
        ),
        {"u": user_id, "td": str(td)},
    ).fetchall()

    closed = []
    total_pnl = 0.0
    wins = 0
    losses = 0
    for row in closed_rows:
        dtx = str(row[3] or "LONG").strip().upper()
        pnl_pts = float(row[15]) if row[15] is not None else None
        pnl_rs = float(row[16]) if row[16] is not None else None
        if dtx == "SHORT":
            et_disp = str(row[9]).strip() if row[9] is not None else None
            ep_disp = float(row[10]) if row[10] is not None else None
            xt = str(row[13]).strip() if row[13] is not None else (str(row[11]).strip() if row[11] is not None else None)
            xp = float(row[14]) if row[14] is not None else (float(row[12]) if row[12] is not None else None)
        else:
            et_disp = str(row[7]).strip() if row[7] is not None else None
            ep_disp = float(row[8]) if row[8] is not None else None
            xt = str(row[11]).strip() if row[11] is not None else None
            xp = float(row[12]) if row[12] is not None else None
        wl = None
        if pnl_rs is not None:
            total_pnl += pnl_rs
            if pnl_rs > 0:
                wins += 1
                wl = "Win"
            elif pnl_rs < 0:
                losses += 1
                wl = "Loss"
            else:
                wl = "Flat"
        closed.append(
            {
                "trade_id": row[0],
                "screening_id": row[1],
                "underlying": row[2],
                "direction_type": dtx,
                "future_symbol": row[4],
                "instrument_key": row[5],
                "lot_size": int(row[6]) if row[6] is not None else None,
                "entry_time": et_disp,
                "entry_price": ep_disp,
                "exit_time": xt,
                "exit_price": xp,
                "pnl_points": pnl_pts,
                "pnl_rupees": pnl_rs,
                "first_scan_time": row[17].isoformat() if row[17] is not None and hasattr(row[17], "isoformat") else None,
                "ltp": float(row[18]) if row[18] is not None else None,
                "win_loss": wl,
            }
        )

    # Re-entry cooldown: after a same-day sell for an underlying, keep Enter disabled until
    # at least one fresh scan arrives after the latest sold timestamp.
    sold_latest_by_underlying: Dict[str, datetime] = {}
    sold_latest_rows = db.execute(
        text(
            """
            SELECT UPPER(TRIM(s.underlying)) AS u, MAX(COALESCE(t.updated_at, t.created_at)) AS sold_ts
            FROM daily_futures_user_trade t
            JOIN daily_futures_screening s ON s.id = t.screening_id
            WHERE t.user_id = :u
              AND t.order_status = 'sold'
              AND s.trade_date = CAST(:td AS DATE)
            GROUP BY UPPER(TRIM(s.underlying))
            """
        ),
        {"u": user_id, "td": str(td)},
    ).fetchall()
    for u_raw, ts in sold_latest_rows:
        u = str(u_raw or "").strip().upper()
        if not u or ts is None:
            continue
        if isinstance(ts, datetime):
            sold_latest_by_underlying[u] = ts.astimezone(IST) if ts.tzinfo else IST.localize(ts)

    # Re-entry policy: if a symbol was sold earlier today but shows fresh conviction again,
    # keep it visible in Today's pick so users can re-enter.
    n_hidden_closed = 0
    not_bought_open = list(not_bought)

    def _under_fifty(s: Dict[str, Any]) -> bool:
        cs = s.get("conviction_score")
        return cs is not None and float(cs) < 50.0

    picks_low_conv_bull: List[Dict[str, Any]] = sorted(
        [
            s
            for s in not_bought_open
            if str(s.get("direction_type") or "LONG").strip().upper() == "LONG" and _under_fifty(s)
        ],
        key=lambda x: (-float(x.get("conviction_score") or 0.0), str(x.get("underlying") or "")),
    )
    picks_low_conv_bear: List[Dict[str, Any]] = sorted(
        [
            s
            for s in not_bought_open
            if str(s.get("direction_type") or "LONG").strip().upper() == "SHORT" and _under_fifty(s)
        ],
        key=lambda x: (-float(x.get("conviction_score") or 0.0), str(x.get("underlying") or "")),
    )

    picks_mixed = list(picks)
    if _bearish_index_gate_enabled():
        index_bear = index_bearish_gate_from_quotes()
    else:
        index_bear = {
            "ok": True,
            "nifty_ltp": None,
            "nifty_open": None,
            "nifty_bearish": None,
            "nifty_bullish": None,
            "index_gate_disabled": True,
        }
    picks_bull = [
        p
        for p in picks_mixed
        if str(p.get("direction_type") or "LONG").strip().upper() != "SHORT"
    ]
    picks_bear_all = [
        p
        for p in picks_mixed
        if str(p.get("direction_type") or "LONG").strip().upper() == "SHORT"
    ]
    # With index gate: NIFTY LTP below day open. Without gate: show all bearish ≥50.
    if _bearish_index_gate_enabled():
        picks_bearish = picks_bear_all if bool(index_bear.get("ok")) else []
    else:
        picks_bearish = list(picks_bear_all)

    denom = wins + losses
    win_rate = round(100.0 * wins / denom, 1) if denom else None

    _low_conv_extras: List[Dict[str, Any]] = list(picks_low_conv_bull) + list(picks_low_conv_bear)
    _picks_for_quotes: List[Dict[str, Any]] = list(picks_mixed) + _low_conv_extras
    # Main page always loads with lite=1 for speed. Still refresh live LTP (and rel. str. below) for
    # Today's pick rows; otherwise the UI shows em dashes. Full mode also updates running/closed.
    try:
        if lite_mode:
            _apply_live_ltps_to_picks_and_running(_picks_for_quotes, [], [])
        else:
            _apply_live_ltps_to_picks_and_running(_picks_for_quotes, running, closed)
    except Exception as e:
        logger.warning("daily_futures: live LTP refresh failed: %s", e, exc_info=True)

    # For What-If continuing: after session close, treat current LTP as 15:15 close.
    if closed and now_ist.time() >= dt_time(15, 15):
        close_1515 = IST.localize(datetime.combine(td, datetime.min.time()).replace(hour=15, minute=15))
        candles_cache: Dict[str, List[Dict[str, Any]]] = {}
        try:
            upstox_1515 = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
        except Exception as e:
            logger.warning("daily_futures: 15:15 LTP override init failed: %s", e)
            upstox_1515 = None
        if upstox_1515 is not None:
            for r in closed:
                ik = str(r.get("instrument_key") or "").strip()
                if not ik:
                    continue
                if ik not in candles_cache:
                    candles_cache[ik] = _fetch_intraday_1m_cached(upstox_1515, ik, td)
                cset = candles_cache.get(ik) or []
                ltp_1515 = _ltp_asof_ist(cset, close_1515)
                if ltp_1515 is not None:
                    r["ltp"] = ltp_1515

    try:
        if lite_mode:
            _apply_live_rel_strength_to_picks_and_running(_picks_for_quotes, [], td)
        else:
            _apply_live_rel_strength_to_picks_and_running(_picks_for_quotes, running, td)
    except Exception as e:
        logger.warning("daily_futures: rel-strength refresh failed: %s", e, exc_info=True)

    strip_debug: Dict[str, Any] = {}
    if not lite_mode:
        try:
            strip_debug = _apply_exit_alerts_to_running(db, running, td) or {}
        except Exception as e:
            logger.warning("daily_futures: exit alerts failed: %s", e, exc_info=True)
            for r in running:
                r["nifty_structure_weakening"] = bool(r.get("nifty_structure_weakening"))
                r["trail_stop_hit"] = bool(r.get("trail_stop_hit"))
                r["momentum_exhausting"] = bool(r.get("momentum_exhausting"))
                r["exit_review"] = bool(r.get("trail_stop_hit")) or (
                    bool(r.get("nifty_structure_weakening")) and bool(r.get("momentum_exhausting"))
                )
                r["alert_strip"] = {
                    "l1": "nifty_no_higher_high",
                    "l2": "building",
                    "l3": "strong",
                    "decision": "hold",
                }

    for p in picks_mixed:
        reasons: List[str] = []
        dtp = str(p.get("direction_type") or "LONG").strip().upper()
        und_u = str(p.get("underlying") or "").strip().upper()
        sold_ts = sold_latest_by_underlying.get(und_u)
        if sold_ts is not None:
            last_hit_dt = _parse_iso_ist(p.get("last_hit_at"))
            if last_hit_dt is None or last_hit_dt <= sold_ts:
                reasons.append("Re-entry unlocks after next scan post-exit")
        if int(p.get("scan_count") or 0) < 2:
            reasons.append("Needs at least 2 scans")
        live_conv = p.get("conviction_score")
        c2 = live_conv
        if c2 is None:
            reasons.append("Live conviction unavailable")
        elif dtp == "SHORT":
            if float(c2) <= 50.0:
                reasons.append(f"Live conviction {round(float(c2),1)} is not above 50")
            if _bearish_index_gate_enabled() and not bool(index_bear.get("ok")):
                reasons.append("NIFTY is not below the day open (no SHORT from this list)")
        else:
            if float(live_conv) < 60.0:
                reasons.append(f"Live conviction {round(float(live_conv),1)} is below 60")
        p["order_eligible"] = len(reasons) == 0
        p["order_block_reason"] = reasons[0] if reasons else None

    return {
        "trade_date": str(td),
        "session_before_open": False,
        "session_message": None,
        "picks": picks_bull,
        "picks_mixed": picks_mixed,
        "picks_bearish": picks_bearish,
        "picks_low_conv_bull": picks_low_conv_bull,
        "picks_low_conv_bear": picks_low_conv_bear,
        "index_bearish_gate": index_bear,
        "picks_diagnostics": {
            "screening_count": screening_total,
            "hidden_because_bought": n_hidden_bought,
            "hidden_because_sold_today": n_hidden_closed,
        },
        "running": running,
        "closed": closed,
        "trade_if_could_have_done": [] if lite_mode else _build_trade_if_could_rows(picks_mixed, closed, td),
        "summary": {
            "cumulative_pnl_rupees": round(total_pnl, 2),
            "wins": wins,
            "losses": losses,
            "win_rate_pct": win_rate,
            "strip_debug": strip_debug,
        },
        "lite_mode": bool(lite_mode),
    }


def get_workspace_running_enriched(db: Session, user_id: int) -> Dict[str, Any]:
    """Running section only: fetch lite base, then apply running enrichments."""
    base = get_workspace(db, user_id, lite_mode=True)
    if base.get("session_before_open"):
        return {
            "trade_date": base.get("trade_date"),
            "session_before_open": True,
            "running": [],
            "summary": {"strip_debug": {}},
        }
    td = base.get("trade_date")
    try:
        td = date.fromisoformat(str(td))
    except Exception:
        td = _workspace_trade_date_ist()
    running = list(base.get("running") or [])
    try:
        _apply_live_ltps_to_picks_and_running([], running, [])
    except Exception as e:
        logger.warning("daily_futures: running_enriched LTP refresh failed: %s", e, exc_info=True)
    try:
        _apply_live_rel_strength_to_picks_and_running([], running, td)
    except Exception as e:
        logger.warning("daily_futures: running_enriched rel-strength failed: %s", e, exc_info=True)
    strip_debug: Dict[str, Any] = {}
    try:
        strip_debug = _apply_exit_alerts_to_running(db, running, td) or {}
    except Exception as e:
        logger.warning("daily_futures: running_enriched exit alerts failed: %s", e, exc_info=True)
    return {
        "trade_date": base.get("trade_date"),
        "session_before_open": False,
        "running": running,
        "summary": {"strip_debug": strip_debug},
    }


def get_workspace_trade_if_could(db: Session, user_id: int) -> Dict[str, Any]:
    """Heavy Trade-if-could section as an isolated call."""
    base = get_workspace(db, user_id, lite_mode=True)
    if base.get("session_before_open"):
        return {
            "trade_date": base.get("trade_date"),
            "session_before_open": True,
            "trade_if_could_have_done": [],
        }
    td = base.get("trade_date")
    try:
        td = date.fromisoformat(str(td))
    except Exception:
        td = _workspace_trade_date_ist()
    pm = list(base.get("picks_mixed") or base.get("picks") or [])
    rows = _build_trade_if_could_rows(pm, list(base.get("closed") or []), td)
    return {
        "trade_date": base.get("trade_date"),
        "session_before_open": False,
        "trade_if_could_have_done": rows,
    }


def confirm_buy(db: Session, user_id: int, screening_id: int, entry_time: str, entry_price: float) -> Dict[str, Any]:
    ensure_daily_futures_tables()
    if not is_daily_futures_session_open_ist():
        raise ValueError("Daily Futures session opens at 09:00 IST. Orders are not accepted before that.")
    row = db.execute(
        text(
            """
            SELECT id, underlying, direction_type, future_symbol, instrument_key, lot_size, scan_count,
                   conviction_score, second_scan_conviction_score
            FROM daily_futures_screening WHERE id = :sid AND trade_date = CAST(:d AS DATE)
            """
        ),
        {"sid": screening_id, "d": str(ist_today())},
    ).fetchone()
    if not row:
        raise ValueError("Screening row not found for today")
    if int(row[6] or 0) < 2:
        raise ValueError("Needs at least 2 consecutive scans before order")
    dtp = str(row[2] or "LONG").strip().upper()
    live_score = float(row[7]) if row[7] is not None else None
    c2 = live_score
    if dtp == "SHORT":
        if c2 is None or c2 <= 50.0:
            raise ValueError(f"Conviction must be above 50 for SHORT (current: {round(c2 or 0.0, 1)})")
        if _bearish_index_gate_enabled():
            ig = index_bearish_gate_from_quotes()
            if not bool(ig.get("ok")):
                raise ValueError(
                    "SHORT is allowed only when NIFTY is below the day open. NIFTY is not bearish vs open right now."
                )
    else:
        if live_score is None or live_score < 60.0:
            raise ValueError(
                f"Live conviction must be at least 60 (current: {round(live_score or 0.0, 1)})"
            )

    exists = db.execute(
        text(
            """
            SELECT id FROM daily_futures_user_trade
            WHERE user_id = :u AND screening_id = :sid AND order_status = 'bought'
            """
        ),
        {"u": user_id, "sid": screening_id},
    ).fetchone()
    if exists:
        raise ValueError("Already bought this pick")

    if dtp == "SHORT":
        ins = db.execute(
            text(
                """
                INSERT INTO daily_futures_user_trade (
                  user_id, screening_id, underlying, direction_type, future_symbol, instrument_key, lot_size,
                  order_status, sell_time, sell_price, consecutive_webhook_misses
                ) VALUES (
                  :u, :sid, :und, :dt, :fs, :ik, :lot, 'bought', :st, :sp, 0
                ) RETURNING id
                """
            ),
            {
                "u": user_id,
                "sid": screening_id,
                "und": row[1],
                "dt": dtp,
                "fs": row[3],
                "ik": row[4],
                "lot": row[5],
                "st": entry_time.strip(),
                "sp": entry_price,
            },
        ).fetchone()
    else:
        ins = db.execute(
            text(
                """
                INSERT INTO daily_futures_user_trade (
                  user_id, screening_id, underlying, direction_type, future_symbol, instrument_key, lot_size,
                  order_status, entry_time, entry_price, consecutive_webhook_misses
                ) VALUES (
                  :u, :sid, :und, :dt, :fs, :ik, :lot, 'bought', :et, :ep, 0
                ) RETURNING id
                """
            ),
            {
                "u": user_id,
                "sid": screening_id,
                "und": row[1],
                "dt": dtp,
                "fs": row[3],
                "ik": row[4],
                "lot": row[5],
                "et": entry_time.strip(),
                "ep": entry_price,
            },
        ).fetchone()
    trade_id = int(ins[0]) if ins and ins[0] is not None else None
    ikey = str(row[4] or "").strip()
    if trade_id and ikey:
        try:
            uxs = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
            atr0 = _compute_position_atr_15m_5d(uxs, ikey, ist_today())
            if atr0 is not None and atr0 > 0:
                db.execute(
                    text(
                        """
                        UPDATE daily_futures_user_trade SET
                          position_atr = CAST(:a AS NUMERIC), updated_at = CURRENT_TIMESTAMP
                        WHERE id = :id
                        """
                    ),
                    {"a": float(atr0), "id": trade_id},
                )
        except Exception as e:
            logger.warning("daily_futures: position ATR at entry (non-fatal): %s", e)
    db.commit()
    return {"success": True}


def confirm_sell(db: Session, user_id: int, trade_id: int, exit_time: str, exit_price: float) -> Dict[str, Any]:
    ensure_daily_futures_tables()
    row = db.execute(
        text(
            """
            SELECT id, screening_id, underlying, entry_price, sell_price, lot_size, instrument_key,
                   COALESCE(direction_type, 'LONG') AS direction_type
            FROM daily_futures_user_trade
            WHERE id = :id AND user_id = :u AND order_status = 'bought'
            """
        ),
        {"id": trade_id, "u": user_id},
    ).fetchone()
    if not row:
        raise ValueError("Open trade not found")

    dtx = str(row[7] or "LONG").strip().upper()
    lot = int(row[5]) if row[5] is not None else None
    ref_px: Optional[float] = None
    if dtx == "SHORT":
        ref_px = float(row[4]) if row[4] is not None else None  # sell_price
    else:
        ref_px = float(row[3]) if row[3] is not None else None
    cover_px = float(exit_price)
    pts: Optional[float] = None
    pnl_rs: Optional[float] = None
    if ref_px is not None:
        if dtx == "SHORT":
            pts = round(float(ref_px) - cover_px, 4)
        else:
            pts = round(cover_px - float(ref_px), 4)
        if lot:
            pnl_rs = round(float(pts) * int(lot), 2)

    if dtx == "SHORT":
        db.execute(
            text(
                """
                UPDATE daily_futures_user_trade SET
                  order_status = 'sold',
                  buy_time = :bt,
                  buy_price = :bp,
                  exit_time = :bt,
                  exit_price = :bp,
                  pnl_points = :pts,
                  pnl_rupees = :pnl,
                  updated_at = CURRENT_TIMESTAMP
                WHERE id = :id AND user_id = :u
                """
            ),
            {
                "bt": exit_time.strip(),
                "bp": cover_px,
                "pts": pts,
                "pnl": pnl_rs,
                "id": trade_id,
                "u": user_id,
            },
        )
    else:
        db.execute(
            text(
                """
                UPDATE daily_futures_user_trade SET
                  order_status = 'sold',
                  exit_time = :xt,
                  exit_price = :xp,
                  pnl_points = :pts,
                  pnl_rupees = :pnl,
                  updated_at = CURRENT_TIMESTAMP
                WHERE id = :id AND user_id = :u
                """
            ),
            {
                "xt": exit_time.strip(),
                "xp": cover_px,
                "pts": pts,
                "pnl": pnl_rs,
                "id": trade_id,
                "u": user_id,
            },
        )
    db.commit()
    return {"success": True, "pnl_points": pts, "pnl_rupees": pnl_rs}


def get_conviction_breakdown_debug(
    db: Session,
    future_symbol: str,
    trade_date: Optional[str] = None,
) -> Dict[str, Any]:
    ensure_daily_futures_tables()
    td = trade_date or str(ist_today())
    row = db.execute(
        text(
            """
            SELECT id, trade_date, underlying, future_symbol, instrument_key,
                   conviction_score, second_scan_conviction_score,
                   conviction_oi_leg, conviction_vwap_leg,
                   candle_is_green, candle_higher_high, candle_higher_low,
                   conviction_breakdown_json, updated_at
            FROM daily_futures_screening
            WHERE trade_date = CAST(:td AS DATE)
              AND UPPER(TRIM(future_symbol)) = :fs
            ORDER BY id DESC
            LIMIT 1
            """
        ),
        {"td": td, "fs": str(future_symbol or "").strip().upper()},
    ).mappings().first()
    if not row:
        raise ValueError(f"No screening row found for {future_symbol!r} on {td}")

    out = dict(row)
    for k in ("trade_date", "updated_at"):
        v = out.get(k)
        if v is not None and hasattr(v, "isoformat"):
            out[k] = v.isoformat()
    for k in ("conviction_score", "second_scan_conviction_score", "conviction_oi_leg", "conviction_vwap_leg"):
        v = out.get(k)
        if v is not None:
            out[k] = float(v)
    return out


def chartink_webhook_inbox_dir() -> Path:
    """
    Where raw ChartInk POST bodies are written before parsing/processing.
    Override with env CHARTINK_DF_WEBHOOK_INBOX (absolute path on server is fine).
    """
    override = (os.getenv("CHARTINK_DF_WEBHOOK_INBOX") or "").strip()
    if override:
        return Path(override).expanduser()
    # backend/services/... -> project root
    root = Path(__file__).resolve().parents[2]
    return root / "inbox" / "chartink_daily_futures"


def persist_chartink_webhook_raw_body(body: bytes) -> str:
    """
    Synchronous write of the exact bytes ChartInk sent. Call before any heavy work
    so disconnects/499 or worker crashes do not lose the payload (operators can
    re-run from this file if needed). Returns absolute path. Raises on I/O error.
    """
    d = chartink_webhook_inbox_dir()
    d.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S_%fZ")
    short = uuid.uuid4().hex[:8]
    p = d / f"{ts}_{short}.raw.json"
    p.write_bytes(body)
    return str(p)


def webhook_secret_ok(provided: Optional[str]) -> bool:
    default_secret = "tradewithctodailyfuture"
    expected = (os.getenv("CHARTINK_DAILY_FUTURES_SECRET") or default_secret).strip()
    return bool(provided and provided.strip() == expected)


def chartink_bearish_webhook_inbox_dir() -> Path:
    override = (os.getenv("CHARTINK_DF_BEARISH_WEBHOOK_INBOX") or "").strip()
    if override:
        return Path(override).expanduser()
    root = Path(__file__).resolve().parents[2]
    return root / "inbox" / "chartink_daily_futures_bearish"


def persist_chartink_bearish_webhook_raw_body(body: bytes) -> str:
    d = chartink_bearish_webhook_inbox_dir()
    d.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S_%fZ")
    short = uuid.uuid4().hex[:8]
    p = d / f"{ts}_{short}.raw.bear.json"
    p.write_bytes(body)
    return str(p)


def webhook_bearish_secret_ok(provided: Optional[str]) -> bool:
    default_secret = "tradewithctodailyfuture_bearish"
    expected = (os.getenv("CHARTINK_DF_BEARISH_SECRET") or default_secret).strip()
    return bool(provided and provided.strip() == expected)


def refresh_chartink_webhook_inbox_dirs() -> Dict[str, Any]:
    """
    Remove persisted raw JSON files written before processing ChartInk webhooks
    (``*.raw.json`` / ``*.raw.bear.json``). Does not remove directories.
    """
    out: Dict[str, Any] = {"ok": True, "files_removed": 0, "by_dir": [], "errors": []}

    def _clean_one(label: str, d: Path) -> None:
        n = 0
        if not d.exists():
            out["by_dir"].append({"label": label, "path": str(d), "existed": False, "removed": 0})
            return
        for p in list(d.iterdir()):
            if not p.is_file():
                continue
            if not (p.name.endswith(".raw.json") or p.name.endswith(".raw.bear.json")):
                continue
            try:
                p.unlink()
                n += 1
            except OSError as e:
                out["errors"].append(f"{p}: {e}")
        out["by_dir"].append({"label": label, "path": str(d), "existed": True, "removed": n})
        out["files_removed"] += n

    _clean_one("long", chartink_webhook_inbox_dir())
    _clean_one("bear", chartink_bearish_webhook_inbox_dir())
    out["ok"] = len(out["errors"]) == 0
    return out

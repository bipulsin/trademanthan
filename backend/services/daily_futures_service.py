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
from typing import Any, Dict, List, Literal, Optional, Set, Tuple
from urllib.parse import quote, parse_qs

import pytz
from starlette.datastructures import UploadFile
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
    When True, Today's pick — Bearish is filtered by NIFTY spot 5m structure (quotes/candles).

    **Default is off**: no index-based hiding or SHORT entry block from this gate.

    Legacy opt-in: set DAILY_FUTURES_BEARISH_INDEX_GATE_ENABLED=1|true|yes|on on the server.
    """
    raw = os.getenv("DAILY_FUTURES_BEARISH_INDEX_GATE_ENABLED")
    if raw is None or (isinstance(raw, str) and raw.strip() == ""):
        return False
    v = str(raw).strip().lower()
    return v in ("1", "true", "yes", "on")
_DF_TABLES_READY = False
_DF_TABLES_LOCK = threading.Lock()
_INDICATOR_CANDLE_CACHE: Dict[Tuple[str, date, datetime], List[Dict[str, Any]]] = {}
_INDICATOR_EVAL_CACHE: Dict[Tuple[str, str], Dict[str, Any]] = {}


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
        second_scan_oi_leg NUMERIC(8,2),
        second_scan_vwap_leg NUMERIC(8,2),
        second_scan_stock_change_pct NUMERIC(18,6),
        second_scan_nifty_change_pct NUMERIC(18,6),
        qualifying_scan_streak INTEGER NOT NULL DEFAULT 0,
        entry_window_start TIMESTAMPTZ,
        entry_window_end TIMESTAMPTZ,
        effective_conviction NUMERIC(5,1),
        last_5m_momentum_pass BOOLEAN,
        last_5m_evaluated_at TIMESTAMPTZ,
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
        peak_unrealized_pnl_rupees NUMERIC(18,4),
        profit_giveback_breach BOOLEAN NOT NULL DEFAULT FALSE,
        bearish_signal_count INTEGER NOT NULL DEFAULT 0,
        bearish_conditions_active TEXT,
        last_bearish_evaluated_at TIMESTAMPTZ,
        indicator_decision TEXT,
        last_indicator_candle_ts TIMESTAMPTZ,
        first_amber_alert_at TIMESTAMPTZ,
        first_hard_exit_alert_at TIMESTAMPTZ,
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
            conn.execute(text("ALTER TABLE daily_futures_screening ADD COLUMN IF NOT EXISTS second_scan_oi_leg NUMERIC(8,2)"))
            conn.execute(text("ALTER TABLE daily_futures_screening ADD COLUMN IF NOT EXISTS second_scan_vwap_leg NUMERIC(8,2)"))
            conn.execute(text("ALTER TABLE daily_futures_screening ADD COLUMN IF NOT EXISTS second_scan_stock_change_pct NUMERIC(18,6)"))
            conn.execute(text("ALTER TABLE daily_futures_screening ADD COLUMN IF NOT EXISTS second_scan_nifty_change_pct NUMERIC(18,6)"))
            conn.execute(text("ALTER TABLE daily_futures_screening ADD COLUMN IF NOT EXISTS qualifying_scan_streak INTEGER NOT NULL DEFAULT 0"))
            conn.execute(text("ALTER TABLE daily_futures_screening ADD COLUMN IF NOT EXISTS entry_window_start TIMESTAMPTZ"))
            conn.execute(text("ALTER TABLE daily_futures_screening ADD COLUMN IF NOT EXISTS entry_window_end TIMESTAMPTZ"))
            conn.execute(text("ALTER TABLE daily_futures_screening ADD COLUMN IF NOT EXISTS effective_conviction NUMERIC(5,1)"))
            conn.execute(text("ALTER TABLE daily_futures_screening ADD COLUMN IF NOT EXISTS last_5m_momentum_pass BOOLEAN"))
            conn.execute(text("ALTER TABLE daily_futures_screening ADD COLUMN IF NOT EXISTS last_5m_evaluated_at TIMESTAMPTZ"))
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
            conn.execute(
                text("ALTER TABLE daily_futures_user_trade ADD COLUMN IF NOT EXISTS peak_unrealized_pnl_rupees NUMERIC(18,4)")
            )
            conn.execute(
                text(
                    "ALTER TABLE daily_futures_user_trade ADD COLUMN IF NOT EXISTS profit_giveback_breach BOOLEAN NOT NULL DEFAULT FALSE"
                )
            )
            conn.execute(text("ALTER TABLE daily_futures_user_trade ADD COLUMN IF NOT EXISTS bearish_signal_count INTEGER NOT NULL DEFAULT 0"))
            conn.execute(text("ALTER TABLE daily_futures_user_trade ADD COLUMN IF NOT EXISTS bearish_conditions_active TEXT"))
            conn.execute(text("ALTER TABLE daily_futures_user_trade ADD COLUMN IF NOT EXISTS last_bearish_evaluated_at TIMESTAMPTZ"))
            conn.execute(text("ALTER TABLE daily_futures_user_trade ADD COLUMN IF NOT EXISTS indicator_decision TEXT"))
            conn.execute(text("ALTER TABLE daily_futures_user_trade ADD COLUMN IF NOT EXISTS last_indicator_candle_ts TIMESTAMPTZ"))
            conn.execute(text("ALTER TABLE daily_futures_user_trade ADD COLUMN IF NOT EXISTS first_amber_alert_at TIMESTAMPTZ"))
            conn.execute(text("ALTER TABLE daily_futures_user_trade ADD COLUMN IF NOT EXISTS first_hard_exit_alert_at TIMESTAMPTZ"))
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
            if ts is None or ts.date() != session_date or ts > cutoff:
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


def _last_completed_15m_candles_for_indicator(
    upstox: UpstoxService,
    instrument_key: str,
    session_date: date,
    now_ist: datetime,
    limit: int = 30,
) -> List[Dict[str, Any]]:
    ik = str(instrument_key or "").strip()
    if not ik:
        return []
    cutoff = _floor_ist_to_15m(now_ist)
    ck = (ik, session_date, cutoff)
    cached = _INDICATOR_CANDLE_CACHE.get(ck)
    if cached:
        return cached[-max(30, int(limit)) :]

    rows_all: List[Dict[str, Any]] = []
    try:
        raw = upstox.get_historical_candles_by_instrument_key(
            ik, interval="minutes/15", days_back=15, range_end_date=session_date
        ) or []
        rows_all.extend(raw)
    except Exception as e:
        logger.debug("daily_futures: indicator 15m historical failed for %s: %s", ik, e)
    if session_date == ist_today():
        try:
            key_enc = quote(ik, safe="")
            intraday_url = f"{upstox.base_url}/historical-candle/intraday/{key_enc}/minutes/15"
            raw_i = upstox.make_api_request(intraday_url, method="GET", timeout=15, max_retries=2) or {}
            if isinstance(raw_i, dict) and raw_i.get("status") == "success":
                rows_i = ((raw_i.get("data") or {}).get("candles")) or []
                rows_all.extend(_candles_rows_to_structured(rows_i) or [])
        except Exception as e:
            logger.debug("daily_futures: indicator 15m intraday failed for %s: %s", ik, e)

    merged: Dict[str, Dict[str, Any]] = {}
    for c in rows_all:
        ts = _parse_iso_ist((c or {}).get("timestamp"))
        if ts is None or ts > cutoff:
            continue
        op = _safe_float((c or {}).get("open"))
        hi = _safe_float((c or {}).get("high"))
        lo = _safe_float((c or {}).get("low"))
        cl = _safe_float((c or {}).get("close"))
        vol = _safe_float((c or {}).get("volume"))
        if op is None or hi is None or lo is None or cl is None or vol is None:
            continue
        k = ts.isoformat()
        merged[k] = {"timestamp": ts, "open": op, "high": hi, "low": lo, "close": cl, "volume": vol}
    out = sorted(merged.values(), key=lambda x: x["timestamp"])
    _INDICATOR_CANDLE_CACHE[ck] = out
    return out[-max(30, int(limit)) :]


def _ema(values: List[float], period: int) -> List[float]:
    if not values:
        return []
    p = max(1, int(period))
    k = 2.0 / (p + 1.0)
    out: List[float] = []
    ema_v = float(values[0])
    for v in values:
        ema_v = (float(v) * k) + (ema_v * (1.0 - k))
        out.append(float(ema_v))
    return out


def _wma(values: List[float], period: int) -> List[Optional[float]]:
    p = max(1, int(period))
    if not values:
        return []
    weights = list(range(1, p + 1))
    den = float(sum(weights))
    out: List[Optional[float]] = [None] * len(values)
    for i in range(p - 1, len(values)):
        window = values[i - p + 1 : i + 1]
        num = 0.0
        for j, v in enumerate(window):
            num += float(v) * float(weights[j])
        out[i] = num / den if den > 0 else None
    return out


def _rma(values: List[float], period: int) -> List[Optional[float]]:
    p = max(1, int(period))
    out: List[Optional[float]] = [None] * len(values)
    if len(values) < p:
        return out
    seed = sum(float(x) for x in values[:p]) / float(p)
    out[p - 1] = seed
    prev = seed
    for i in range(p, len(values)):
        prev = ((prev * (p - 1)) + float(values[i])) / float(p)
        out[i] = prev
    return out


def _rsi_wilder(closes: List[float], period: int = 9) -> List[Optional[float]]:
    if len(closes) < 2:
        return [None] * len(closes)
    gains: List[float] = [0.0]
    losses: List[float] = [0.0]
    for i in range(1, len(closes)):
        d = float(closes[i]) - float(closes[i - 1])
        gains.append(max(0.0, d))
        losses.append(max(0.0, -d))
    rg = _rma(gains, period)
    rl = _rma(losses, period)
    out: List[Optional[float]] = [None] * len(closes)
    for i in range(len(closes)):
        ag = rg[i]
        al = rl[i]
        if ag is None or al is None:
            continue
        if al == 0:
            out[i] = 100.0
        else:
            rs = float(ag) / float(al)
            out[i] = 100.0 - (100.0 / (1.0 + rs))
    return out


def _obv(closes: List[float], volumes: List[float]) -> List[float]:
    if not closes or not volumes or len(closes) != len(volumes):
        return []
    out: List[float] = [0.0]
    for i in range(1, len(closes)):
        if closes[i] > closes[i - 1]:
            out.append(out[-1] + float(volumes[i]))
        elif closes[i] < closes[i - 1]:
            out.append(out[-1] - float(volumes[i]))
        else:
            out.append(out[-1])
    return out


def _sma(values: List[float], period: int) -> List[Optional[float]]:
    p = max(1, int(period))
    out: List[Optional[float]] = [None] * len(values)
    if len(values) < p:
        return out
    run = sum(float(x) for x in values[:p])
    out[p - 1] = run / float(p)
    for i in range(p, len(values)):
        run += float(values[i]) - float(values[i - p])
        out[i] = run / float(p)
    return out


def _adx_di_wilder(highs: List[float], lows: List[float], closes: List[float], period: int = 14) -> Tuple[List[Optional[float]], List[Optional[float]]]:
    n = len(closes)
    plus_dm = [0.0] * n
    minus_dm = [0.0] * n
    tr = [0.0] * n
    for i in range(1, n):
        up_move = float(highs[i]) - float(highs[i - 1])
        down_move = float(lows[i - 1]) - float(lows[i])
        plus_dm[i] = up_move if (up_move > down_move and up_move > 0) else 0.0
        minus_dm[i] = down_move if (down_move > up_move and down_move > 0) else 0.0
        tr[i] = max(
            float(highs[i]) - float(lows[i]),
            abs(float(highs[i]) - float(closes[i - 1])),
            abs(float(lows[i]) - float(closes[i - 1])),
        )
    tr_rma = _rma(tr, period)
    plus_rma = _rma(plus_dm, period)
    minus_rma = _rma(minus_dm, period)
    di_plus: List[Optional[float]] = [None] * n
    di_minus: List[Optional[float]] = [None] * n
    for i in range(n):
        trv = tr_rma[i]
        pv = plus_rma[i]
        mv = minus_rma[i]
        if trv is None or pv is None or mv is None or float(trv) <= 0:
            continue
        di_plus[i] = (float(pv) / float(trv)) * 100.0
        di_minus[i] = (float(mv) / float(trv)) * 100.0
    return di_plus, di_minus


def _evaluate_indicator_exit_signal(
    instrument_key: str,
    direction_type: str,
    candles: List[Dict[str, Any]],
) -> Dict[str, Any]:
    if len(candles) < 30:
        return {"count": 0, "bullish_count": 0, "bearish_count": 0, "conditions": [], "latest_candle_ts": None}
    latest_ts = (candles[-1].get("timestamp") or datetime.now(IST)).isoformat()
    cache_key = (f"{str(instrument_key or '')}:{str(direction_type or 'LONG').upper()}", str(latest_ts))
    cached = _INDICATOR_EVAL_CACHE.get(cache_key)
    if cached:
        return cached

    closes = [float(c["close"]) for c in candles]
    highs = [float(c["high"]) for c in candles]
    lows = [float(c["low"]) for c in candles]
    vols = [float(c["volume"]) for c in candles]
    macd_line = []
    e12 = _ema(closes, 12)
    e26 = _ema(closes, 26)
    for i in range(len(closes)):
        macd_line.append(float(e12[i]) - float(e26[i]))
    macd_sig = _ema(macd_line, 9)
    di_plus, di_minus = _adx_di_wilder(highs, lows, closes, 14)
    wma21 = _wma(closes, 21)
    rsi9 = _rsi_wilder(closes, 9)
    ema3 = _ema(closes, 3)
    obv = _obv(closes, vols)
    obv_sma10 = _sma(obv, 10)
    i = len(candles) - 1

    is_short = str(direction_type or "LONG").strip().upper() == "SHORT"
    bull_map = {
        "C1": bool(macd_line[i] < macd_sig[i]),
        "C2": bool((di_minus[i] or 0) > (di_plus[i] or 0)),
        # Hilega-Milega bearish flip (for LONG exit): fast EMA below WMA and RSI below WMA.
        "C3": bool(
            (wma21[i] is not None and rsi9[i] is not None and ema3[i] is not None)
            and (ema3[i] < wma21[i] and rsi9[i] < wma21[i])
        ),
        "C4": bool(obv_sma10[i] is not None and obv[i] < float(obv_sma10[i])),
    }
    bear_map = {
        "C1": bool(macd_line[i] > macd_sig[i]),
        "C2": bool((di_minus[i] or 0) < (di_plus[i] or 0)),
        # Hilega-Milega bullish flip (for SHORT exit): fast EMA above WMA and RSI above WMA.
        "C3": bool(
            (wma21[i] is not None and rsi9[i] is not None and ema3[i] is not None)
            and (ema3[i] > wma21[i] and rsi9[i] > wma21[i])
        ),
        "C4": bool(obv_sma10[i] is not None and obv[i] > float(obv_sma10[i])),
    }
    bullish_count = len([k for k, v in bull_map.items() if v])
    bearish_count = len([k for k, v in bear_map.items() if v])
    cond_map = bear_map if is_short else bull_map
    active = [k for k, v in cond_map.items() if v]
    count = bearish_count if is_short else bullish_count
    txt_long = {
        "C1": "MACD bearish",
        "C2": "DI- crossed above DI+",
        "C3": "Hilega-Milega flipped",
        "C4": "OBV broke SMA10",
    }
    txt_short = {
        "C1": "MACD bullish",
        "C2": "DI+ crossed above DI-",
        "C3": "Hilega-Milega flipped bullish",
        "C4": "OBV crossed above SMA10",
    }
    label_map = txt_short if is_short else txt_long
    out = {
        "count": count,
        "bullish_count": bullish_count,
        "bearish_count": bearish_count,
        "conditions": active,
        "conditions_text": [label_map[c] for c in active],
        "latest_candle_ts": latest_ts,
    }
    _INDICATOR_EVAL_CACHE[cache_key] = out
    return out


def _compute_effective_conviction_and_5m_momentum(
    screenings: List[Dict[str, Any]],
    now_ist: datetime,
) -> None:
    """Live effective conviction decay on workspace read. FO 5m momentum gate removed (no longer fetched or enforced)."""
    if not screenings:
        return
    decay_raw = os.getenv(
        "DAILY_FUTURES_CONVICTION_DECAY_PER_SCAN",
        str(getattr(settings, "DAILY_FUTURES_CONVICTION_DECAY_PER_SCAN", 0.08) or 0.08),
    )
    decay_per_scan = float(decay_raw or 0.08)
    decay_per_scan = max(0.0, min(0.5, decay_per_scan))
    for s in screenings:
        raw_conv = _safe_float(s.get("conviction_score"))
        scan_count = int(s.get("scan_count") or 0)
        decay_factor = max(0.5, 1.0 - max(0, scan_count - 1) * decay_per_scan)
        eff = round(float(raw_conv) * float(decay_factor), 1) if raw_conv is not None else None
        s["effective_conviction"] = eff
        s["conviction_decay_factor"] = round(float(decay_factor), 4)
        s["last_5m_momentum_pass"] = True
        s["last_5m_evaluated_at"] = now_ist.isoformat()

    # Persist live-computed values on workspace read (required).
    try:
        with engine.begin() as conn:
            for s in screenings:
                conn.execute(
                    text(
                        """
                        UPDATE daily_futures_screening
                        SET effective_conviction = :ec,
                            last_5m_momentum_pass = :mp,
                            last_5m_evaluated_at = CAST(:ev AS TIMESTAMPTZ),
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = :id
                        """
                    ),
                    {
                        "ec": float(s["effective_conviction"]) if s.get("effective_conviction") is not None else None,
                        "mp": s.get("last_5m_momentum_pass"),
                        "ev": s.get("last_5m_evaluated_at"),
                        "id": int(s.get("screening_id") or 0),
                    },
                )
    except Exception as e:
        logger.warning("daily_futures: persist effective conviction/5m momentum failed: %s", e)


def _apply_running_sl_ladder(rows: List[Dict[str, Any]]) -> None:
    """
    Running SL policy:
    - Initial SL: entry +/- 1.8 * ATR(15m ATR(5d) proxy)
    - If unrealized >= 5k, lock 2k
    - If unrealized >= 8k, lock 5k; then +1k lock for every +1k unrealized above 8k
    """
    if not rows:
        return
    for r in rows:
        d = str(r.get("direction_type") or "LONG").strip().upper()
        lot = _safe_float(r.get("lot_size")) or 0.0
        atr = _safe_float(r.get("position_atr")) or 0.0
        ep = _safe_float(r.get("sell_price") if d == "SHORT" else r.get("entry_price"))
        ltp = _safe_float(r.get("ltp"))
        if lot <= 0 or ep is None or atr <= 0:
            r["running_sl_price"] = None
            r["running_sl_source"] = None
            continue
        init_sl = (ep + 1.8 * atr) if d == "SHORT" else (ep - 1.8 * atr)
        pnl = None
        if ltp is not None:
            pnl = (ep - ltp) * lot if d == "SHORT" else (ltp - ep) * lot
        lock_rs = 0.0
        if pnl is not None and pnl >= 8000.0:
            lock_rs = 5000.0 + (int((pnl - 8000.0) // 1000.0) * 1000.0)
        elif pnl is not None and pnl >= 5000.0:
            lock_rs = 2000.0
        if lock_rs > 0:
            lock_pts = lock_rs / lot
            lock_sl = (ep - lock_pts) if d == "SHORT" else (ep + lock_pts)
            if d == "SHORT":
                sl = min(init_sl, lock_sl)
            else:
                sl = max(init_sl, lock_sl)
            r["running_sl_source"] = "Profit Lock"
            r["running_sl_locked_rupees"] = round(lock_rs, 2)
        else:
            sl = init_sl
            r["running_sl_source"] = "Initial (1.8x ATR)"
            r["running_sl_locked_rupees"] = 0.0
        r["running_sl_price"] = round(float(sl), 4)


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
    # Giveback hard-exit: once peak open PnL is meaningful, breach if giveback exceeds threshold.
    giveback_pct = float(getattr(settings, "DAILY_FUTURES_GIVEBACK_EXIT_PCT", 0.30) or 0.30)
    giveback_abs_floor = float(getattr(settings, "DAILY_FUTURES_GIVEBACK_EXIT_RUPEES_FLOOR", 2000.0) or 2000.0)
    giveback_min_peak = float(getattr(settings, "DAILY_FUTURES_GIVEBACK_MIN_PEAK_RUPEES", 3000.0) or 3000.0)
    stock_candle_cache: Dict[str, List[Dict[str, Any]]] = {}
    prev_state: Dict[int, Dict[str, Any]] = {}
    try:
        tids = [int(r.get("trade_id") or 0) for r in running if int(r.get("trade_id") or 0) > 0]
        if tids:
            rs = db.execute(
                text(
                    """
                    SELECT id, bearish_signal_count, bearish_conditions_active, indicator_decision, last_indicator_candle_ts
                    FROM daily_futures_user_trade
                    WHERE id = ANY(:ids)
                    """
                ),
                {"ids": tids},
            ).mappings().all()
            prev_state = {int(x["id"]): dict(x) for x in rs}
    except Exception as e:
        logger.debug("daily_futures: previous indicator state load failed: %s", e)

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
        lot = _safe_float(r.get("lot_size"))
        current_unrealized_rs: Optional[float] = None
        if lot is not None and lot > 0 and ep is not None and ltp is not None:
            if dirt == "SHORT":
                current_unrealized_rs = (float(ep) - float(ltp)) * float(lot)
            else:
                current_unrealized_rs = (float(ltp) - float(ep)) * float(lot)
        old_peak = _safe_float(r.get("peak_unrealized_pnl_rupees"))
        new_peak = old_peak
        if current_unrealized_rs is not None:
            if new_peak is None:
                new_peak = float(current_unrealized_rs)
            else:
                new_peak = max(float(new_peak), float(current_unrealized_rs))
        old_giveback = bool(r.get("profit_giveback_breach"))
        new_giveback = False
        if (
            new_peak is not None
            and current_unrealized_rs is not None
            and float(new_peak) >= max(0.0, giveback_min_peak)
        ):
            giveback = float(new_peak) - float(current_unrealized_rs)
            giveback_thr = max(float(giveback_abs_floor), float(new_peak) * float(giveback_pct))
            new_giveback = giveback >= giveback_thr
        merged_giveback = old_giveback or new_giveback

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

        exit_review = bool(merged_hit or (merged_n and merged_m) or drawdown_15atr_breach or merged_giveback)

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
        elif merged_giveback:
            as_dec = "giveback_exit"
        elif l1_amber and l3_fading:
            as_dec = "dual_exit"
        elif l1_amber or l3_fading:
            as_dec = "watch"
        else:
            as_dec = "hold"

        ind = {"count": 0, "conditions": [], "conditions_text": [], "latest_candle_ts": None}
        if ikey:
            try:
                candles_15 = _last_completed_15m_candles_for_indicator(
                    upstox, ikey, trade_date, now_ist, limit=30
                )
                ind = _evaluate_indicator_exit_signal(ikey, dirt, candles_15)
            except Exception as e:
                logger.debug("daily_futures: indicator eval failed %s: %s", ikey, e)
        ind_count_new = int(ind.get("count") or 0)
        ind_bull_new = int(ind.get("bullish_count") or 0)
        ind_bear_new = int(ind.get("bearish_count") or 0)
        ind_conds_new = list(ind.get("conditions") or [])
        ind_text_new = list(ind.get("conditions_text") or [])
        ind_latest_ts = ind.get("latest_candle_ts")
        # Fixed thresholds by product decision (do not override from env):
        # Exit Now at 2; Hard Exit at 3.
        exit_now_thr = 2
        hard_exit_thr = 3
        exit_now_thr = max(1, exit_now_thr)
        hard_exit_thr = max(exit_now_thr, hard_exit_thr)
        prev = prev_state.get(tid) or {}
        prev_count = int(prev.get("bearish_signal_count") or 0)
        prev_codes = [x.strip() for x in str(prev.get("bearish_conditions_active") or "").split(",") if x and str(x).strip()]
        prev_dec = str(prev.get("indicator_decision") or "hold").strip().lower()
        prev_candle_dt = prev.get("last_indicator_candle_ts")
        new_candle_dt = _parse_iso_ist(ind_latest_ts) if ind_latest_ts else None
        same_or_older_candle = bool(
            new_candle_dt is None
            or (prev_candle_dt is not None and isinstance(prev_candle_dt, datetime) and new_candle_dt <= prev_candle_dt)
        )
        if same_or_older_candle:
            ind_count = prev_count
            ind_bull = prev_count if dirt != "SHORT" else 0
            ind_bear = prev_count if dirt == "SHORT" else 0
            ind_conds = prev_codes
            txt_long = {"C1": "MACD bearish", "C2": "DI- crossed above DI+", "C3": "Hilega-Milega flipped", "C4": "OBV broke SMA10"}
            txt_short = {"C1": "MACD bullish", "C2": "DI+ crossed above DI-", "C3": "Hilega-Milega flipped bullish", "C4": "OBV crossed above SMA10"}
            label_map = txt_short if dirt == "SHORT" else txt_long
            ind_text = [label_map[c] for c in ind_conds if c in label_map]
            as_dec = prev_dec if prev_dec in {"hold", "exit_now", "hard_exit"} else "hold"
        else:
            ind_count = ind_count_new
            ind_bull = ind_bull_new
            ind_bear = ind_bear_new
            ind_conds = ind_conds_new
            ind_text = ind_text_new
            if ind_count >= hard_exit_thr:
                as_dec = "hard_exit"
            elif ind_count >= exit_now_thr:
                as_dec = "exit_now"
            else:
                as_dec = "hold"

        r["alert_strip"] = {
            "l1": nifty_l1_state,
            "l2": l2k,
            "l3": "fading" if l3_fading else "strong",
            "decision": as_dec,
            "indicator_count": ind_count,
            "indicator_bullish_count": ind_bull,
            "indicator_bearish_count": ind_bear,
            "indicator_conditions": ind_conds,
            "indicator_conditions_text": ind_text,
            "indicator_latest_candle_ts": ind_latest_ts,
            "indicator_exit_now_threshold": exit_now_thr,
            "indicator_hard_exit_threshold": hard_exit_thr,
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
                      peak_unrealized_pnl_rupees = COALESCE(CAST(:peak_rs AS NUMERIC), peak_unrealized_pnl_rupees),
                      profit_giveback_breach = CAST(:pgb AS BOOLEAN),
                      bearish_signal_count = CAST(:bcount AS INTEGER),
                      bearish_conditions_active = :bconds,
                      last_bearish_evaluated_at = CAST(:beval AS TIMESTAMPTZ),
                      indicator_decision = :idec,
                      last_indicator_candle_ts = COALESCE(CAST(:icts AS TIMESTAMPTZ), last_indicator_candle_ts),
                      first_amber_alert_at = CASE
                        WHEN CAST(:bcount AS INTEGER) >= CAST(:exit_now_thr AS INTEGER) AND first_amber_alert_at IS NULL THEN CURRENT_TIMESTAMP
                        ELSE first_amber_alert_at
                      END,
                      first_hard_exit_alert_at = CASE
                        WHEN CAST(:bcount AS INTEGER) >= CAST(:hard_exit_thr AS INTEGER) AND first_hard_exit_alert_at IS NULL THEN CURRENT_TIMESTAMP
                        ELSE first_hard_exit_alert_at
                      END,
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
                    "peak_rs": float(new_peak) if new_peak is not None else None,
                    "pgb": merged_giveback,
                    "bcount": ind_count,
                    "bconds": ",".join(ind_conds) if ind_conds else None,
                    "beval": now_ist.isoformat(),
                    "idec": as_dec,
                    "icts": ind_latest_ts,
                    "exit_now_thr": exit_now_thr,
                    "hard_exit_thr": hard_exit_thr,
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
        r["peak_unrealized_pnl_rupees"] = round(float(new_peak), 2) if new_peak is not None else None
        r["profit_giveback_breach"] = merged_giveback
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


def _apply_sector_mover_badges(rows: List[Dict[str, Any]]) -> None:
    """
    Annotate picks with Dashboard Top Gainers/Losers sector ranks (same source as dashboard.html).
    - sector_in_top_gainers_rank: 1–3 when underlying's Nifty sector is in top 3 movers.
    - sector_in_top_losers_rank: 1–3 when in bottom 3 movers.
    """
    if not rows:
        return
    try:
        from backend.services.sector_movers import get_sector_movers_cached, nifty_sector_label_for_nse_equity
    except Exception:
        return
    try:
        mv = get_sector_movers_cached(top_n=3)
        gsectors = [r.get("sector") for r in (mv.get("gainers") or []) if r.get("sector")]
        lsectors = [r.get("sector") for r in (mv.get("losers") or []) if r.get("sector")]
        gmap = {lbl: idx + 1 for idx, lbl in enumerate(gsectors[:3])}
        lmap = {lbl: idx + 1 for idx, lbl in enumerate(lsectors[:3])}
    except Exception as e:
        logger.debug("daily_futures: sector_movers_cached for badges failed: %s", e)
        return
    for p in rows:
        u = str(p.get("underlying") or "").strip().upper()
        lbl = nifty_sector_label_for_nse_equity(u)
        p["nifty_sector_label"] = lbl
        p["sector_in_top_gainers_rank"] = gmap.get(lbl) if lbl else None
        p["sector_in_top_losers_rank"] = lmap.get(lbl) if lbl else None


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


def _canonical_chartink_key_df(raw_key: str) -> str:
    """Map ChartInk / client synonyms to stable names (mirror scan router loosely)."""
    k = str(raw_key).strip().lower().replace("-", "_")
    return {
        "stock": "stocks",
        "symbols": "stocks",
        "symbol": "stocks",
        "tickers": "stocks",
        "ticker": "stocks",
        "scr": "stocks",
        "nsecode": "stocks",
    }.get(k, k)


def _normalize_chartink_like_dict_df(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Merge synonym keys so ``normalize_symbols_from_payload`` sees ``stocks`` etc."""
    if not raw:
        return {}
    out: Dict[str, Any] = {}
    for key, val in raw.items():
        if not isinstance(key, str):
            continue
        ck = _canonical_chartink_key_df(key)
        if ck in ("stocks", "trigger_prices") and ck in out and out[ck] not in (None, "") and val not in (None, ""):
            out[ck] = f"{out[ck]},{val}"
        else:
            out[ck] = val
    if isinstance(out.get("stocks"), list):
        out["stocks"] = ",".join(str(x).strip() for x in out["stocks"] if str(x).strip())
    return out


def parse_daily_futures_chartink_webhook_body(
    raw: bytes, content_type: Optional[str] = None
) -> Any:
    """
    Decode Daily Futures webhook body the way ChartInk often sends it:
    - JSON
    - application/x-www-form-urlencoded (field ``stocks`` / ``symbol`` / ``stock``, etc.)

    Previously only JSON was decoded; URL-encoded POSTs stayed as ``stocks=X&…`` strings and
    ``normalize_symbols_from_payload`` produced zero symbols → 400 and nothing ingested.

    Note: multipart/form-data is parsed in the router via ``request.form()`` (do not ``body()`` first).
    """
    ct = (content_type or "").lower().strip()
    if not raw or not raw.strip():
        raise ValueError("empty body")
    if "application/json" in ct:
        return json.loads(raw.decode("utf-8", errors="replace"))
    if "application/x-www-form-urlencoded" in ct:
        qs = parse_qs(raw.decode("utf-8", errors="replace"), keep_blank_values=True)
        flat: Dict[str, Any] = {}
        for k, vals in qs.items():
            if not isinstance(k, str):
                continue
            ck = _canonical_chartink_key_df(k)
            part = ",".join(str(v).strip() for v in vals if v is not None and str(v).strip())
            if not part:
                continue
            if ck in flat and flat[ck]:
                flat[ck] = f"{flat[ck]},{part}"
            else:
                flat[ck] = part
        return _normalize_chartink_like_dict_df(flat)
    if "multipart/form-data" in ct:
        raise ValueError("multipart body must be parsed with request.form() in the webhook handler")
    # Content-Type missing or exotic: attempt JSON literal, then raw string (comma/split fallback)
    s = raw.decode("utf-8", errors="replace").strip()
    if s.startswith("{") or s.startswith("["):
        return json.loads(s)
    return s


def df_chartink_accum_init_from_query(multi_items: Any) -> Dict[str, List[str]]:
    """Collect repeat query keys into lists (stocks merges). ``multi_items`` from ``request.query_params.multi_items()``."""
    acc: Dict[str, List[str]] = {}
    for rk, rv in multi_items or ():
        if not isinstance(rk, str):
            continue
        v = str(rv).strip()
        if not v:
            continue
        ck = _canonical_chartink_key_df(rk)
        acc.setdefault(ck, []).append(v)
    return acc


def df_chartink_ingest_parsed_into_accum(acc: Dict[str, List[str]], obj: Any) -> None:
    """Merge body parse result into accum (dict JSON, list, CSV string)."""
    if obj is None:
        return
    if isinstance(obj, dict):
        for rk, rv in obj.items():
            if not isinstance(rk, str):
                continue
            if isinstance(rv, list):
                for x in rv:
                    v = str(x).strip()
                    if v:
                        ck = _canonical_chartink_key_df(rk)
                        acc.setdefault(ck, []).append(v)
            elif rv is not None:
                v = str(rv).strip()
                if v:
                    ck = _canonical_chartink_key_df(rk)
                    acc.setdefault(ck, []).append(v)
        return
    if isinstance(obj, list):
        for x in obj:
            v = str(x).strip()
            if v:
                acc.setdefault("stocks", []).append(v)
        return
    if isinstance(obj, str):
        for part in re.split(r"[\s,;\n]+", obj.strip()):
            if part.strip():
                acc.setdefault("stocks", []).append(part.strip())


def df_chartink_accum_finalize_payload(acc: Dict[str, List[str]]) -> Dict[str, Any]:
    if not acc:
        return {}
    flat: Dict[str, str] = {}
    for ck, lst in acc.items():
        if not lst:
            continue
        flat[ck] = ",".join(lst) if len(lst) > 1 else lst[0]
    return _normalize_chartink_like_dict_df(flat)


def df_chartink_accum_extend_from_starlette_form(
    acc: Dict[str, List[str]], form_data: Any
) -> Dict[str, str]:
    """
    Merge ChartInk multipart fields into accum. UploadFile parts are skipped (not used for DF symbols).
    Returns a short summary for audit JSON (filename marker only).
    """
    summary: Dict[str, str] = {}
    try:
        items = form_data.multi_items()
    except Exception:
        items = ()

    for k, v in items:
        key = str(k) if isinstance(k, str) else str(k or "")
        if not key.strip():
            continue
        if isinstance(v, UploadFile):
            fn = getattr(v, "filename", None)
            summary[key] = f"<UploadFile:{fn or ''}>"
            continue
        try:
            s = str(v).strip()
        except Exception:
            continue
        if not s:
            continue
        ck = _canonical_chartink_key_df(key)
        acc.setdefault(ck, []).append(s)
        summary[key] = s

    return summary


def df_chartink_audit_json_bytes(
    *,
    method: str,
    content_type: str,
    query_multi_items: Optional[List[Tuple[str, str]]] = None,
    multipart_field_summary: Optional[Dict[str, str]] = None,
    raw_body_note: Optional[str] = None,
) -> bytes:
    """Structured audit line for inbox when body is multipart or GET (no raw POST bytes)."""
    doc: Dict[str, Any] = {
        "df_chartink_audit": True,
        "method": method,
        "query_params": list(query_multi_items or ()),
        "content_type": content_type or None,
    }
    if multipart_field_summary is not None:
        doc["form_fields"] = multipart_field_summary
    if raw_body_note:
        doc["body_note"] = raw_body_note
    return json.dumps(doc, ensure_ascii=False, sort_keys=True).encode("utf-8")


def normalize_symbols_from_payload(payload: Any) -> List[str]:
    out: List[str] = []
    if payload is None:
        return out
    if isinstance(payload, dict):
        payload = _normalize_chartink_like_dict_df(payload)
    if isinstance(payload, str):
        return sorted({s.strip().upper() for s in re.split(r"[\s,;\n]+", payload) if s.strip()})
    if isinstance(payload, list):
        return sorted({str(x).strip().upper() for x in payload if str(x).strip()})
    if isinstance(payload, dict):
        for key in ("symbols", "symbol", "stock", "stocks", "tickers", "ticker", "data", "alert_symbols"):
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

    def _norm_key(k: str) -> str:
        return str(k or "").replace(":", "|").replace(" ", "").upper()

    def _resolve_quote_snapshot(req_key: str) -> Dict[str, Any]:
        """
        Resolve market-quote snapshot robustly for a requested instrument key.
        Batch snapshot can miss some NSE_FO keys; fallback to single quote by key.
        """
        if req_key in batch_quotes and isinstance(batch_quotes.get(req_key), dict):
            return batch_quotes.get(req_key) or {}
        nk = _norm_key(req_key)
        for bk, qd in (batch_quotes or {}).items():
            if isinstance(qd, dict) and _norm_key(bk) == nk:
                return qd
        try:
            sq = upstox.get_market_quote_by_key(req_key) or {}
            return sq if isinstance(sq, dict) else {}
        except Exception as qe:
            logger.debug("daily_futures: single quote fallback failed for %s: %s", req_key, qe)
            return {}

    prev_close_cache = _get_or_init_prev_close_cache(trade_date, upstox, symbol_to_key)
    nifty_quote = _resolve_quote_snapshot(NIFTY50_INDEX_KEY)
    nifty_ltp = _safe_float(nifty_quote.get("last_price"))
    nifty_vwap = _session_vwap_for_conviction(nifty_quote)
    nifty_prev_close = _safe_float((prev_close_cache.get("nifty")))
    nifty_change_pct = (
        round(((nifty_ltp - nifty_prev_close) / nifty_prev_close) * 100.0, 6)
        if nifty_ltp is not None and nifty_prev_close and nifty_prev_close > 0
        else None
    )

    ingest_now = datetime.now(IST)
    decay_raw = os.getenv(
        "DAILY_FUTURES_CONVICTION_DECAY_PER_SCAN",
        str(getattr(settings, "DAILY_FUTURES_CONVICTION_DECAY_PER_SCAN", 0.08) or 0.08),
    )
    decay_per_scan = float(decay_raw or 0.08)
    decay_per_scan = max(0.0, min(0.5, decay_per_scan))
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
            q = _resolve_quote_snapshot(ik)
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
                           second_scan_conviction_score, second_scan_oi_leg, second_scan_vwap_leg,
                           second_scan_stock_change_pct, second_scan_nifty_change_pct,
                           candle_is_green, candle_higher_high, candle_higher_low,
                           qualifying_scan_streak, entry_window_start, entry_window_end
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
                second_scan_oi_leg = ex[5]
                second_scan_vwap_leg = ex[6]
                second_scan_stock = ex[7]
                second_scan_nifty = ex[8]
                row_candle_is_green = bool(ex[9]) if ex[9] is not None else bool(x.get("candle_is_green"))
                row_candle_higher_high = bool(ex[10]) if ex[10] is not None else bool(x.get("candle_higher_high"))
                row_candle_higher_low = bool(ex[11]) if ex[11] is not None else bool(x.get("candle_higher_low"))
                qualifying_streak = int(ex[12] or 0)
                entry_window_start = ex[13]
                entry_window_end = ex[14]
                next_scan_count = prior_scan_count + 1
                if prior_scan_count < 1:
                    next_scan_count = 1
                if next_scan_count >= 2 and second_scan_time is None:
                    second_scan_time = ingest_now
                    second_scan_conv = x["conviction_score"]
                    second_scan_oi_leg = x["conviction_oi_leg"]
                    second_scan_vwap_leg = x["conviction_vwap_leg"]
                    second_scan_stock = x["stock_change_pct"]
                    second_scan_nifty = x["nifty_change_pct"]
                cscore = float(x.get("conviction_score") or 0.0)
                live_decay_factor = max(0.5, 1.0 - max(0, next_scan_count - 1) * decay_per_scan)
                effective_now = round(cscore * float(live_decay_factor), 1)
                if effective_now >= 60.0:
                    qualifying_streak = qualifying_streak + 1
                else:
                    qualifying_streak = 0
                if qualifying_streak >= 2 and entry_window_start is None:
                    entry_window_start = ingest_now + timedelta(minutes=5)
                    entry_window_end = entry_window_start + timedelta(minutes=15)
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
                          second_scan_oi_leg = COALESCE(:ss_oi_leg, second_scan_oi_leg),
                          second_scan_vwap_leg = COALESCE(:ss_vwap_leg, second_scan_vwap_leg),
                          second_scan_stock_change_pct = :ss_stock,
                          second_scan_nifty_change_pct = :ss_nifty,
                          qualifying_scan_streak = :qss,
                          entry_window_start = :ews,
                          entry_window_end = :ewe,
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
                        "ss_oi_leg": second_scan_oi_leg if second_scan_oi_leg is not None else x["conviction_oi_leg"],
                        "ss_vwap_leg": second_scan_vwap_leg if second_scan_vwap_leg is not None else x["conviction_vwap_leg"],
                        "ss_stock": second_scan_stock,
                        "ss_nifty": second_scan_nifty,
                        "qss": qualifying_streak,
                        "ews": entry_window_start,
                        "ewe": entry_window_end,
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
                          stock_change_pct, nifty_change_pct, snapshotted_at, qualifying_scan_streak,
                          candle_is_green, candle_higher_high, candle_higher_low, conviction_breakdown_json
                        ) VALUES (
                          CAST(:d AS DATE), :u, :dtd, :fs, :ik, :lot, 1, :fh, :lh, :cs,
                          :oi_leg, :vw_leg, :ltp, :svwap, :toi, :oi_chg,
                          :nltp, :nsvwap, :spc, :npc, :scp, :ncp, :snap, :qss,
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
                        "qss": 1 if float(x.get("conviction_score") or 0.0) >= 60.0 else 0,
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
                   scan_count, first_hit_at, last_hit_at, conviction_score, conviction_oi_leg, conviction_vwap_leg, ltp,
                   second_scan_time, second_scan_conviction_score, second_scan_oi_leg, second_scan_vwap_leg,
                   second_scan_stock_change_pct, second_scan_nifty_change_pct,
                   stock_change_pct, nifty_change_pct, effective_conviction, last_5m_momentum_pass, last_5m_evaluated_at,
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
                "conviction_oi_leg": float(row[10]) if row[10] is not None else None,
                "conviction_vwap_leg": float(row[11]) if row[11] is not None else None,
                "ltp": float(row[12]) if row[12] is not None else None,
                "second_scan_time": row[13].isoformat() if row[13] else None,
                "second_scan_conviction_score": float(row[14]) if row[14] is not None else None,
                "second_scan_oi_leg": float(row[15]) if row[15] is not None else None,
                "second_scan_vwap_leg": float(row[16]) if row[16] is not None else None,
                "second_scan_stock_change_pct": float(row[17]) if row[17] is not None else None,
                "second_scan_nifty_change_pct": float(row[18]) if row[18] is not None else None,
                "stock_change_pct": float(row[19]) if row[19] is not None else None,
                "nifty_change_pct": float(row[20]) if row[20] is not None else None,
                "effective_conviction": float(row[21]) if row[21] is not None else None,
                "last_5m_momentum_pass": bool(row[22]) if row[22] is not None else None,
                "last_5m_evaluated_at": row[23].isoformat() if row[23] else None,
                "candle_is_green": bool(row[24]) if row[24] is not None else None,
                "candle_higher_high": bool(row[25]) if row[25] is not None else None,
                "candle_higher_low": bool(row[26]) if row[26] is not None else None,
                "conviction_breakdown_json": row[27] if row[27] is not None else None,
            }
        )
    return out


def _apply_live_ltps_to_picks_and_running(
    picks: List[Dict[str, Any]],
    running: List[Dict[str, Any]],
    closed: Optional[List[Dict[str, Any]]] = None,
    *,
    persist_screening: bool = True,
) -> None:
    """
    Refresh LTP from Upstox for every row (batch + per-key fallback), update dicts in place.
    Optionally persist ltp on daily_futures_screening (skipped for lite workspace to save many UPDATEs).
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

    if not by_screening or not persist_screening:
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
    *,
    persist_screening: bool = True,
    snapshot_baselines_only: bool = False,
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

    def _norm_key(k: str) -> str:
        return str(k or "").replace(":", "|").replace(" ", "").upper()

    def _ltp_for_instrument(ik: str) -> Optional[float]:
        if not ik or not ltp_by_key:
            return None
        if ik in ltp_by_key:
            v = ltp_by_key[ik]
            if v and float(v) > 0:
                return float(v)
        nk = _norm_key(ik)
        for bk, lp in ltp_by_key.items():
            if not bk or not lp:
                continue
            if _norm_key(bk) == nk and float(lp) > 0:
                return float(lp)
        return None

    # Keep workspace responsive if batch snapshot misses many keys.
    # Single-quote fallback is cached and budgeted.
    snapshot_fallback_cache: Dict[str, Dict[str, Any]] = {}
    snapshot_fallback_budget = max(6, min(14, len(combined) // 2))
    snapshot_fallback_used = 0

    def _snapshot_for_instrument(ik: str) -> Dict[str, Any]:
        nonlocal snapshot_fallback_used
        if not ik:
            return {}
        if ik in snap_by_key and isinstance(snap_by_key.get(ik), dict):
            return snap_by_key.get(ik) or {}
        nk = _norm_key(ik)
        for bk, sv in snap_by_key.items():
            if isinstance(sv, dict) and _norm_key(bk) == nk:
                return sv
        if ik in snapshot_fallback_cache:
            return snapshot_fallback_cache.get(ik) or {}
        if snapshot_fallback_used >= snapshot_fallback_budget:
            return {}
        # Batch snapshot can miss NSE_FO|id keys; single quote by key reliably returns OHLC/open/net_change.
        try:
            snapshot_fallback_used += 1
            sq = upstox.get_market_quote_by_key(ik) or {}
            out = sq if isinstance(sq, dict) else {}
            snapshot_fallback_cache[ik] = out
            return out
        except Exception:
            snapshot_fallback_cache[ik] = {}
            return {}

    nifty_ltp = _ltp_for_instrument(NIFTY50_INDEX_KEY)
    if nifty_ltp is None:
        try:
            q = upstox.get_market_quote_by_key(NIFTY50_INDEX_KEY) or {}
            lp = _safe_float(q.get("last_price"))
            if lp and lp > 0:
                nifty_ltp = lp
        except Exception as e:
            logger.debug("daily_futures: rel-strength Nifty single quote failed: %s", e)

    nifty_snap = _snapshot_for_instrument(NIFTY50_INDEX_KEY)
    # Lite workspace (snapshot_baselines_only): avoid one historical-candles HTTP call per
    # underlying — this was dominating load time (>30s) with many Today's pick symbols.
    if snapshot_baselines_only:
        nifty_prev = _quote_session_open_from_snapshot(nifty_snap)
        if nifty_prev is None:
            nifty_prev = _prev_close_from_snapshot(nifty_snap)
    else:
        nifty_prev = _prev_15m_close_for_instrument(upstox, NIFTY50_INDEX_KEY, now_ist, prev15_cache)
        if nifty_prev is None:
            nifty_prev = _quote_session_open_from_snapshot(nifty_snap)
        if nifty_prev is None:
            nifty_prev = _prev_close_from_snapshot(nifty_snap)
    nifty_change_pct: Optional[float] = None
    if nifty_ltp is not None and nifty_prev and nifty_prev > 0:
        nifty_change_pct = round(((nifty_ltp - nifty_prev) / nifty_prev) * 100.0, 6)

    by_screening: Dict[int, Tuple[Optional[float], Optional[float]]] = {}
    for r in combined:
        u = str(r.get("underlying") or "").strip().upper()
        if not u:
            continue
        ik = str(r.get("instrument_key") or "").strip()
        stock_snap = _snapshot_for_instrument(ik)
        if snapshot_baselines_only:
            stock_prev = _quote_session_open_from_snapshot(stock_snap)
            if stock_prev is None:
                stock_prev = _prev_close_from_snapshot(stock_snap)
        else:
            stock_prev = _prev_15m_close_for_instrument(upstox, ik, now_ist, prev15_cache)
            if stock_prev is None:
                stock_prev = _quote_session_open_from_snapshot(stock_snap)
            if stock_prev is None:
                stock_prev = _prev_close_from_snapshot(stock_snap)
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
    if not by_screening or not persist_screening:
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


def _upstox_fetch_5m_structured_rows(
    ux: UpstoxService,
    instrument_key: str,
    td: date,
    *,
    log_label: str = "",
) -> List[Dict[str, Any]]:
    """
    Today's session: prefer V3 intraday (reliable spot/index 5m) over V2 1m→5m aggregation.
    Other dates: historical only (intraday applies to current calendar session on vendor side).
    """
    ik = str(instrument_key or "").strip()
    if not ik:
        return []

    rows: List[Dict[str, Any]] = []
    prefix = log_label.strip() + ": " if log_label else ""

    if td == ist_today():
        try:
            key_enc = quote(ik, safe="")
            intraday_url = f"{ux.base_url}/historical-candle/intraday/{key_enc}/minutes/5"
            raw_i = ux.make_api_request(intraday_url, method="GET", timeout=20, max_retries=2) or {}
            if isinstance(raw_i, dict) and raw_i.get("status") == "success":
                rows = _candles_rows_to_structured(((raw_i.get("data") or {}).get("candles")) or []) or []
            elif isinstance(raw_i, dict):
                logger.warning(
                    "%sdaily_futures: intraday 5m HTTP ok but status!=success ik=%s body=%s",
                    prefix,
                    ik,
                    (raw_i.get("message") or raw_i.get("error") or str(raw_i))[:300],
                )
        except Exception as e:
            logger.debug("%sdaily_futures: intraday 5m fetch failed ik=%s: %s", prefix, ik, e)

    if not rows:
        try:
            rows = ux.get_historical_candles_by_instrument_key(
                ik, interval="minutes/5", days_back=3, range_end_date=td
            ) or []
        except Exception as e:
            logger.debug("%sdaily_futures: historical 5m fetch failed ik=%s: %s", prefix, ik, e)
            rows = []

    return list(rows or [])


def _bearish_gate_warming_window_ist(now_ist: datetime, td: date) -> bool:
    """
    Fewer than three completed 5m bars is expected before spot regular session settles
    (NSE cash ~09:15; third completed 5m bar ends ~09:30 IST).
    """
    if not _is_trading_day(now_ist.date()) or td != now_ist.date():
        return False
    hm = now_ist.hour * 60 + now_ist.minute
    if hm < 9 * 60 + 15:
        return True
    # Small buffer past 09:30 for API lag
    return hm < 9 * 60 + 35


def index_bearish_gate_from_quotes() -> Dict[str, Any]:
    """
    NIFTY-only gate for bearish (5m structure):
    - last two completed 5-minute candles must be red (close < open)
    - closes of last three completed 5-minute candles must be strictly descending
      (c[-3].close > c[-2].close > c[-1].close)
    Used for Today's pick — Bearish visibility and SHORT order entry.
    """
    out: Dict[str, Any] = {
        "ok": False,
        "nifty_ltp": None,
        "nifty_open": None,
        "nifty_bearish": False,
        "nifty_bullish": False,
        "last_two_red_5m": False,
        "last_three_closes_desc_5m": False,
    }
    try:
        ux = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    except Exception as e:
        out["error"] = str(e)
        return out

    # Keep quote fields for UI metadata, but gating is driven by 5m candle structure.
    try:
        b = ux.get_market_quote_snapshots_batch([NIFTY50_INDEX_KEY]) or {}
        nq = b.get(NIFTY50_INDEX_KEY) or {}
        out["nifty_ltp"] = _safe_float(nq.get("last_price"))
        out["nifty_open"] = _quote_session_open_from_snapshot(nq)
    except Exception:
        pass

    td = _workspace_trade_date_ist()
    now_ist = datetime.now(IST)
    cutoff = now_ist.replace(minute=(now_ist.minute // 5) * 5, second=0, microsecond=0)

    rows = _upstox_fetch_5m_structured_rows(
        ux,
        NIFTY50_INDEX_KEY,
        td,
        log_label="index_bearish_gate",
    )

    cands: List[Dict[str, Any]] = []
    for c in rows:
        ts = _parse_iso_ist((c or {}).get("timestamp"))
        if ts is None or ts.date() != td or ts >= cutoff:
            continue
        op = _safe_float((c or {}).get("open"))
        cl = _safe_float((c or {}).get("close"))
        if op is None or cl is None:
            continue
        cands.append({"timestamp": ts, "open": op, "close": cl})
    cands.sort(key=lambda x: x["timestamp"])
    out["five_m_completed_count_for_gate"] = len(cands)

    if len(cands) < 3:
        out["nifty_quote_incomplete"] = True
        out["warming_up"] = bool(_bearish_gate_warming_window_ist(now_ist, td))
        if (
            out["warming_up"]
            and td == ist_today()
            and isinstance(rows, list)
            and len(rows) > 0
            and len(cands) == 0
        ):
            hm_now = now_ist.hour * 60 + now_ist.minute
            if hm_now >= 9 * 60 + 35:
                out["warming_up"] = False
                logger.warning(
                    "daily_futures: NIFTY gate raw 5m rows=%s but zero session-completed candles after warmup window "
                    "(td=%s now=%s IST)",
                    len(rows),
                    td,
                    now_ist.strftime("%H:%M"),
                )
        if not out["warming_up"] and len(cands) < 3 and not out.get("error"):
            logger.warning(
                "daily_futures: NIFTY gate incomplete after warmup window — completed_5m=%s td=%s (check Upstox 5m for %s)",
                len(cands),
                td,
                NIFTY50_INDEX_KEY,
            )
        return out

    c3, c2, c1 = cands[-3], cands[-2], cands[-1]
    last_two_red = bool(float(c2["close"]) < float(c2["open"]) and float(c1["close"]) < float(c1["open"]))
    last_three_desc = bool(float(c3["close"]) > float(c2["close"]) > float(c1["close"]))
    bearish = bool(last_two_red and last_three_desc)
    out["last_two_red_5m"] = last_two_red
    out["last_three_closes_desc_5m"] = last_three_desc
    out["nifty_bearish"] = bearish
    out["nifty_bullish"] = not bearish
    out["ok"] = bearish
    return out


def get_workspace(db: Session, user_id: int, lite_mode: bool = False) -> Dict[str, Any]:
    ensure_daily_futures_tables()
    td = _workspace_trade_date_ist()
    now_ist = datetime.now(IST)

    with engine.connect() as conn:
        screenings = _fetch_screening_dicts(conn, td)
    _compute_effective_conviction_and_5m_momentum(screenings, now_ist)

    br = db.execute(
        text(
            """
            SELECT t.screening_id
            FROM daily_futures_user_trade t
            JOIN daily_futures_screening s ON s.id = t.screening_id
            WHERE t.user_id = :u
              AND t.order_status = 'bought'
            """
        ),
        {"u": user_id},
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
                   t.trail_stop_hit, t.momentum_exhausting, t.peak_unrealized_pnl_rupees, t.profit_giveback_breach,
                   s.scan_count, s.first_hit_at, s.last_hit_at, s.conviction_score,
                   s.second_scan_conviction_score, s.second_scan_oi_leg, s.second_scan_vwap_leg, s.ltp,
                   s.stock_change_pct, s.nifty_change_pct,
                   s.conviction_oi_leg, s.conviction_vwap_leg, s.session_vwap, s.conviction_breakdown_json,
                   s.effective_conviction,
                   s.trade_date
            FROM daily_futures_user_trade t
            JOIN daily_futures_screening s ON s.id = t.screening_id
            WHERE t.user_id = :u
              AND t.order_status = 'bought'
            ORDER BY t.updated_at DESC
            """
        ),
        {"u": user_id},
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
                "peak_unrealized_pnl_rupees": float(row[18]) if row[18] is not None else None,
                "profit_giveback_breach": bool(row[19]) if row[19] is not None else False,
                "scan_count": int(row[20] or 0),
                "first_hit_at": row[21].isoformat() if row[21] else None,
                "last_hit_at": row[22].isoformat() if row[22] else None,
                "conviction_score": float(row[23]) if row[23] is not None else None,
                "second_scan_conviction_score": float(row[24]) if row[24] is not None else None,
                "second_scan_oi_leg": float(row[25]) if row[25] is not None else None,
                "second_scan_vwap_leg": float(row[26]) if row[26] is not None else None,
                "ltp": float(row[27]) if row[27] is not None else None,
                "stock_change_pct": float(row[28]) if row[28] is not None else None,
                "nifty_change_pct": float(row[29]) if row[29] is not None else None,
                "conviction_oi_leg": float(row[30]) if row[30] is not None else None,
                "conviction_vwap_leg": float(row[31]) if row[31] is not None else None,
                "session_vwap": float(row[32]) if row[32] is not None else None,
                "conviction_breakdown_json": row[33] if len(row) > 33 and row[33] is not None else None,
                "effective_conviction": float(row[34]) if len(row) > 34 and row[34] is not None else None,
                "trade_date": str(row[35]) if len(row) > 35 and row[35] is not None else str(td),
                "warn_two_misses": miss >= 2,
            }
        )

    closed_rows = db.execute(
        text(
            """
            SELECT t.id, t.screening_id, t.underlying, COALESCE(t.direction_type, s.direction_type, 'LONG') AS direction_type, t.future_symbol, t.instrument_key, t.lot_size,
                   t.entry_time, t.entry_price, t.sell_time, t.sell_price, t.exit_time, t.exit_price, t.buy_time, t.buy_price,
                   t.pnl_points, t.pnl_rupees,
                   s.entry_window_start, s.entry_window_end,
                   s.first_hit_at,
                   s.ltp
            FROM daily_futures_user_trade t
            JOIN daily_futures_screening s ON s.id = t.screening_id
            WHERE t.user_id = :u
              AND t.order_status = 'sold'
              AND (
                    s.trade_date = CAST(:td AS DATE)
                    OR (
                        s.trade_date < CAST(:td AS DATE)
                        AND DATE((COALESCE(t.updated_at, t.created_at) AT TIME ZONE 'Asia/Kolkata')) = CAST(:td AS DATE)
                    )
                  )
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
                "entry_window_start": row[17].isoformat() if row[17] is not None and hasattr(row[17], "isoformat") else None,
                "entry_window_end": row[18].isoformat() if row[18] is not None and hasattr(row[18], "isoformat") else None,
                "pnl_points": pnl_pts,
                "pnl_rupees": pnl_rs,
                "first_scan_time": row[19].isoformat() if row[19] is not None and hasattr(row[19], "isoformat") else None,
                "ltp": float(row[20]) if row[20] is not None else None,
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
    # With index gate: NIFTY 5m bearish structure gate. Without gate: show all bearish ≥50.
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
            _apply_live_ltps_to_picks_and_running(_picks_for_quotes, [], [], persist_screening=False)
        else:
            _apply_live_ltps_to_picks_and_running(_picks_for_quotes, running, closed, persist_screening=True)
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
            _apply_live_rel_strength_to_picks_and_running(
                _picks_for_quotes,
                [],
                td,
                persist_screening=False,
                snapshot_baselines_only=True,
            )
        else:
            _apply_live_rel_strength_to_picks_and_running(
                _picks_for_quotes,
                running,
                td,
                persist_screening=True,
                snapshot_baselines_only=False,
            )
    except Exception as e:
        logger.warning("daily_futures: rel-strength refresh failed: %s", e, exc_info=True)

    _apply_running_sl_ladder(running)

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
        second_scan_dt = _parse_iso_ist(p.get("second_scan_time"))
        if second_scan_dt is None:
            reasons.append("Second scan time unavailable")
        else:
            if now_ist < (second_scan_dt + timedelta(minutes=5)):
                reasons.append("Wait 5 minutes after second scan")
        live_conv = p.get("effective_conviction")
        c2 = live_conv
        if c2 is None:
            reasons.append("Effective conviction unavailable")
        elif dtp == "SHORT":
            if float(c2) <= 50.0:
                reasons.append(f"Effective conviction {round(float(c2),1)} is not above 50")
            if _bearish_index_gate_enabled() and not bool(index_bear.get("ok")):
                reasons.append("NIFTY 5m bearish structure is not confirmed (no SHORT from this list)")
        else:
            if float(live_conv) < 60.0:
                reasons.append(f"Effective conviction {round(float(live_conv),1)} is below 60")
        p["order_eligible"] = len(reasons) == 0
        p["order_block_reason"] = reasons[0] if reasons else None

    try:
        _apply_sector_mover_badges(
            list(picks_mixed) + list(picks_low_conv_bull) + list(picks_low_conv_bear)
        )
    except Exception as e:
        logger.debug("daily_futures: sector mover badges skipped: %s", e)

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
                   conviction_score, second_scan_conviction_score, second_scan_time,
                   effective_conviction
            FROM daily_futures_screening WHERE id = :sid AND trade_date = CAST(:d AS DATE)
            """
        ),
        {"sid": screening_id, "d": str(ist_today())},
    ).fetchone()
    if not row:
        raise ValueError("Screening row not found for today")
    if int(row[6] or 0) < 2:
        raise ValueError("Needs at least 2 consecutive scans before order")
    second_scan_time = row[9]
    if second_scan_time is None:
        raise ValueError("Second scan time unavailable")
    ss_dt = second_scan_time.astimezone(IST) if getattr(second_scan_time, "tzinfo", None) else IST.localize(second_scan_time)
    if datetime.now(IST) < (ss_dt + timedelta(minutes=5)):
        raise ValueError("Wait 5 minutes after second scan")
    dtp = str(row[2] or "LONG").strip().upper()
    live_score = float(row[10]) if row[10] is not None else None
    c2 = live_score
    if dtp == "SHORT":
        if c2 is None or c2 <= 50.0:
            raise ValueError(f"Effective conviction must be above 50 for SHORT (current: {round(c2 or 0.0, 1)})")
        if _bearish_index_gate_enabled():
            ig = index_bearish_gate_from_quotes()
            if not bool(ig.get("ok")):
                raise ValueError(
                    "SHORT is allowed only when NIFTY 5m bearish structure is confirmed (last 2 red + last 3 closes descending)."
                )
    else:
        if live_score is None or live_score < 60.0:
            raise ValueError(
                f"Effective conviction must be at least 60 (current: {round(live_score or 0.0, 1)})"
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


def manual_update_conviction_vwap(
    db: Session,
    user_id: int,
    screening_id: int,
    mode: Literal["live", "entry"],
    session_vwap: float,
    vwap_leg_reason: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Manual override for missing VWAP input in conviction computation.
    mode=live  -> updates session_vwap + live VWAP leg + live conviction score.
    mode=entry -> updates second_scan_vwap_leg (+ second_scan_oi_leg fallback) + entry score.
    """
    ensure_daily_futures_tables()
    if session_vwap <= 0:
        raise ValueError("Session VWAP must be greater than 0")
    row = db.execute(
        text(
            """
            SELECT id, trade_date, underlying, direction_type, instrument_key,
                   ltp, conviction_score, conviction_oi_leg, conviction_vwap_leg,
                   second_scan_conviction_score, second_scan_oi_leg, second_scan_vwap_leg,
                   candle_is_green, candle_higher_high, candle_higher_low,
                   conviction_breakdown_json
            FROM daily_futures_screening
            WHERE id = :sid
              AND trade_date = CAST(:d AS DATE)
            LIMIT 1
            """
        ),
        {"sid": screening_id, "d": str(ist_today())},
    ).mappings().first()
    if not row:
        raise ValueError("Screening row not found for today")

    dir_key = str(row.get("direction_type") or "LONG").strip().upper()
    ltp = _safe_float(row.get("ltp"))
    if ltp is None:
        ik = str(row.get("instrument_key") or "").strip()
        if ik:
            try:
                up = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
                q = up.get_market_quote_by_key(ik) or {}
                ltp = _safe_float(q.get("last_price")) or _safe_float(q.get("close"))
            except Exception:
                ltp = None
    if ltp is None:
        raise ValueError("LTP unavailable; cannot recalculate conviction")

    pvp = ((float(ltp) - float(session_vwap)) / float(session_vwap)) * 100.0
    if dir_key == "SHORT":
        vw_leg, vw_reason = _vwap_leg_score_reason_bearish(
            pvp,
            nifty15_close_below_sv=True,
            stock15_close_below_sv=True,
            candle_is_red=not bool(row.get("candle_is_green")),
            candle_lower_low=bool(row.get("candle_higher_low")),
            candle_lower_high=bool(row.get("candle_higher_high")),
        )
    else:
        vw_leg, vw_reason = _vwap_leg_score_reason(
            pvp,
            candle_is_green=bool(row.get("candle_is_green")),
            candle_higher_high=bool(row.get("candle_higher_high")),
            candle_higher_low=bool(row.get("candle_higher_low")),
        )

    if vwap_leg_reason is not None and str(vwap_leg_reason).strip():
        vw_reason = str(vwap_leg_reason).strip()[:500]

    cbj = row.get("conviction_breakdown_json")
    if not isinstance(cbj, dict):
        cbj = {}
    cbj["manual_vwap_override_by_user_id"] = int(user_id)
    cbj["manual_vwap_override_at"] = datetime.now(IST).isoformat()

    if mode == "live":
        oi_leg = _safe_float(row.get("conviction_oi_leg"))
        if oi_leg is None:
            oi_leg = 25.0
        live_score = round(max(0.0, min(100.0, float(oi_leg) + float(vw_leg))), 1)
        cbj["vwap_leg_reason"] = vw_reason
        cbj["price_vs_vwap_pct"] = round(float(pvp), 6)
        cbj["session_vwap"] = round(float(session_vwap), 6)
        cbj["ltp"] = round(float(ltp), 6)
        db.execute(
            text(
                """
                UPDATE daily_futures_screening
                SET session_vwap = :sv,
                    conviction_vwap_leg = :vw,
                    conviction_score = :cs,
                    conviction_breakdown_json = CAST(:cbj AS JSONB),
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = :sid
                """
            ),
            {
                "sid": screening_id,
                "sv": float(session_vwap),
                "vw": round(float(vw_leg), 1),
                "cs": live_score,
                "cbj": json.dumps(cbj),
            },
        )
        db.commit()
        return {
            "success": True,
            "mode": "live",
            "screening_id": screening_id,
            "session_vwap": round(float(session_vwap), 6),
            "price_vs_vwap_pct": round(float(pvp), 6),
            "conviction_vwap_leg": round(float(vw_leg), 1),
            "conviction_score": live_score,
            "vwap_leg_reason": vw_reason,
        }

    # mode == entry
    entry_oi_leg = _safe_float(row.get("second_scan_oi_leg"))
    if entry_oi_leg is None:
        entry_oi_leg = _safe_float(row.get("conviction_oi_leg"))
    if entry_oi_leg is None:
        entry_oi_leg = 25.0
    entry_score = round(max(0.0, min(100.0, float(entry_oi_leg) + float(vw_leg))), 1)
    cbj["entry_vwap_leg_reason"] = vw_reason
    cbj["entry_price_vs_vwap_pct"] = round(float(pvp), 6)
    cbj["entry_manual_session_vwap"] = round(float(session_vwap), 6)
    db.execute(
        text(
            """
            UPDATE daily_futures_screening
            SET second_scan_oi_leg = COALESCE(second_scan_oi_leg, :oi_leg),
                second_scan_vwap_leg = :vw,
                second_scan_conviction_score = :cs,
                conviction_breakdown_json = CAST(:cbj AS JSONB),
                updated_at = CURRENT_TIMESTAMP
            WHERE id = :sid
            """
        ),
        {
            "sid": screening_id,
            "oi_leg": round(float(entry_oi_leg), 1),
            "vw": round(float(vw_leg), 1),
            "cs": entry_score,
            "cbj": json.dumps(cbj),
        },
    )
    db.commit()
    return {
        "success": True,
        "mode": "entry",
        "screening_id": screening_id,
        "manual_session_vwap": round(float(session_vwap), 6),
        "entry_price_vs_vwap_pct": round(float(pvp), 6),
        "second_scan_vwap_leg": round(float(vw_leg), 1),
        "second_scan_oi_leg": round(float(entry_oi_leg), 1),
        "second_scan_conviction_score": entry_score,
        "vwap_leg_reason": vw_reason,
    }


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

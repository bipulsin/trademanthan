"""Relative Strength Scanner — ranks current-month NSE stock futures vs NIFTY.

Pipeline (run once every 5 min by the scheduler, never by the dashboard):
  1. Load current-month futures universe from ``arbitrage_master``.
  2. Fetch 5-minute candles per symbol (+ NIFTY quote).
  3. Compute indicators (EMA 5/9/10, VWAP, Supertrend, MACD, ADX, Volume Ratio).
  4. Compute Relative Strength = Stock %% - NIFTY %%.
  5. Evaluate Kavach state + composite Trade Score (see ``kavach_engine``).
  6. Rank Top 5 Bullish / Top 5 Bearish and bulk-insert into
     ``relative_strength_snapshot``.

The dashboard reads the latest snapshot via :func:`get_latest_snapshot` — it
never recalculates indicators.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pytz
from sqlalchemy import text

from backend.config import settings
from backend.database import SessionLocal
from backend.services.kavach_engine import (
    BEARISH_STATES,
    BULLISH_STATES,
    RANKING_BEARISH,
    RANKING_BULLISH,
    KavachInput,
    compute_trade_score,
    evaluate_kavach,
)
from backend.services.smart_futures_exit import _supertrend_dir_last_two
from backend.services.smart_futures_picker.indicators import adx_value
from backend.services.upstox_service import UpstoxService
from backend.services.vajra.indicators import cumulative_vwap, ema_series

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")
NIFTY_KEY = "NSE_INDEX|Nifty 50"
CANDLE_INTERVAL = "minutes/5"
CANDLE_DAYS_BACK = 5
VOLUME_EMA_PERIOD = 20
ADX_LENGTH = 14
TOP_N = 5
# Minimum bars to compute MACD(12,26,9) / ADX(14) reliably.
MIN_BARS = 40


# --- candle helpers ----------------------------------------------------------


def _parse_ist_date(ts: Any) -> Optional[str]:
    """Return YYYY-MM-DD (IST) for an Upstox candle timestamp, else None."""
    if not ts:
        return None
    try:
        dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except Exception:
        try:
            dt = datetime.strptime(str(ts)[:19], "%Y-%m-%dT%H:%M:%S")
        except Exception:
            return None
    dt = dt.astimezone(IST) if dt.tzinfo else IST.localize(dt)
    return dt.strftime("%Y-%m-%d")


def _sorted_candles(candles: List[Dict]) -> List[Dict]:
    return sorted(candles, key=lambda c: str(c.get("timestamp") or ""))


def _f(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


# --- indicator computation ---------------------------------------------------


def _current_and_prev_day_close(
    candles: List[Dict],
) -> Optional[Tuple[float, float, int]]:
    """Return (current_price, previous_day_close, first_today_index).

    ``current_price`` is the last 5m close; ``previous_day_close`` is the close of
    the last 5m bar of the most recent *prior trading day* (i.e. the previous day's
    closing price). Returns None if no prior-day bar exists in the window.
    """
    if not candles:
        return None
    closes = [_f(c.get("close")) for c in candles]
    last_date = _parse_ist_date(candles[-1].get("timestamp"))
    first_today: Optional[int] = None
    for i, c in enumerate(candles):
        if _parse_ist_date(c.get("timestamp")) == last_date:
            first_today = i
            break
    if first_today is None or first_today == 0:
        return None  # no prior-day bar -> cannot determine previous day close
    previous_day_close = closes[first_today - 1]
    current_price = closes[-1]
    if previous_day_close <= 0 or current_price <= 0:
        return None
    return current_price, previous_day_close, first_today


def _macd_last(closes: List[float]) -> Tuple[float, float, float]:
    """Return (macd_line, signal, histogram) at the last bar."""
    fast = ema_series(closes, 12)
    slow = ema_series(closes, 26)
    line = [fast[i] - slow[i] for i in range(len(closes))]
    signal = ema_series(line, 9)
    return line[-1], signal[-1], line[-1] - signal[-1]


def _compute_symbol_metrics(
    upstox: UpstoxService, entry: Dict[str, str], nifty_pct: float
) -> Optional[Dict[str, Any]]:
    """Fetch candles and compute all indicators + RS + Kavach for one symbol."""
    instrument_key = entry.get("instrument_key") or ""
    symbol = entry.get("stock") or ""
    if not instrument_key or not symbol:
        return None

    candles = upstox.get_historical_candles_by_instrument_key(
        instrument_key, interval=CANDLE_INTERVAL, days_back=CANDLE_DAYS_BACK
    )
    if not candles or len(candles) < MIN_BARS:
        return None
    candles = _sorted_candles(candles)

    closes = [_f(c.get("close")) for c in candles]
    highs = [_f(c.get("high")) for c in candles]
    lows = [_f(c.get("low")) for c in candles]
    volumes = [_f(c.get("volume")) for c in candles]

    # Stock %% basis = (current price - previous DAY close) / previous DAY close.
    split = _current_and_prev_day_close(candles)
    if split is None:
        return None
    current_price, previous_close, first_today = split

    # EMAs / MACD / Supertrend / ADX over the continuous 5m series (chart basis).
    ema5_s = ema_series(closes, 5)
    ema9_s = ema_series(closes, 9)
    ema10_s = ema_series(closes, 10)
    ema5, ema9, ema10 = ema5_s[-1], ema9_s[-1], ema10_s[-1]
    ema9_slope = ema9_s[-1] - ema9_s[-2] if len(ema9_s) >= 2 else 0.0

    # Session-anchored VWAP over today's bars only.
    t_highs = highs[first_today:]
    t_lows = lows[first_today:]
    t_closes = closes[first_today:]
    t_vols = volumes[first_today:]
    vwap = cumulative_vwap(t_highs, t_lows, t_closes, t_vols)[-1] if t_closes else current_price

    macd, macd_signal, macd_hist = _macd_last(closes)

    st_dir, _ = _supertrend_dir_last_two(highs, lows, closes)
    supertrend_bullish: Optional[bool] = None if st_dir is None else (st_dir > 0)

    adx = adx_value(highs, lows, closes, ADX_LENGTH) or 0.0

    cur_volume = volumes[-1]
    vol_ema = ema_series(volumes, VOLUME_EMA_PERIOD)[-1] if volumes else 0.0
    volume_ratio = (cur_volume / vol_ema) if vol_ema > 0 else 0.0

    stock_pct = (current_price - previous_close) / previous_close * 100.0
    relative_strength = stock_pct - nifty_pct

    kav = evaluate_kavach(
        KavachInput(
            price=current_price,
            ema5=ema5,
            ema9=ema9,
            ema9_slope=ema9_slope,
            vwap=vwap,
            supertrend_bullish=supertrend_bullish,
            macd=macd,
            macd_signal=macd_signal,
            macd_histogram=macd_hist,
            adx=adx,
            volume_ratio=volume_ratio,
        )
    )

    return {
        "symbol": symbol,
        "instrument_key": instrument_key,
        "future_symbol": entry.get("future_symbol") or "",
        "current_price": current_price,
        "previous_close": previous_close,
        "stock_percent": stock_pct,
        "nifty_percent": nifty_pct,
        "relative_strength": relative_strength,
        "ema5": ema5,
        "ema9": ema9,
        "ema10": ema10,
        "vwap": vwap,
        "supertrend": (1.0 if supertrend_bullish else (-1.0 if supertrend_bullish is False else 0.0)),
        "macd": macd,
        "macd_signal": macd_signal,
        "macd_histogram": macd_hist,
        "adx": adx,
        "volume": cur_volume,
        "avg_volume": vol_ema,
        "volume_ratio": volume_ratio,
        "kavach_state": kav.state,
        "kavach_strength": kav.strength,
    }


def _is_market_live_ist() -> bool:
    """True during NSE cash/derivatives session (Mon–Fri 09:15–15:30 IST)."""
    now = datetime.now(IST)
    if now.weekday() >= 5:
        return False
    minutes = now.hour * 60 + now.minute
    return 555 <= minutes <= 930  # 09:15 .. 15:30


def _nifty_change_pct(upstox: UpstoxService) -> Optional[float]:
    """NIFTY 50 %% change = (current NIFTY price - previous DAY close) / previous DAY close.

    The NIFTY index does not expose multi-day intraday candles, so previous-day
    close is taken from *daily* candles. Current price is the live LTP during
    market hours; after hours we use the last completed daily close vs the prior
    daily close (so a manual/off-schedule run reflects the last session's close).
    """
    daily = upstox.get_historical_candles_by_instrument_key(
        NIFTY_KEY, interval="days/1", days_back=12
    )
    closes_dated: List[Tuple[str, float]] = []
    if daily:
        for c in _sorted_candles(daily):
            cl = _f(c.get("close"))
            d = _parse_ist_date(c.get("timestamp"))
            if cl > 0 and d:
                closes_dated.append((d, cl))

    if len(closes_dated) >= 2:
        last_date, last_close = closes_dated[-1]
        prev_close = closes_dated[-2][1]
        ist_today = datetime.now(IST).strftime("%Y-%m-%d")
        quote = upstox.get_market_quote_by_key(NIFTY_KEY)
        ltp = _f(quote.get("last_price")) if quote else 0.0

        if last_date == ist_today and ltp > 0:
            # Today's daily candle already present -> current=LTP, prev=prior day.
            return (ltp - prev_close) / prev_close * 100.0
        if last_date < ist_today and _is_market_live_ist() and ltp > 0:
            # Live session, today's daily candle not yet in history.
            return (ltp - last_close) / last_close * 100.0
        # After hours / pre-open: last completed session vs the session before.
        return (last_close - prev_close) / prev_close * 100.0

    # Last resort: live quote (close_price = prior session close).
    quote = upstox.get_market_quote_by_key(NIFTY_KEY)
    if not quote:
        return None
    last = _f(quote.get("last_price"))
    prev = _f(quote.get("close_price")) or _f((quote.get("ohlc") or {}).get("close"))
    if prev <= 0 or last <= 0:
        return None
    return (last - prev) / prev * 100.0


# --- ranking -----------------------------------------------------------------


def _rank(rows: List[Dict[str, Any]]) -> Tuple[List[Dict], List[Dict]]:
    """Build Top-N Bullish / Bearish lists with trade scores and rank positions."""
    bullish: List[Dict] = []
    bearish: List[Dict] = []
    for r in rows:
        state = r["kavach_state"]
        if state in BULLISH_STATES:
            ranking_type = RANKING_BULLISH
            bucket = bullish
        elif state in BEARISH_STATES:
            ranking_type = RANKING_BEARISH
            bucket = bearish
        else:
            continue
        r = dict(r)
        r["ranking_type"] = ranking_type
        r["trade_score"] = compute_trade_score(
            rs=r["relative_strength"],
            state=state,
            volume_ratio=r["volume_ratio"],
            adx=r["adx"],
            price=r["current_price"],
            vwap=r["vwap"],
            ranking_type=ranking_type,
        )
        bucket.append(r)

    bullish.sort(key=lambda x: (-x["trade_score"], -x["relative_strength"]))
    bearish.sort(key=lambda x: (-x["trade_score"], x["relative_strength"]))
    bullish = bullish[:TOP_N]
    bearish = bearish[:TOP_N]
    for i, r in enumerate(bullish, start=1):
        r["rank_position"] = i
    for i, r in enumerate(bearish, start=1):
        r["rank_position"] = i
    return bullish, bearish


# --- persistence -------------------------------------------------------------

_INSERT_SQL = text(
    """
    INSERT INTO relative_strength_snapshot (
        scan_time, symbol, current_price, previous_close, stock_percent,
        nifty_percent, relative_strength, ema5, ema9, ema10, vwap, supertrend,
        macd, macd_signal, macd_histogram, adx, volume, avg_volume, volume_ratio,
        kavach_state, kavach_strength, trade_score, ranking_type, rank_position
    ) VALUES (
        :scan_time, :symbol, :current_price, :previous_close, :stock_percent,
        :nifty_percent, :relative_strength, :ema5, :ema9, :ema10, :vwap, :supertrend,
        :macd, :macd_signal, :macd_histogram, :adx, :volume, :avg_volume, :volume_ratio,
        :kavach_state, :kavach_strength, :trade_score, :ranking_type, :rank_position
    )
    """
)

_PERSIST_COLS = (
    "symbol", "current_price", "previous_close", "stock_percent", "nifty_percent",
    "relative_strength", "ema5", "ema9", "ema10", "vwap", "supertrend", "macd",
    "macd_signal", "macd_histogram", "adx", "volume", "avg_volume", "volume_ratio",
    "kavach_state", "kavach_strength", "trade_score", "ranking_type", "rank_position",
)


def _persist(scan_time: datetime, ranked: List[Dict[str, Any]]) -> None:
    if not ranked:
        return
    params = [
        {"scan_time": scan_time, **{c: r.get(c) for c in _PERSIST_COLS}} for r in ranked
    ]
    db = SessionLocal()
    try:
        db.execute(_INSERT_SQL, params)
        db.commit()
    finally:
        db.close()


# --- orchestrator ------------------------------------------------------------


def run_relative_strength_scan(scan_trigger: str = "5m_interval") -> Dict[str, Any]:
    """Run one full scan and persist Top-5 Bullish / Bearish. Returns a summary."""
    started = time.time()
    upstox = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)

    nifty_pct = _nifty_change_pct(upstox)
    if nifty_pct is None:
        logger.warning("Relative Strength scan (%s): NIFTY %% unavailable — aborting", scan_trigger)
        return {"ok": False, "reason": "nifty_unavailable"}

    # Lazy import avoids a circular import at module load time.
    from backend.services.vajra.job import load_arbitrage_curr_mth_universe

    universe = load_arbitrage_curr_mth_universe()
    rows: List[Dict[str, Any]] = []
    for entry in universe:
        try:
            m = _compute_symbol_metrics(upstox, entry, nifty_pct)
            if m:
                rows.append(m)
        except Exception as exc:  # one bad symbol must not abort the scan
            logger.warning(
                "Relative Strength scan: %s failed: %s", entry.get("stock"), exc
            )

    bullish, bearish = _rank(rows)
    scan_time = datetime.now(IST)
    _persist(scan_time, bullish + bearish)

    duration = time.time() - started
    logger.info(
        "Relative Strength scan (%s): %d/%d symbols, NIFTY %+.2f%%, "
        "%d bullish / %d bearish in %.1fs",
        scan_trigger, len(rows), len(universe), nifty_pct, len(bullish), len(bearish),
        duration,
    )
    return {
        "ok": True,
        "scanned": len(rows),
        "universe": len(universe),
        "nifty_percent": nifty_pct,
        "bullish": len(bullish),
        "bearish": len(bearish),
        "duration_sec": round(duration, 1),
    }


# --- read API ----------------------------------------------------------------

_LATEST_SQL = text(
    """
    SELECT s.scan_time, s.symbol, s.current_price, s.relative_strength, s.stock_percent,
           s.nifty_percent, s.vwap, s.supertrend, s.macd, s.macd_signal,
           s.macd_histogram, s.adx, s.volume_ratio, s.kavach_state,
           s.kavach_strength, s.trade_score, s.ranking_type, s.rank_position,
           am.currmth_future_instrument_key AS instrument_key,
           am.currmth_future_symbol AS future_symbol
    FROM relative_strength_snapshot s
    LEFT JOIN arbitrage_master am ON am.stock = s.symbol
    WHERE s.scan_time = (SELECT MAX(scan_time) FROM relative_strength_snapshot)
    ORDER BY s.ranking_type, s.rank_position
    """
)


def _row_to_dict(r) -> Dict[str, Any]:
    price = _f(r.current_price)
    vwap = _f(r.vwap)
    return {
        "rank": int(r.rank_position),
        "symbol": r.symbol,
        "instrument_key": r.instrument_key or "",
        "future_symbol": r.future_symbol or "",
        "price": round(price, 2),
        "rs_percent": round(_f(r.relative_strength), 2),
        "stock_percent": round(_f(r.stock_percent), 2),
        "nifty_percent": round(_f(r.nifty_percent), 2),
        "trade_score": round(_f(r.trade_score)),
        "volume_ratio": round(_f(r.volume_ratio), 2),
        "vwap": round(vwap, 2),
        "above_vwap": price > vwap,
        "supertrend_bullish": _f(r.supertrend) > 0,
        "macd_bullish": _f(r.macd) > _f(r.macd_signal),
        "adx": round(_f(r.adx), 1),
        "kavach_state": r.kavach_state,
        "kavach_strength": int(r.kavach_strength or 0),
    }


def get_latest_snapshot() -> Dict[str, Any]:
    """Return the most recent scan as ``{last_updated, bullish, bearish}``."""
    db = SessionLocal()
    try:
        rows = db.execute(_LATEST_SQL).fetchall()
    finally:
        db.close()

    bullish: List[Dict] = []
    bearish: List[Dict] = []
    last_updated = ""
    for r in rows:
        if not last_updated and r.scan_time is not None:
            last_updated = r.scan_time.isoformat()
        item = _row_to_dict(r)
        if r.ranking_type == RANKING_BULLISH:
            bullish.append(item)
        else:
            bearish.append(item)

    return {"last_updated": last_updated, "bullish": bullish, "bearish": bearish}

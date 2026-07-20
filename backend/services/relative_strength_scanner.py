"""Relative Strength Scanner — ranks current-month NSE stock futures vs NIFTY.

Pipeline (run once every 5 min by the scheduler, never by the dashboard):
  1. Load current-month futures universe from ``arbitrage_master``.
  2. Reuse the 5-minute candles the centralized market-data refresh already
     fetched (shared in-process ``candle_cache``); only fall back to a direct
     Upstox fetch on a cache miss. This keeps the scanner off the Upstox
     rate-limit (429) hot path at market open.
  3. Compute indicators (EMA 5/9/10, VWAP, Supertrend, MACD, ADX, Volume Ratio).
  4. Compute Relative Strength = Stock %% - NIFTY %%.
  5. Evaluate Kavach state + composite Trade Score (see ``kavach_engine``).
  6. Rank Top-N Bullish / Bearish (persist Top-10 for lock-removal hysteresis;
     actionable Top-5 remains the UI / morning-lock / entry band) and bulk-insert
     into ``relative_strength_snapshot``.

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
from backend.services.kavach_confidence import (
    REGIME_TREND,
    compute_vwap_purity_pct,
    detect_market_regime,
    resolve_score_and_grade,
)
from backend.services.kavach_volume import (
    closed_bar_volume_ratio,
    cumulative_volume_tod_ratio,
    last_closed_bar_index,
    volume_participation_label,
)
from backend.services.kavach_engine import (
    BEARISH_STATES,
    BULLISH_STATES,
    RANKING_BEARISH,
    RANKING_BULLISH,
    KavachInput,
    compute_trade_score,
    evaluate_kavach,
)
from backend.services.rs_scanner_maturity import (
    default_maturity_fields,
    load_today_maturity_map,
)
from backend.services.market_data import candle_cache
from backend.services.smart_futures_exit import _supertrend_dir_last_two
from backend.services.smart_futures_picker.indicators import adx_value
from backend.services.upstox_service import UpstoxService
from backend.services.vajra.indicators import cumulative_vwap, ema_series

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")
NIFTY_KEY = "NSE_INDEX|Nifty 50"
CANDLE_INTERVAL = "minutes/5"
CANDLE_DAYS_BACK = 5
# Accept market-data cache candles up to this old. The centralized refresh cycle
# can take ~8-9 min under Upstox rate limits, so use a window wide enough to
# accumulate symbols across consecutive cycles rather than expiring them early.
CACHE_MAX_AGE_SEC = 900
VOLUME_EMA_PERIOD = 20
ADX_LENGTH = 14
TOP_N = 5
# Persist beyond Top-5 so lock-removal R2 can apply a wider hysteresis band (default Top-10).
PERSIST_TOP_N = 10

# NIFTY daily closes change once per session — cache the (expensive) daily-candle
# fetch per IST date so a transient 429 storm cannot zero out RS. Also remember the
# last good NIFTY % as a fallback when the live quote momentarily fails.
_NIFTY_DAILY_CACHE: Optional[Tuple[str, List[Tuple[str, float]]]] = None  # (fetched_date, closes_dated)
_LAST_GOOD_NIFTY_PCT: Optional[float] = None
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


def _macd_last(
    closes: List[float],
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
) -> Tuple[float, float, float]:
    """Return (macd_line, signal, histogram) at the last bar.

    Defaults are classic 12/26/9 (RS scanner). TWCTO Kavach Pine v2.6 uses 6/13/5
    — pass those explicitly from ``kavach_10m`` for chart-panel parity.
    """
    fast_s = ema_series(closes, fast)
    slow_s = ema_series(closes, slow)
    line = [fast_s[i] - slow_s[i] for i in range(len(closes))]
    sig = ema_series(line, signal)
    return line[-1], sig[-1], line[-1] - sig[-1]


def _candles_for_symbol(
    upstox: UpstoxService, instrument_key: str, *, cache_only: bool
) -> Tuple[Optional[List[Dict]], bool]:
    """Return (candles, from_cache). Prefer the shared market-data cache.

    When ``cache_only`` (scheduled market-hours runs) we never issue our own
    Upstox fetch — the platform's historical-candle quota is shared and heavily
    rate-limited, so adding 200 fetches just starves the core refresh. Manual /
    off-hours runs may fall back to a direct fetch (and warm the cache)."""
    cached = candle_cache.get_recent(instrument_key, CANDLE_INTERVAL, CACHE_MAX_AGE_SEC)
    if cached and len(cached) >= MIN_BARS:
        return cached, True
    if cache_only:
        return None, False
    # Off-hours / manual run: a direct fetch auto-populates the shared cache.
    fetched = upstox.get_historical_candles_by_instrument_key(
        instrument_key, interval=CANDLE_INTERVAL, days_back=CANDLE_DAYS_BACK
    )
    return fetched, False


def _compute_symbol_metrics(
    upstox: UpstoxService, entry: Dict[str, str], nifty_pct: float, *, cache_only: bool
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """Compute all indicators + RS + Kavach for one symbol (cache-first candles).

    Returns ``(metrics, exclusion_reason)``. On success ``exclusion_reason`` is
    None; on failure ``metrics`` is None and reason is a canonical code from
    ``rs_exclusion_audit``.
    """
    from backend.services.rs_exclusion_audit import (
        REASON_MISSING_CANDLES,
        REASON_MISSING_KEY,
        REASON_NO_CLOSED_BAR,
        REASON_NO_PREV_CLOSE,
    )

    instrument_key = entry.get("instrument_key") or ""
    symbol = entry.get("stock") or ""
    if not instrument_key or not symbol:
        return None, REASON_MISSING_KEY

    candles, from_cache = _candles_for_symbol(upstox, instrument_key, cache_only=cache_only)
    if not candles or len(candles) < MIN_BARS:
        return None, REASON_MISSING_CANDLES
    candles = _sorted_candles(candles)

    closes = [_f(c.get("close")) for c in candles]
    highs = [_f(c.get("high")) for c in candles]
    lows = [_f(c.get("low")) for c in candles]
    volumes = [_f(c.get("volume")) for c in candles]

    # Stock %% basis = (current price - previous DAY close) / previous DAY close.
    split = _current_and_prev_day_close(candles)
    if split is None:
        return None, REASON_NO_PREV_CLOSE
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

    adx = adx_value(highs, lows, closes, ADX_LENGTH) or 0.0

    closed_idx = last_closed_bar_index(candles)
    if closed_idx < 0:
        return None, REASON_NO_CLOSED_BAR
    closed_price = closes[closed_idx]
    cur_volume, vol_ema, volume_ratio = closed_bar_volume_ratio(volumes, closed_idx=closed_idx)
    volume_tod_ratio = cumulative_volume_tod_ratio(candles, closed_idx=closed_idx)
    vol_label = volume_participation_label(volume_ratio, volume_tod_ratio)

    # Session VWAP series for today (for purity on 10m-equivalent bars).
    t_highs_full = highs[first_today : closed_idx + 1]
    t_lows_full = lows[first_today : closed_idx + 1]
    t_closes_full = closes[first_today : closed_idx + 1]
    t_vols_full = volumes[first_today : closed_idx + 1]
    vwap_series_today = (
        cumulative_vwap(t_highs_full, t_lows_full, t_closes_full, t_vols_full)
        if t_closes_full
        else [vwap]
    )

    # Regime: compare last two closed bars.
    prev_idx = max(first_today, closed_idx - 1)
    st_prev_dir, st_curr_dir = _supertrend_dir_last_two(
        highs[: closed_idx + 1], lows[: closed_idx + 1], closes[: closed_idx + 1]
    )
    st_prev = None if st_prev_dir is None else (st_prev_dir > 0)
    st_curr = None if st_curr_dir is None else (st_curr_dir > 0)
    macd_prev, sig_prev, _ = _macd_last(closes[:prev_idx + 1]) if prev_idx >= 26 else (macd, macd_signal, 0.0)
    regime = detect_market_regime(
        st_prev=st_prev,
        st_curr=st_curr,
        macd_prev=macd_prev,
        macd_sig_prev=sig_prev,
        macd_curr=macd,
        macd_sig_curr=macd_signal,
        ema5_prev=ema5_s[prev_idx] if prev_idx < len(ema5_s) else ema5,
        vwap_prev=vwap_series_today[-2] if len(vwap_series_today) >= 2 else vwap,
        ema5_curr=ema5,
        vwap_curr=vwap,
    )

    stock_pct = (closed_price - previous_close) / previous_close * 100.0
    relative_strength = stock_pct - nifty_pct

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

    purity_dir = "SHORT" if kav.state in BEARISH_STATES else "LONG"
    vwap_purity = compute_vwap_purity_pct(
        t_closes_full, vwap_series_today, direction=purity_dir
    )

    return {
        "symbol": symbol,
        "instrument_key": instrument_key,
        "future_symbol": entry.get("future_symbol") or "",
        "from_cache": from_cache,
        "current_price": closed_price,
        "previous_close": previous_close,
        "stock_percent": stock_pct,
        "nifty_percent": nifty_pct,
        "relative_strength": relative_strength,
        "ema5": ema5,
        "ema9": ema9,
        "ema10": ema10,
        "vwap": vwap,
        "supertrend": (1.0 if st_curr else (-1.0 if st_curr is False else 0.0)),
        "macd": macd,
        "macd_signal": macd_signal,
        "macd_histogram": macd_hist,
        "adx": adx,
        "volume": cur_volume,
        "avg_volume": vol_ema,
        "volume_ratio": volume_ratio,
        "volume_tod_ratio": volume_tod_ratio,
        "volume_label": vol_label,
        "vwap_purity_pct": vwap_purity,
        "market_regime": regime,
        "kavach_state": kav.state,
        "kavach_strength": kav.strength,
    }, None


def _is_market_live_ist() -> bool:
    """True during NSE cash/derivatives session (Mon–Fri 09:15–15:30 IST)."""
    now = datetime.now(IST)
    if now.weekday() >= 5:
        return False
    minutes = now.hour * 60 + now.minute
    return 555 <= minutes <= 930  # 09:15 .. 15:30


def _nifty_pct_from_index_db() -> Optional[float]:
    """NIFTY %% from the ``index_prices`` table — current ltp vs the previous
    trading day's stored close. Zero Upstox calls, so it is immune to the shared
    historical-candle 429 storm (the index_price scheduler refreshes ltp ~5-min)."""
    db = SessionLocal()
    try:
        cur = db.execute(
            text(
                "SELECT ltp FROM index_prices WHERE index_name='NIFTY50' AND ltp > 0 "
                "ORDER BY price_time DESC LIMIT 1"
            )
        ).fetchone()
        if not cur or not cur.ltp:
            return None
        today_ist = datetime.now(IST).date()
        rows = db.execute(
            text(
                "SELECT close_price, price_time FROM index_prices "
                "WHERE index_name='NIFTY50' AND close_price IS NOT NULL "
                "ORDER BY price_time DESC LIMIT 20"
            )
        ).fetchall()
        for r in rows:
            pt = r.price_time
            if pt is None:
                continue
            if pt.tzinfo is None:
                pt = pytz.utc.localize(pt)
            if pt.astimezone(IST).date() < today_ist:  # previous-day close
                prev_close = float(r.close_price)
                if prev_close > 0:
                    return (float(cur.ltp) - prev_close) / prev_close * 100.0
                break
        return None
    except Exception as exc:
        logger.debug("nifty pct from index_prices failed: %s", exc)
        return None
    finally:
        db.close()


def _nifty_daily_closes(upstox: UpstoxService) -> List[Tuple[str, float]]:
    """NIFTY daily (date, close) list, fetched at most once per IST date."""
    global _NIFTY_DAILY_CACHE
    ist_today = datetime.now(IST).strftime("%Y-%m-%d")
    if _NIFTY_DAILY_CACHE and _NIFTY_DAILY_CACHE[0] == ist_today and len(_NIFTY_DAILY_CACHE[1]) >= 2:
        return _NIFTY_DAILY_CACHE[1]
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
        _NIFTY_DAILY_CACHE = (ist_today, closes_dated)
    elif _NIFTY_DAILY_CACHE is not None:
        # Daily fetch failed (e.g. 429) — reuse yesterday's closes rather than nothing.
        return _NIFTY_DAILY_CACHE[1]
    return closes_dated


def _nifty_change_pct(upstox: UpstoxService) -> Optional[float]:
    """NIFTY 50 %% change = (current NIFTY price - previous DAY close) / previous DAY close.

    Primary source is the ``index_prices`` DB table (no Upstox call, immune to the
    candle 429 storm). Falls back to *daily* candles / live quote when the DB has
    no usable rows (e.g. before the index_price scheduler has run).
    """
    global _LAST_GOOD_NIFTY_PCT

    db_pct = _nifty_pct_from_index_db()
    if db_pct is not None:
        _LAST_GOOD_NIFTY_PCT = db_pct
        return db_pct

    closes_dated = _nifty_daily_closes(upstox)

    if len(closes_dated) >= 2:
        last_date, last_close = closes_dated[-1]
        prev_close = closes_dated[-2][1]
        ist_today = datetime.now(IST).strftime("%Y-%m-%d")
        quote = upstox.get_market_quote_by_key(NIFTY_KEY)
        ltp = _f(quote.get("last_price")) if quote else 0.0

        pct: Optional[float] = None
        if last_date == ist_today and ltp > 0:
            pct = (ltp - prev_close) / prev_close * 100.0
        elif last_date < ist_today and _is_market_live_ist() and ltp > 0:
            pct = (ltp - last_close) / last_close * 100.0
        elif not _is_market_live_ist():
            # After hours / pre-open: last completed session vs the one before.
            pct = (last_close - prev_close) / prev_close * 100.0

        if pct is not None:
            _LAST_GOOD_NIFTY_PCT = pct
            return pct
        # Live session but quote failed — reuse last good % rather than abort.
        if _LAST_GOOD_NIFTY_PCT is not None:
            return _LAST_GOOD_NIFTY_PCT

    # Last resort: live quote (close_price = prior session close).
    quote = upstox.get_market_quote_by_key(NIFTY_KEY)
    if quote:
        last = _f(quote.get("last_price"))
        prev = _f(quote.get("close_price")) or _f((quote.get("ohlc") or {}).get("close"))
        if prev > 0 and last > 0:
            _LAST_GOOD_NIFTY_PCT = (last - prev) / prev * 100.0
            return _LAST_GOOD_NIFTY_PCT
    return _LAST_GOOD_NIFTY_PCT


# --- ranking -----------------------------------------------------------------


def _rank(rows: List[Dict[str, Any]]) -> Tuple[List[Dict], List[Dict], List[Dict]]:
    """Build Top-N Bullish / Bearish lists with trade scores and rank positions.

    Also returns exclusion rows for NEUTRAL Kavach and beyond-persist truncates
    (audit only — ranking output unchanged).
    """
    from backend.services.rs_exclusion_audit import (
        REASON_BEYOND_PERSIST,
        REASON_NEUTRAL,
        exclusion_row,
    )

    bullish: List[Dict] = []
    bearish: List[Dict] = []
    exclusions: List[Dict] = []
    for r in rows:
        state = r["kavach_state"]
        if state in BULLISH_STATES:
            ranking_type = RANKING_BULLISH
            bucket = bullish
        elif state in BEARISH_STATES:
            ranking_type = RANKING_BEARISH
            bucket = bearish
        else:
            exclusions.append(
                exclusion_row(
                    symbol=r.get("symbol") or "",
                    exclusion_reason=REASON_NEUTRAL,
                    instrument_key=r.get("instrument_key"),
                    kavach_state=state,
                    relative_strength=r.get("relative_strength"),
                    ranking_side="NEUTRAL",
                    current_price=r.get("current_price"),
                    volume_ratio=r.get("volume_ratio"),
                    volume_label=r.get("volume_label"),
                    detail=f"kavach_state={state}",
                )
            )
            continue
        r = dict(r)
        r["ranking_type"] = ranking_type
        raw_score = compute_trade_score(
            rs=r["relative_strength"],
            state=state,
            volume_ratio=r["volume_ratio"],
            adx=r["adx"],
            price=r["current_price"],
            vwap=r["vwap"],
            ranking_type=ranking_type,
        )
        resolved = resolve_score_and_grade(
            raw_score,
            r.get("volume_label") or "Low",
            r.get("vwap_purity_pct") or 0.0,
            r.get("market_regime") or REGIME_TREND,
            close=r.get("current_price"),
            ema10=r.get("ema10"),
            vwap=r.get("vwap"),
        )
        r["trade_score"] = resolved["trade_score"]
        r["trade_score_raw"] = raw_score
        r["confidence_grade"] = resolved["confidence_grade"]
        r["stretch"] = resolved.get("stretch") or {}
        bucket.append(r)

    # Bullish: highest RS%% on top. Bearish: lowest RS%% on top. Trade Score breaks ties.
    # Persist PERSIST_TOP_N so removal hysteresis can see ranks 6..band; actionable Top-N stays TOP_N.
    persist_n = max(int(TOP_N), int(PERSIST_TOP_N))
    bullish.sort(key=lambda x: (-x["relative_strength"], -x["trade_score"]))
    bearish.sort(key=lambda x: (x["relative_strength"], -x["trade_score"]))

    def _truncate(bucket: List[Dict], side: str) -> List[Dict]:
        cutoff_rs_persist = (
            bucket[persist_n - 1]["relative_strength"] if len(bucket) >= persist_n else None
        )
        cutoff_rs_top_n = (
            bucket[TOP_N - 1]["relative_strength"] if len(bucket) >= TOP_N else None
        )
        kept: List[Dict] = []
        for i, row in enumerate(bucket, start=1):
            if i <= persist_n:
                row = dict(row)
                row["rank_position"] = i
                kept.append(row)
            else:
                exclusions.append(
                    exclusion_row(
                        symbol=row.get("symbol") or "",
                        exclusion_reason=REASON_BEYOND_PERSIST,
                        instrument_key=row.get("instrument_key"),
                        kavach_state=row.get("kavach_state"),
                        relative_strength=row.get("relative_strength"),
                        trade_score=row.get("trade_score"),
                        confidence_grade=row.get("confidence_grade"),
                        ranking_side=side,
                        would_be_rank=i,
                        rank_cutoff=persist_n,
                        top_n_cutoff=TOP_N,
                        cutoff_rs_persist=cutoff_rs_persist,
                        cutoff_rs_top_n=cutoff_rs_top_n,
                        current_price=row.get("current_price"),
                        volume_ratio=row.get("volume_ratio"),
                        volume_label=row.get("volume_label"),
                        detail=f"side={side} would_be_rank={i} persist_n={persist_n}",
                    )
                )
        return kept

    return _truncate(bullish, RANKING_BULLISH), _truncate(bearish, RANKING_BEARISH), exclusions


# --- persistence -------------------------------------------------------------

_INSERT_SQL = text(
    """
    INSERT INTO relative_strength_snapshot (
        scan_time, symbol, current_price, previous_close, stock_percent,
        nifty_percent, relative_strength, ema5, ema9, ema10, vwap, supertrend,
        macd, macd_signal, macd_histogram, adx, volume, avg_volume, volume_ratio,
        volume_tod_ratio, volume_label, vwap_purity_pct, market_regime, confidence_grade,
        kavach_state, kavach_strength, trade_score, ranking_type, rank_position
    ) VALUES (
        :scan_time, :symbol, :current_price, :previous_close, :stock_percent,
        :nifty_percent, :relative_strength, :ema5, :ema9, :ema10, :vwap, :supertrend,
        :macd, :macd_signal, :macd_histogram, :adx, :volume, :avg_volume, :volume_ratio,
        :volume_tod_ratio, :volume_label, :vwap_purity_pct, :market_regime, :confidence_grade,
        :kavach_state, :kavach_strength, :trade_score, :ranking_type, :rank_position
    )
    """
)

_PERSIST_COLS = (
    "symbol", "current_price", "previous_close", "stock_percent", "nifty_percent",
    "relative_strength", "ema5", "ema9", "ema10", "vwap", "supertrend", "macd",
    "macd_signal", "macd_histogram", "adx", "volume", "avg_volume", "volume_ratio",
    "volume_tod_ratio", "volume_label", "vwap_purity_pct", "market_regime",
    "confidence_grade", "kavach_state", "kavach_strength", "trade_score",
    "ranking_type", "rank_position",
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


def run_relative_strength_scan(
    scan_trigger: str = "5m_interval", *, cache_only: Optional[bool] = None
) -> Dict[str, Any]:
    """Run one full scan and persist Top-5 Bullish / Bearish. Returns a summary.

    ``cache_only`` controls whether the scan may issue its own Upstox candle
    fetches. Default: cache-only during a live session (to stay off the shared,
    rate-limited historical-candle quota), fetch-allowed off-hours / manual runs.
    """
    if cache_only is None:
        cache_only = _is_market_live_ist()
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
    exclusions: List[Dict[str, Any]] = []
    cache_hits = 0
    from backend.services.rs_exclusion_audit import (
        REASON_EXCEPTION,
        exclusion_row,
        write_exclusion_log,
    )

    for entry in universe:
        try:
            m, excl_reason = _compute_symbol_metrics(
                upstox, entry, nifty_pct, cache_only=cache_only
            )
            if m:
                if m.pop("from_cache", False):
                    cache_hits += 1
                rows.append(m)
            elif excl_reason:
                exclusions.append(
                    exclusion_row(
                        symbol=entry.get("stock") or "",
                        exclusion_reason=excl_reason,
                        instrument_key=entry.get("instrument_key"),
                        detail=f"cache_only={cache_only}",
                    )
                )
        except Exception as exc:  # one bad symbol must not abort the scan
            logger.warning(
                "Relative Strength scan: %s failed: %s", entry.get("stock"), exc
            )
            exclusions.append(
                exclusion_row(
                    symbol=entry.get("stock") or "",
                    exclusion_reason=REASON_EXCEPTION,
                    instrument_key=entry.get("instrument_key"),
                    detail=str(exc)[:500],
                )
            )

    bullish, bearish, rank_exclusions = _rank(rows)
    exclusions.extend(rank_exclusions)
    ranked = bullish + bearish
    try:
        from backend.services.rs_scanner_maturity import enrich_ranked_with_maturity

        enrich_ranked_with_maturity(ranked, upstox)
    except Exception as exc:
        logger.warning("Relative Strength scan: maturity enrichment failed: %s", exc)

    scan_time = datetime.now(IST)
    _persist(scan_time, ranked)
    excl_n = write_exclusion_log(
        scan_time=scan_time, scan_trigger=scan_trigger, exclusions=exclusions
    )

    duration = time.time() - started
    logger.info(
        "Relative Strength scan (%s, cache_only=%s): %d/%d symbols (%d from cache), "
        "NIFTY %+.2f%%, %d bullish / %d bearish, %d exclusions logged in %.1fs",
        scan_trigger, cache_only, len(rows), len(universe), cache_hits, nifty_pct,
        len(bullish), len(bearish), excl_n, duration,
    )
    return {
        "ok": True,
        "scanned": len(rows),
        "universe": len(universe),
        "cache_only": cache_only,
        "cache_hits": cache_hits,
        "nifty_percent": nifty_pct,
        "bullish": len(bullish),
        "bearish": len(bearish),
        "exclusions_logged": excl_n,
        "duration_sec": round(duration, 1),
    }


# --- read API ----------------------------------------------------------------

_LATEST_SQL = text(
    """
    SELECT s.scan_time, s.symbol, s.current_price, s.relative_strength, s.stock_percent,
           s.nifty_percent, s.vwap, s.supertrend, s.macd, s.macd_signal,
           s.macd_histogram, s.adx, s.volume_ratio, s.volume_tod_ratio, s.volume_label,
           s.vwap_purity_pct, s.market_regime, s.confidence_grade,
           s.kavach_state,
           s.kavach_strength, s.trade_score, s.ranking_type, s.rank_position,
           am.currmth_future_instrument_key AS instrument_key,
           am.currmth_future_symbol AS future_symbol
    FROM relative_strength_snapshot s
    LEFT JOIN arbitrage_master am ON am.stock = s.symbol
    WHERE s.scan_time = (SELECT MAX(scan_time) FROM relative_strength_snapshot)
    ORDER BY s.ranking_type, s.rank_position
    """
)


def _row_to_dict(r, maturity: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    price = _f(r.current_price)
    vwap = _f(r.vwap)
    mat = maturity or default_maturity_fields()
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
        "volume_tod_ratio": round(_f(getattr(r, "volume_tod_ratio", 0) or 0), 2),
        "volume_label": getattr(r, "volume_label", None) or "",
        "vwap_purity_pct": round(_f(getattr(r, "vwap_purity_pct", 0) or 0), 1),
        "market_regime": getattr(r, "market_regime", None) or REGIME_TREND,
        "confidence_grade": getattr(r, "confidence_grade", None) or "",
        "vwap": round(vwap, 2),
        "above_vwap": price > vwap,
        "supertrend_bullish": _f(r.supertrend) > 0,
        "macd_bullish": _f(r.macd) > _f(r.macd_signal),
        "adx": round(_f(r.adx), 1),
        "kavach_state": r.kavach_state,
        "kavach_strength": int(r.kavach_strength or 0),
        "maturity_tag": mat.get("maturity_tag", "FRESH"),
        "consecutive_days_on_list": int(mat.get("consecutive_days_on_list") or 1),
        "range_vs_atr_ratio": round(_f(mat.get("range_vs_atr_ratio")), 2),
    }


def get_latest_snapshot() -> Dict[str, Any]:
    """Return the most recent scan as ``{last_updated, bullish, bearish}``."""
    db = SessionLocal()
    try:
        rows = db.execute(_LATEST_SQL).fetchall()
    finally:
        db.close()

    maturity_map = load_today_maturity_map()

    bullish: List[Dict] = []
    bearish: List[Dict] = []
    last_updated = ""
    for r in rows:
        if not last_updated and r.scan_time is not None:
            last_updated = r.scan_time.isoformat()
        # Persist may include ranks beyond TOP_N for lock-removal hysteresis; UI stays Top-5.
        if r.rank_position is not None and int(r.rank_position) > TOP_N:
            continue
        mat = maturity_map.get(r.symbol) or default_maturity_fields()
        item = _row_to_dict(r, mat)
        if r.ranking_type == RANKING_BULLISH:
            bullish.append(item)
        else:
            bearish.append(item)

    return {"last_updated": last_updated, "bullish": bullish, "bearish": bearish}

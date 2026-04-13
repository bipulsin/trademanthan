"""
Dashboard: OI buildup heatmap for top 10 F&O names (premarket watchlist when available).

Uses NSE derivative quote API when reachable; falls back to Upstox current-month future
quotes (OI + price) when NSE blocks datacenter IPs. Server-side response cache + locks.
"""
from __future__ import annotations

import fcntl
import logging
import threading
import time
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

import pytz
from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.oi_integration import NSEOIFetcher, interpret_oi_signal
from backend.services.premarket_watchlist_job import (
    fetch_premarket_watchlist_for_date,
    run_premarket_watchlist_job,
)

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")

# Slightly below client poll (180s) so browsers usually get a warm cache.
_RESPONSE_CACHE_TTL_SEC = 150.0
# Gentle spacing between NSE calls within one refresh (same session).
_SLEEP_BETWEEN_NSE_CALLS_SEC = 0.06
_TOP_N = 10

# Serialize premarket job across workers; cooldown avoids hammering Upstox if the job keeps failing.
_PREMARKET_FLOCK_PATH = "/tmp/tm_heatmap_premarket_job.flock"
_PREMARKET_ATTEMPT_COOLDOWN_SEC = 45 * 60.0
_last_premarket_attempt_mono: float = 0.0

_upstox_cached: Any = None
_upstox_init_failed: bool = False

_response_cache: Optional[Tuple[float, Dict[str, Any]]] = None
_refresh_lock = threading.Lock()


def _session_today_ist() -> date:
    return datetime.now(IST).date()


def _symbols_in_rank_order(rows: List[Dict[str, Any]]) -> List[str]:
    out: List[str] = []
    for r in rows:
        s = (r.get("stock") or "").strip().upper()
        if s and s not in out:
            out.append(s)
    return out[:_TOP_N]


def _wait_for_premarket_rows(session_date: date, want_min: int, max_wait_sec: float = 150.0) -> List[Dict[str, Any]]:
    """Poll DB while another worker runs the premarket job (can take 1–2+ minutes)."""
    deadline = time.monotonic() + max_wait_sec
    step = 0.4
    while time.monotonic() < deadline:
        rows = fetch_premarket_watchlist_for_date(session_date)
        if len(rows) >= want_min:
            return rows
        time.sleep(step)
    return fetch_premarket_watchlist_for_date(session_date)


def _upstox_service():
    """Lazy singleton; NSE OI often fails on server IPs — Upstox is the primary fallback."""
    global _upstox_cached, _upstox_init_failed
    if _upstox_init_failed:
        return None
    if _upstox_cached is not None:
        return _upstox_cached
    try:
        from backend.config import settings
        from backend.services.upstox_service import UpstoxService

        _upstox_cached = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
        return _upstox_cached
    except Exception as e:
        logger.warning("dashboard_oi_heatmap: Upstox init failed: %s", e)
        _upstox_init_failed = True
        return None


def _future_key_from_arbitrage(symbol: str) -> Optional[str]:
    sym = (symbol or "").strip().upper()
    if not sym:
        return None
    db = SessionLocal()
    try:
        row = db.execute(
            text(
                """
                SELECT currmth_future_instrument_key
                FROM arbitrage_master
                WHERE UPPER(TRIM(stock)) = :s
                  AND currmth_future_instrument_key IS NOT NULL
                  AND TRIM(currmth_future_instrument_key) <> ''
                LIMIT 1
                """
            ),
            {"s": sym},
        ).fetchone()
        if not row or not row[0]:
            return None
        return str(row[0]).strip()
    finally:
        db.close()


def _nse_derivative_usable(raw: Dict[str, Any]) -> bool:
    oi = int(raw.get("oi") or 0)
    lp = float(raw.get("last_price") or 0)
    pc = float(raw.get("prev_close") or 0)
    return oi > 0 or lp > 1e-6 or pc > 1e-6


def _run_premarket_job_under_flock() -> None:
    with open(_PREMARKET_FLOCK_PATH, "w") as fp:
        fcntl.flock(fp.fileno(), fcntl.LOCK_EX)
        try:
            run_premarket_watchlist_job()
        finally:
            fcntl.flock(fp.fileno(), fcntl.LOCK_UN)


def _needs_auto_premarket_job(rows: List[Dict[str, Any]]) -> bool:
    """
    Run the same ranked scan as the scheduler when we do not yet have a full top 10 for today.
    - Empty: any IST weekday (pre-market or post-market).
    - Partial (1..9): only after 15:15 IST (post-market top-up for today).
    """
    if len(rows) >= _TOP_N:
        return False
    now = datetime.now(IST)
    if now.weekday() >= 5:
        return False
    if len(rows) == 0:
        return True
    if 0 < len(rows) < _TOP_N:
        return now.hour > 15 or (now.hour == 15 and now.minute >= 15)
    return False


def _maybe_run_premarket_for_today() -> None:
    global _last_premarket_attempt_mono
    today = _session_today_ist()
    rows = fetch_premarket_watchlist_for_date(today)
    if not _needs_auto_premarket_job(rows):
        return
    now_m = time.monotonic()
    if now_m - _last_premarket_attempt_mono < _PREMARKET_ATTEMPT_COOLDOWN_SEC:
        return
    _last_premarket_attempt_mono = now_m
    try:
        _run_premarket_job_under_flock()
    except Exception as e:
        logger.exception("dashboard_oi_heatmap: premarket job under flock failed: %s", e)


def _fetch_latest_session_date_min_rows(min_rows: int) -> Optional[date]:
    db = SessionLocal()
    try:
        row = db.execute(
            text(
                """
                SELECT session_date
                FROM premarket_watchlist
                GROUP BY session_date
                HAVING COUNT(*) >= :m
                ORDER BY session_date DESC
                LIMIT 1
                """
            ),
            {"m": min_rows},
        ).fetchone()
        return row[0] if row else None
    finally:
        db.close()


def _resolve_top10_symbols() -> Tuple[List[str], str]:
    """
    Same ranked universe as the scheduled pre-market job (OBV + gap + range), never alphabetical.

    1) Today's premarket_watchlist when it has a full top 10.
    2) Else: may run ``run_premarket_watchlist_job()`` under flock (weekdays): on empty rows anytime,
       or on partial rows after 15:15 IST (post-market top-up for today). Cooldown between attempts.
    3) Partial / latest session fallback as before.
    """
    today = _session_today_ist()
    rows_today = fetch_premarket_watchlist_for_date(today)

    if len(rows_today) >= _TOP_N:
        return _symbols_in_rank_order(rows_today), "premarket_today"

    _maybe_run_premarket_for_today()
    rows_today = fetch_premarket_watchlist_for_date(today)
    if len(rows_today) >= _TOP_N:
        return _symbols_in_rank_order(rows_today), "premarket_today"
    if len(rows_today) > 0:
        return _symbols_in_rank_order(rows_today), "premarket_today_partial"

    if datetime.now(IST).weekday() < 5:
        rows_today = _wait_for_premarket_rows(today, 1, max_wait_sec=35.0)
        if len(rows_today) >= _TOP_N:
            return _symbols_in_rank_order(rows_today), "premarket_today"
        if len(rows_today) > 0:
            return _symbols_in_rank_order(rows_today), "premarket_today_partial"

    d10 = _fetch_latest_session_date_min_rows(_TOP_N)
    if d10:
        rows = fetch_premarket_watchlist_for_date(d10)
        return _symbols_in_rank_order(rows), f"premarket_session_{d10.isoformat()}"

    d1 = _fetch_latest_session_date_min_rows(1)
    if d1:
        rows = fetch_premarket_watchlist_for_date(d1)
        return _symbols_in_rank_order(rows), f"premarket_session_{d1.isoformat()}"

    return [], "none"


def _heatmap_row_dict(
    rank: int,
    symbol: str,
    lp: float,
    pc: float,
    oi: int,
    chg: int,
    oi_source: str,
) -> Dict[str, Any]:
    prev_oi = max(0, int(oi) - int(chg))
    dp = float(lp) - float(pc)
    price_change_pct = (dp / pc * 100.0) if pc > 1e-9 else 0.0
    oi_change_pct = (float(chg) / float(prev_oi) * 100.0) if prev_oi > 0 else 0.0
    signal = interpret_oi_signal(float(dp), float(chg))
    heat01 = max(0.0, min(1.0, abs(oi_change_pct) / 12.0))
    return {
        "rank": rank,
        "symbol": symbol,
        "last_price": round(float(lp), 2),
        "prev_close": round(float(pc), 2),
        "price_change_pct": round(price_change_pct, 3),
        "oi": int(oi),
        "change_in_oi": int(chg),
        "oi_change_pct": round(oi_change_pct, 3),
        "signal": signal,
        "heat01": round(heat01, 4),
        "oi_source": oi_source,
    }


def _upstox_oi_change_vs_prior_daily(u: Any, instrument_key: str, current_oi: int) -> int:
    """
    Full market quote often omits change_in_oi. Use last two daily candles' OI (7th field) when present:
    current OI vs previous trading day's OI in the series.
    """
    if current_oi <= 0:
        return 0
    try:
        candles = u.get_historical_candles_by_instrument_key(
            instrument_key, interval="days/1", days_back=12
        )
    except Exception as e:
        logger.debug("dashboard_oi_heatmap: daily OI history %s: %s", instrument_key, e)
        return 0
    if not candles or len(candles) < 2:
        return 0
    sorted_c = sorted(candles, key=lambda c: str(c.get("timestamp") or ""))
    prev = sorted_c[-2]
    p_oi = prev.get("oi")
    if p_oi is None:
        return 0
    try:
        p_int = int(float(p_oi))
    except (TypeError, ValueError):
        return 0
    return int(current_oi) - p_int


def _try_upstox_heatmap_row(rank: int, symbol: str) -> Optional[Dict[str, Any]]:
    u = _upstox_service()
    if not u:
        return None
    ik = _future_key_from_arbitrage(symbol)
    if not ik:
        return None
    q = u.get_market_quote_by_key(ik)
    if not q:
        return None
    lp = float(q.get("last_price") or 0)
    ohlc = q.get("ohlc") if isinstance(q.get("ohlc"), dict) else {}
    open_ = float(ohlc.get("open") or 0)
    # Upstox: net_change = last_price − previous day's close (see Full Market Quotes API docs)
    net_chg = float(q.get("net_change") or 0)
    if abs(net_chg) > 1e-9:
        pc = lp - net_chg
    elif open_ > 1e-9:
        # Intraday vs session open when net_change not populated
        pc = open_
    else:
        pc = float(q.get("close_price") or ohlc.get("close") or 0)

    oi = int(q.get("oi") or 0)
    chg = int(q.get("change_in_oi") or 0)
    if chg == 0 and oi > 0:
        chg = _upstox_oi_change_vs_prior_daily(u, ik, oi)

    if lp <= 1e-9 and oi <= 0:
        return None
    return _heatmap_row_dict(rank, symbol, lp, pc, oi, chg, "upstox")


def _build_payload() -> Dict[str, Any]:
    symbols, source_tag = _resolve_top10_symbols()
    if not symbols:
        return {
            "success": True,
            "updated_at": datetime.now(IST).isoformat(),
            "cache_ttl_sec": int(_RESPONSE_CACHE_TTL_SEC),
            "symbols_source": "none",
            "rows": [],
            "message": "No premarket watchlist data (run the scheduled job or POST /scan/premarket-watchlist/run).",
        }

    fetcher = NSEOIFetcher()
    rows_out: List[Dict[str, Any]] = []

    for rank, sym in enumerate(symbols, start=1):
        raw: Optional[Dict[str, Any]] = None
        nse_err: Optional[str] = None
        try:
            raw = fetcher.get_oi(sym)
        except Exception as e:
            nse_err = str(e)
            logger.info("dashboard_oi_heatmap: NSE quote-derivative failed for %s: %s", sym, e)

        if raw is not None and _nse_derivative_usable(raw):
            oi = int(raw.get("oi") or 0)
            chg = int(raw.get("change_in_oi") or 0)
            lp = float(raw.get("last_price") or 0.0)
            pc = float(raw.get("prev_close") or 0.0)
            rows_out.append(_heatmap_row_dict(rank, sym, lp, pc, oi, chg, "nse"))
        else:
            ux = _try_upstox_heatmap_row(rank, sym)
            if ux:
                rows_out.append(ux)
            else:
                hint = nse_err or (
                    "NSE unreachable or empty; no Upstox future quote (check arbitrage_master currmth key)"
                )
                rows_out.append(
                    {
                        "rank": rank,
                        "symbol": sym,
                        "last_price": None,
                        "prev_close": None,
                        "price_change_pct": None,
                        "oi": None,
                        "change_in_oi": None,
                        "oi_change_pct": None,
                        "signal": "ERROR",
                        "heat01": 0.0,
                        "error": hint[:160],
                        "oi_source": "none",
                    }
                )

        time.sleep(_SLEEP_BETWEEN_NSE_CALLS_SEC)

    return {
        "success": True,
        "updated_at": datetime.now(IST).isoformat(),
        "cache_ttl_sec": int(_RESPONSE_CACHE_TTL_SEC),
        "symbols_source": source_tag,
        "rows": rows_out,
    }


def get_dashboard_oi_heatmap_response() -> Dict[str, Any]:
    """
    Return cached JSON payload for /scan/dashboard-oi-heatmap.
    Thread-safe; at most one NSE refresh runs at a time.
    """
    global _response_cache
    now = time.monotonic()
    if _response_cache is not None:
        ts, payload = _response_cache
        if now - ts < _RESPONSE_CACHE_TTL_SEC:
            out = dict(payload)
            out["cached"] = True
            out["cache_age_sec"] = round(now - ts, 2)
            return out

    with _refresh_lock:
        now = time.monotonic()
        if _response_cache is not None:
            ts, payload = _response_cache
            if now - ts < _RESPONSE_CACHE_TTL_SEC:
                out = dict(payload)
                out["cached"] = True
                out["cache_age_sec"] = round(now - ts, 2)
                return out
        try:
            payload = _build_payload()
        except Exception as e:
            logger.exception("dashboard_oi_heatmap: build failed: %s", e)
            payload = {
                "success": False,
                "updated_at": datetime.now(IST).isoformat(),
                "cache_ttl_sec": int(_RESPONSE_CACHE_TTL_SEC),
                "symbols_source": "error",
                "rows": [],
                "message": str(e),
            }
        _response_cache = (time.monotonic(), payload)

    out = dict(payload)
    out["cached"] = False
    out["cache_age_sec"] = 0.0
    return out

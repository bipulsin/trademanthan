"""
Dashboard: OI buildup heatmap for top 10 F&O names (premarket watchlist when available).

Uses NSE derivative quote API (see oi_integration.NSEOIFetcher) with a single shared
response cache and a refresh lock so concurrent dashboard users do not stampede NSE.
"""
from __future__ import annotations

import logging
import threading
import time
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

import pytz
from sqlalchemy import text
from sqlalchemy.orm import Session

from backend.database import SessionLocal
from backend.services.oi_integration import NSEOIFetcher, interpret_oi_signal
from backend.services.premarket_watchlist_job import fetch_premarket_watchlist_for_date

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")

# Slightly below client poll (180s) so browsers usually get a warm cache.
_RESPONSE_CACHE_TTL_SEC = 150.0
# Gentle spacing between NSE calls within one refresh (same session).
_SLEEP_BETWEEN_NSE_CALLS_SEC = 0.06
_TOP_N = 10

_response_cache: Optional[Tuple[float, Dict[str, Any]]] = None
_refresh_lock = threading.Lock()


def _session_today_ist() -> date:
    return datetime.now(IST).date()


def _symbols_from_premarket(session_date: date) -> List[str]:
    rows = fetch_premarket_watchlist_for_date(session_date)
    out: List[str] = []
    for r in rows:
        s = (r.get("stock") or "").strip().upper()
        if s and s not in out:
            out.append(s)
    return out[:_TOP_N]


def _symbols_from_arbitrage(db: Session, exclude: List[str], limit: int) -> List[str]:
    if limit <= 0:
        return []
    exc = {x.strip().upper() for x in exclude if x and str(x).strip()}
    rows = db.execute(
        text(
            """
            SELECT DISTINCT UPPER(TRIM(stock)) AS s
            FROM arbitrage_master
            WHERE stock IS NOT NULL AND TRIM(stock) <> ''
            ORDER BY s
            """
        )
    ).fetchall()
    out: List[str] = []
    for (sym,) in rows:
        if not sym:
            continue
        if sym in exc:
            continue
        out.append(sym)
        if len(out) >= limit:
            break
    return out


def _resolve_top10_symbols() -> Tuple[List[str], str]:
    """
    Prefer today's premarket_top list; pad with arbitrage_master names alphabetically if needed.
    """
    sd = _session_today_ist()
    primary = _symbols_from_premarket(sd)
    if len(primary) >= _TOP_N:
        return primary[:_TOP_N], "premarket"

    need = _TOP_N - len(primary)
    db = SessionLocal()
    try:
        extra = _symbols_from_arbitrage(db, primary, need)
    finally:
        db.close()

    merged = primary + [s for s in extra if s not in primary]
    if len(merged) >= _TOP_N:
        return merged[:_TOP_N], "premarket+arbitrage" if primary else "arbitrage"

    return merged, "premarket+arbitrage" if primary else "arbitrage"


def _build_payload() -> Dict[str, Any]:
    symbols, source_tag = _resolve_top10_symbols()
    if not symbols:
        return {
            "success": True,
            "updated_at": datetime.now(IST).isoformat(),
            "cache_ttl_sec": int(_RESPONSE_CACHE_TTL_SEC),
            "symbols_source": "none",
            "rows": [],
            "message": "No F&O universe (premarket empty and arbitrage_master empty).",
        }

    fetcher = NSEOIFetcher()
    rows_out: List[Dict[str, Any]] = []

    for rank, sym in enumerate(symbols, start=1):
        try:
            raw = fetcher.get_oi(sym)
            oi = int(raw.get("oi") or 0)
            chg = int(raw.get("change_in_oi") or 0)
            lp = float(raw.get("last_price") or 0.0)
            pc = float(raw.get("prev_close") or 0.0)
            prev_oi = max(0, int(raw.get("prev_oi") or max(0, oi - chg)))

            dp = lp - pc
            price_change_pct = (dp / pc * 100.0) if pc > 1e-9 else 0.0
            oi_change_pct = (chg / float(prev_oi) * 100.0) if prev_oi > 0 else 0.0

            signal = interpret_oi_signal(float(dp), float(chg))
            # Intensity 0..1 from absolute OI % move (cap at 12% day move for display scaling)
            heat01 = max(0.0, min(1.0, abs(oi_change_pct) / 12.0))

            rows_out.append(
                {
                    "rank": rank,
                    "symbol": sym,
                    "last_price": round(lp, 2),
                    "prev_close": round(pc, 2),
                    "price_change_pct": round(price_change_pct, 3),
                    "oi": oi,
                    "change_in_oi": chg,
                    "oi_change_pct": round(oi_change_pct, 3),
                    "signal": signal,
                    "heat01": round(heat01, 4),
                }
            )
        except Exception as e:
            logger.warning("dashboard_oi_heatmap: %s failed: %s", sym, e)
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
                    "error": str(e)[:120],
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

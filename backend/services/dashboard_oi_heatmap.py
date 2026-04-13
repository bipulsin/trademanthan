"""
Dashboard: OI buildup heatmap for top 10 F&O names (premarket watchlist when available).

Uses NSE derivative quote API (see oi_integration.NSEOIFetcher) with a single shared
response cache and a refresh lock so concurrent dashboard users do not stampede NSE.
"""
from __future__ import annotations

import logging
import os
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


def _heatmap_premarket_marker_path(session_date: date) -> str:
    """Exclusive marker so multiple app workers do not each run the 200-symbol premarket job."""
    return f"/tmp/tm_heatmap_premarket_{session_date.isoformat()}.lock"


def _try_create_exclusive_premarket_marker(session_date: date) -> bool:
    path = _heatmap_premarket_marker_path(session_date)
    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
        os.close(fd)
        return True
    except FileExistsError:
        return False


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

    Order of preference:
    1) Today's persisted premarket_watchlist (rank 1..10) when present.
    2) If today has no rows: on IST weekdays, run ``run_premarket_watchlist_job()`` at most once
       per calendar day (mirrors scheduler), then re-read today.
    3) If still empty (e.g. weekend, holiday, or job insufficient_data): most recent session in DB
       with 10 rows, else most recent session with any rows.
    4) Partial today (1..9 rows): use those in rank order (no padding).
    """
    today = _session_today_ist()
    rows_today = fetch_premarket_watchlist_for_date(today)

    if len(rows_today) >= _TOP_N:
        return _symbols_in_rank_order(rows_today), "premarket_today"

    if len(rows_today) > 0:
        return _symbols_in_rank_order(rows_today), "premarket_today_partial"

    # No rows for today — optional one-shot same computation as scheduler (weekdays only).
    now_ist = datetime.now(IST)
    if now_ist.weekday() < 5:
        exclusive = _try_create_exclusive_premarket_marker(today)
        if exclusive:
            try:
                out = run_premarket_watchlist_job()
                if isinstance(out, dict) and out.get("skipped"):
                    logger.info("dashboard_oi_heatmap: premarket job skipped: %s", out.get("skipped"))
            except Exception as e:
                logger.exception("dashboard_oi_heatmap: premarket job failed: %s", e)
            rows_today = fetch_premarket_watchlist_for_date(today)
        else:
            rows_today = _wait_for_premarket_rows(today, 1)

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

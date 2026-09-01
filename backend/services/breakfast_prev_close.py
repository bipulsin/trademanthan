"""Breakfast prev-session close prefill job (benchmarks + arbitrage_master FUT)."""
from __future__ import annotations

import logging
import time
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import text

from backend.config import settings
from backend.database import SessionLocal, engine
from backend.services import market_holiday as mh
from backend.services.upstox_service import UpstoxService

logger = logging.getLogger(__name__)
_THROTTLE_SEC = 0.12


def parse_daily_bars(candles: List[dict]) -> List[Tuple[date, float]]:
    out: List[Tuple[date, float]] = []
    for c in candles or []:
        ts = str(c.get("timestamp") or "")
        cl = float(c.get("close") or 0)
        if len(ts) < 10 or cl <= 0:
            continue
        try:
            d = datetime.strptime(ts[:10], "%Y-%m-%d").date()
        except ValueError:
            continue
        out.append((d, cl))
    out.sort(key=lambda x: x[0])
    return out


def daily_settled_for_ist(now_ist: datetime) -> bool:
    """True when today's NSE session daily bar should be treated as complete."""
    now_ist = mh._normalize_ist(now_ist)
    if mh.should_skip_scheduled_market_jobs_ist(now_ist):
        return True
    cutoff = now_ist.replace(hour=15, minute=35, second=0, microsecond=0)
    return now_ist >= cutoff


def latest_settled_daily_close(
    candles: List[dict],
    *,
    now_ist: datetime,
) -> Tuple[Optional[date], Optional[float]]:
    """Most recent completed session daily close relative to now (IST)."""
    bars = parse_daily_bars(candles)
    if not bars:
        return None, None
    today = mh._normalize_ist(now_ist).date()
    if daily_settled_for_ist(now_ist) and bars[-1][0] == today:
        return bars[-1]
    for d, cl in reversed(bars):
        if d < today:
            return d, cl
    return None, None


def _source_tag(trigger: str) -> str:
    return f"upstox_days/1@{trigger}"


def _upsert_benchmark_prev_close(
    instrument_key: str,
    close_date: date,
    close_px: float,
    source: str,
) -> bool:
    with engine.begin() as conn:
        n = conn.execute(
            text(
                """
                UPDATE nifty_benchmark_reference
                SET prev_session_close = :px,
                    prev_session_close_for_date = CAST(:for_date AS date),
                    prev_session_close_source = :src,
                    updated_at = NOW()
                WHERE instrument_key = :ik
                  AND (
                      prev_session_close_for_date IS NULL
                      OR prev_session_close_for_date <= CAST(:for_date AS date)
                  )
                """
            ),
            {"ik": instrument_key, "px": close_px, "for_date": close_date.isoformat(), "src": source},
        ).rowcount
    return int(n or 0) > 0


def _upsert_stock_prev_close(
    stock: str,
    close_date: date,
    close_px: float,
    source: str,
) -> bool:
    with engine.begin() as conn:
        n = conn.execute(
            text(
                """
                UPDATE arbitrage_master
                SET prev_session_close = :px,
                    prev_session_close_for_date = CAST(:for_date AS date),
                    prev_session_close_source = :src
                WHERE UPPER(TRIM(stock)) = UPPER(TRIM(:stock))
                  AND (
                      prev_session_close_for_date IS NULL
                      OR prev_session_close_for_date <= CAST(:for_date AS date)
                  )
                """
            ),
            {"stock": stock, "px": close_px, "for_date": close_date.isoformat(), "src": source},
        ).rowcount
    return int(n or 0) > 0


def load_stored_prev_closes() -> Tuple[Dict[str, float], Dict[str, float]]:
    """instrument_key → prev close (Nifty + sectors) and stock symbol → prev close."""
    bench: Dict[str, float] = {}
    stocks: Dict[str, float] = {}
    db = SessionLocal()
    try:
        for row in db.execute(
            text(
                """
                SELECT instrument_key, prev_session_close
                FROM nifty_benchmark_reference
                WHERE prev_session_close IS NOT NULL AND prev_session_close > 0
                """
            )
        ).mappings():
            ik = str(row.get("instrument_key") or "").strip()
            px = float(row.get("prev_session_close") or 0)
            if ik and px > 0:
                bench[ik] = px
        for row in db.execute(
            text(
                """
                SELECT UPPER(TRIM(stock)) AS stock, prev_session_close
                FROM arbitrage_master
                WHERE prev_session_close IS NOT NULL AND prev_session_close > 0
                  AND stock IS NOT NULL
                """
            )
        ).mappings():
            sym = str(row.get("stock") or "").strip().upper()
            px = float(row.get("prev_session_close") or 0)
            if sym and px > 0:
                stocks[sym] = px
    except Exception as e:
        logger.warning("load_stored_prev_closes failed: %s", e)
    finally:
        db.close()
    return bench, stocks


def run_breakfast_prev_close_job(*, trigger: str = "manual") -> Dict[str, Any]:
    """Idempotent UPSERT of prev_session_close on benchmarks + arbitrage_master FUT rows."""
    now = mh._normalize_ist(None)
    if mh.should_skip_scheduled_market_jobs_ist(now) and trigger.startswith("scheduled"):
        return {"ok": True, "skipped": "holiday_or_weekend", "trigger": trigger}

    ux = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    ux.reload_token_from_storage()
    source = _source_tag(trigger)

    db = SessionLocal()
    try:
        benchmarks = db.execute(
            text("SELECT instrument_key FROM nifty_benchmark_reference ORDER BY instrument_key")
        ).scalars().all()
        stocks = db.execute(
            text(
                """
                SELECT TRIM(stock) AS stock, TRIM(currmth_future_instrument_key) AS fut_key
                FROM arbitrage_master
                WHERE stock IS NOT NULL
                  AND currmth_future_instrument_key IS NOT NULL
                  AND TRIM(currmth_future_instrument_key) <> ''
                ORDER BY stock
                """
            )
        ).mappings().all()
    finally:
        db.close()

    bench_updated = bench_skipped = 0
    for ik in benchmarks:
        key = str(ik or "").strip()
        if not key:
            continue
        try:
            candles = ux.get_historical_candles_by_instrument_key(
                key, interval="days/1", days_back=12, range_end_date=now.date()
            ) or []
            d, px = latest_settled_daily_close(candles, now_ist=now)
            if d is None or px is None:
                bench_skipped += 1
                continue
            if _upsert_benchmark_prev_close(key, d, float(px), source):
                bench_updated += 1
            else:
                bench_skipped += 1
        except Exception as e:
            bench_skipped += 1
            logger.warning("breakfast prev_close benchmark %s: %s", key, e)
        time.sleep(_THROTTLE_SEC)

    stock_updated = stock_skipped = 0
    for row in stocks:
        sym = str(row.get("stock") or "").strip().upper()
        fut_key = str(row.get("fut_key") or "").strip()
        if not sym or not fut_key:
            stock_skipped += 1
            continue
        try:
            candles = ux.get_historical_candles_by_instrument_key(
                fut_key, interval="days/1", days_back=12, range_end_date=now.date()
            ) or []
            d, px = latest_settled_daily_close(candles, now_ist=now)
            if d is None or px is None:
                stock_skipped += 1
                continue
            if _upsert_stock_prev_close(sym, d, float(px), source):
                stock_updated += 1
            else:
                stock_skipped += 1
        except Exception as e:
            stock_skipped += 1
            logger.debug("breakfast prev_close stock %s: %s", sym, e)
        time.sleep(_THROTTLE_SEC)

    out = {
        "ok": True,
        "trigger": trigger,
        "source": source,
        "benchmarks": {"updated": bench_updated, "skipped": bench_skipped, "total": len(benchmarks)},
        "stocks": {"updated": stock_updated, "skipped": stock_skipped, "total": len(stocks)},
    }
    logger.info("breakfast_prev_close_job: %s", out)
    return out

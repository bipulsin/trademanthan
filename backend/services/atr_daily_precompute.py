"""Nightly ATR(14)% precompute for arbitrage_master current-month futures.

Persists Wilder ATR14 / yesterday-close × 100 (same formula as
``rs_scanner_maturity.compute_yesterday_range_metrics``) into
``atr_daily_precomputed``, keyed by (as_of_date, symbol) with full history.

Never writes sentinel 0.0 on failure — rows get NULL metrics +
``computation_failed=true`` (or are skipped when the fetch never yields a
usable candle set after retries).
"""
from __future__ import annotations

import logging
import time
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence, Tuple

from sqlalchemy import text

from backend.config import settings
from backend.database import SessionLocal, engine
from backend.services.market_holiday import IST, should_skip_scheduled_market_jobs_ist
from backend.services.rs_scanner_maturity import (
    compute_yesterday_range_metrics,
    today_ist,
)

logger = logging.getLogger(__name__)

_FETCH_RETRIES = 3
_BACKOFF_SEC = (1.0, 2.0, 4.0)
_DAYS_BACK = 30

_ENSURE_SQL = """
CREATE TABLE IF NOT EXISTS atr_daily_precomputed (
    as_of_date DATE NOT NULL,
    symbol TEXT NOT NULL,
    instrument_key TEXT,
    atr14 DOUBLE PRECISION,
    atr14_pct DOUBLE PRECISION,
    daily_range_pct DOUBLE PRECISION,
    range_vs_atr_ratio DOUBLE PRECISION,
    y_close DOUBLE PRECISION,
    computation_failed BOOLEAN NOT NULL DEFAULT FALSE,
    fail_reason TEXT,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (as_of_date, symbol)
);
CREATE INDEX IF NOT EXISTS idx_atr_daily_precomputed_symbol_date
    ON atr_daily_precomputed (symbol, as_of_date DESC);
CREATE INDEX IF NOT EXISTS idx_atr_daily_precomputed_as_of
    ON atr_daily_precomputed (as_of_date DESC);

CREATE TABLE IF NOT EXISTS atr_daily_precompute_runs (
    id BIGSERIAL PRIMARY KEY,
    run_started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    run_finished_at TIMESTAMPTZ,
    as_of_date DATE NOT NULL,
    trigger TEXT NOT NULL DEFAULT 'scheduled',
    universe_n INTEGER NOT NULL DEFAULT 0,
    succeeded INTEGER NOT NULL DEFAULT 0,
    failed INTEGER NOT NULL DEFAULT 0,
    failed_symbols TEXT[],
    notes TEXT
);
CREATE INDEX IF NOT EXISTS idx_atr_daily_precompute_runs_as_of
    ON atr_daily_precompute_runs (as_of_date DESC);
"""

_UPSERT_SQL = text(
    """
    INSERT INTO atr_daily_precomputed (
        as_of_date, symbol, instrument_key,
        atr14, atr14_pct, daily_range_pct, range_vs_atr_ratio, y_close,
        computation_failed, fail_reason, computed_at
    ) VALUES (
        CAST(:as_of_date AS DATE), :symbol, :instrument_key,
        :atr14, :atr14_pct, :daily_range_pct, :range_vs_atr_ratio, :y_close,
        :computation_failed, :fail_reason, NOW()
    )
    ON CONFLICT (as_of_date, symbol) DO UPDATE SET
        instrument_key = EXCLUDED.instrument_key,
        atr14 = EXCLUDED.atr14,
        atr14_pct = EXCLUDED.atr14_pct,
        daily_range_pct = EXCLUDED.daily_range_pct,
        range_vs_atr_ratio = EXCLUDED.range_vs_atr_ratio,
        y_close = EXCLUDED.y_close,
        computation_failed = EXCLUDED.computation_failed,
        fail_reason = EXCLUDED.fail_reason,
        computed_at = NOW()
    """
)

_READ_ONE_SQL = text(
    """
    SELECT atr14, atr14_pct, daily_range_pct, range_vs_atr_ratio, y_close,
           computation_failed, fail_reason, instrument_key, computed_at
    FROM atr_daily_precomputed
    WHERE as_of_date = CAST(:d AS DATE) AND UPPER(symbol) = UPPER(:sym)
    LIMIT 1
    """
)

_LATEST_SQL = text(
    """
    SELECT as_of_date, atr14, atr14_pct, daily_range_pct, range_vs_atr_ratio,
           y_close, computation_failed, fail_reason, instrument_key, computed_at
    FROM atr_daily_precomputed
    WHERE UPPER(symbol) = UPPER(:sym)
      AND computation_failed = FALSE
      AND atr14_pct IS NOT NULL AND atr14_pct > 0
    ORDER BY as_of_date DESC
    LIMIT 1
    """
)


def ensure_atr_daily_precompute_tables() -> None:
    if engine is None:
        return
    with engine.begin() as conn:
        for stmt in _ENSURE_SQL.split(";"):
            s = stmt.strip()
            if s:
                conn.execute(text(s))


def next_nse_session_date(from_d: date) -> date:
    """Next weekday that is not an NSE holiday after ``from_d``."""
    from backend.services import market_holiday as mh

    holidays = mh._holiday_dates_cached()
    cand = from_d + timedelta(days=1)
    for _ in range(14):
        if cand.weekday() < 5 and cand not in holidays:
            return cand
        cand += timedelta(days=1)
    return from_d + timedelta(days=1)


def list_currmth_future_universe() -> List[Tuple[str, str]]:
    """Return [(symbol, currmth_future_instrument_key), ...] from arbitrage_master."""
    if SessionLocal is None:
        return []
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT UPPER(TRIM(stock)) AS symbol,
                       TRIM(currmth_future_instrument_key) AS ikey
                FROM arbitrage_master
                WHERE stock IS NOT NULL
                  AND TRIM(stock) <> ''
                  AND currmth_future_instrument_key IS NOT NULL
                  AND TRIM(currmth_future_instrument_key) <> ''
                ORDER BY stock
                """
            )
        ).fetchall()
        out: List[Tuple[str, str]] = []
        seen = set()
        for r in rows:
            sym = (r.symbol or "").strip().upper()
            ikey = (r.ikey or "").strip()
            if not sym or not ikey or sym in seen:
                continue
            seen.add(sym)
            out.append((sym, ikey))
        return out
    finally:
        db.close()


def _metrics_valid(dr: float, atr_pct: float, ratio: float) -> bool:
    return atr_pct is not None and float(atr_pct) > 0


def try_compute_yesterday_range_metrics(
    daily_candles: List[Dict],
    *,
    as_of_date: str,
) -> Optional[Tuple[float, float, float]]:
    """Like compute_yesterday_range_metrics but returns None instead of (0,0,0)."""
    if not daily_candles:
        return None
    dr, atr_pct, ratio = compute_yesterday_range_metrics(
        daily_candles, as_of_date=as_of_date
    )
    if not _metrics_valid(dr, atr_pct, ratio):
        return None
    return float(dr), float(atr_pct), float(ratio)


def _fetch_daily_candles_with_retry(
    upstox: Any,
    instrument_key: str,
    *,
    as_of: date,
    symbol: str,
) -> Tuple[Optional[List[Dict]], Optional[str]]:
    last_err: Optional[str] = None
    for attempt in range(_FETCH_RETRIES):
        try:
            candles = upstox.get_historical_candles_by_instrument_key(
                instrument_key,
                interval="days/1",
                days_back=_DAYS_BACK,
                range_end_date=as_of,
            )
            if candles:
                return list(candles), None
            last_err = "empty_candles"
        except Exception as exc:
            last_err = f"{type(exc).__name__}: {exc}"
            logger.warning(
                "atr_daily_precompute: fetch %s attempt %s/%s failed: %s",
                symbol,
                attempt + 1,
                _FETCH_RETRIES,
                last_err,
            )
        if attempt < _FETCH_RETRIES - 1:
            time.sleep(_BACKOFF_SEC[min(attempt, len(_BACKOFF_SEC) - 1)])
    return None, last_err or "fetch_failed"


def _y_close_from_metrics(candles: List[Dict], as_of_date: str) -> Optional[float]:
    """Best-effort yesterday close used in atr14_pct (for audit columns)."""
    from backend.services.rs_scanner_maturity import _parse_ist_date, _sorted_daily_candles, _f

    sorted_c = _sorted_daily_candles(candles)
    if len(sorted_c) < 2:
        return None
    dates = [_parse_ist_date(c.get("timestamp")) for c in sorted_c]
    if dates and dates[-1] == as_of_date:
        sorted_c = sorted_c[:-1]
    if not sorted_c:
        return None
    yc = _f(sorted_c[-1].get("close"))
    return yc if yc > 0 else None


def compute_one_symbol(
    upstox: Any,
    symbol: str,
    instrument_key: str,
    as_of_date: str,
) -> Dict[str, Any]:
    """Compute + return upsert payload for one symbol (never uses 0.0 sentinel)."""
    as_of = date.fromisoformat(as_of_date)
    candles, err = _fetch_daily_candles_with_retry(
        upstox, instrument_key, as_of=as_of, symbol=symbol
    )
    if not candles:
        return {
            "as_of_date": as_of_date,
            "symbol": symbol,
            "instrument_key": instrument_key,
            "atr14": None,
            "atr14_pct": None,
            "daily_range_pct": None,
            "range_vs_atr_ratio": None,
            "y_close": None,
            "computation_failed": True,
            "fail_reason": err or "empty_candles",
            "ok": False,
        }

    metrics = try_compute_yesterday_range_metrics(candles, as_of_date=as_of_date)
    if metrics is None:
        return {
            "as_of_date": as_of_date,
            "symbol": symbol,
            "instrument_key": instrument_key,
            "atr14": None,
            "atr14_pct": None,
            "daily_range_pct": None,
            "range_vs_atr_ratio": None,
            "y_close": _y_close_from_metrics(candles, as_of_date),
            "computation_failed": True,
            "fail_reason": "insufficient_history_or_invalid_metrics",
            "ok": False,
        }

    dr, atr_pct, ratio = metrics
    y_close = _y_close_from_metrics(candles, as_of_date)
    atr14 = None
    if y_close and y_close > 0 and atr_pct > 0:
        atr14 = atr_pct / 100.0 * y_close
    return {
        "as_of_date": as_of_date,
        "symbol": symbol,
        "instrument_key": instrument_key,
        "atr14": round(atr14, 6) if atr14 is not None else None,
        "atr14_pct": round(atr_pct, 4),
        "daily_range_pct": round(dr, 4),
        "range_vs_atr_ratio": round(ratio, 4),
        "y_close": round(y_close, 4) if y_close else None,
        "computation_failed": False,
        "fail_reason": None,
        "ok": True,
    }


def upsert_precomputed_row(db: Any, row: Dict[str, Any]) -> None:
    db.execute(
        _UPSERT_SQL,
        {
            "as_of_date": row["as_of_date"],
            "symbol": row["symbol"],
            "instrument_key": row.get("instrument_key"),
            "atr14": row.get("atr14"),
            "atr14_pct": row.get("atr14_pct"),
            "daily_range_pct": row.get("daily_range_pct"),
            "range_vs_atr_ratio": row.get("range_vs_atr_ratio"),
            "y_close": row.get("y_close"),
            "computation_failed": bool(row.get("computation_failed")),
            "fail_reason": row.get("fail_reason"),
        },
    )


def get_precomputed_atr(
    symbol: str,
    as_of_date: str,
    *,
    db: Any = None,
) -> Optional[Dict[str, Any]]:
    """Return valid precomputed metrics for (symbol, as_of_date), else None."""
    own = db is None
    if own:
        if SessionLocal is None:
            return None
        db = SessionLocal()
    try:
        r = db.execute(_READ_ONE_SQL, {"d": as_of_date, "sym": symbol}).fetchone()
        if not r:
            return None
        if r.computation_failed or r.atr14_pct is None or float(r.atr14_pct) <= 0:
            return None
        return {
            "atr14": float(r.atr14) if r.atr14 is not None else None,
            "atr14_pct": float(r.atr14_pct),
            "daily_range_pct": float(r.daily_range_pct) if r.daily_range_pct is not None else None,
            "range_vs_atr_ratio": (
                float(r.range_vs_atr_ratio) if r.range_vs_atr_ratio is not None else None
            ),
            "y_close": float(r.y_close) if r.y_close is not None else None,
            "instrument_key": r.instrument_key,
            "computed_at": r.computed_at,
        }
    finally:
        if own:
            db.close()


def get_latest_precomputed_atr(symbol: str, *, db: Any = None) -> Optional[Dict[str, Any]]:
    """Latest successful ATR row for symbol (any as_of_date)."""
    own = db is None
    if own:
        if SessionLocal is None:
            return None
        db = SessionLocal()
    try:
        r = db.execute(_LATEST_SQL, {"sym": symbol}).fetchone()
        if not r:
            return None
        return {
            "as_of_date": str(r.as_of_date),
            "atr14": float(r.atr14) if r.atr14 is not None else None,
            "atr14_pct": float(r.atr14_pct),
            "daily_range_pct": float(r.daily_range_pct) if r.daily_range_pct is not None else None,
            "range_vs_atr_ratio": (
                float(r.range_vs_atr_ratio) if r.range_vs_atr_ratio is not None else None
            ),
            "y_close": float(r.y_close) if r.y_close is not None else None,
            "instrument_key": r.instrument_key,
            "computed_at": r.computed_at,
        }
    finally:
        if own:
            db.close()


def _record_run(
    db: Any,
    *,
    as_of_date: str,
    trigger: str,
    universe_n: int,
    succeeded: int,
    failed: int,
    failed_symbols: List[str],
    notes: Optional[str] = None,
    started_at: Optional[datetime] = None,
) -> None:
    db.execute(
        text(
            """
            INSERT INTO atr_daily_precompute_runs (
                run_started_at, run_finished_at, as_of_date, trigger,
                universe_n, succeeded, failed, failed_symbols, notes
            ) VALUES (
                :started, NOW(), CAST(:as_of AS DATE), :trigger,
                :universe_n, :succeeded, :failed, :failed_symbols, :notes
            )
            """
        ),
        {
            "started": started_at or datetime.now(IST),
            "as_of": as_of_date,
            "trigger": trigger,
            "universe_n": universe_n,
            "succeeded": succeeded,
            "failed": failed,
            "failed_symbols": failed_symbols or None,
            "notes": notes,
        },
    )


def run_atr_daily_precompute_job(
    *,
    as_of_date: Optional[str] = None,
    trigger: str = "scheduled",
    symbols: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    """
    Precompute ATR(14)% for every current-month future in arbitrage_master.

    Default ``as_of_date`` is the next NSE session after today IST (nightly
    after-close prepares tomorrow's intraday consumers).
    """
    ensure_atr_daily_precompute_tables()
    started = datetime.now(IST)

    if as_of_date:
        as_of = as_of_date
    else:
        today = datetime.now(IST).date()
        as_of = next_nse_session_date(today).isoformat()

    universe = list_currmth_future_universe()
    if symbols:
        want = {s.strip().upper() for s in symbols if s and str(s).strip()}
        universe = [(s, k) for s, k in universe if s in want]

    if not universe:
        out = {
            "ok": False,
            "error": "empty_universe",
            "as_of_date": as_of,
            "trigger": trigger,
            "succeeded": 0,
            "failed": 0,
            "failed_symbols": [],
        }
        logger.warning("atr_daily_precompute: empty universe as_of=%s", as_of)
        return out

    try:
        from backend.services.upstox_service import UpstoxService

        upstox = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    except Exception as exc:
        logger.error("atr_daily_precompute: Upstox init failed: %s", exc)
        return {"ok": False, "error": str(exc), "as_of_date": as_of, "trigger": trigger}

    succeeded = 0
    failed_symbols: List[str] = []
    if SessionLocal is None:
        return {"ok": False, "error": "no SessionLocal", "as_of_date": as_of}

    db = SessionLocal()
    try:
        for sym, ikey in universe:
            row = compute_one_symbol(upstox, sym, ikey, as_of)
            upsert_precomputed_row(db, row)
            if row.get("ok"):
                succeeded += 1
            else:
                failed_symbols.append(sym)
            # Light pacing to reduce Upstox 429 pressure on large universes
            time.sleep(0.05)
        failed = len(failed_symbols)
        _record_run(
            db,
            as_of_date=as_of,
            trigger=trigger,
            universe_n=len(universe),
            succeeded=succeeded,
            failed=failed,
            failed_symbols=failed_symbols,
            started_at=started,
        )
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()

    summary = {
        "ok": True,
        "as_of_date": as_of,
        "trigger": trigger,
        "universe_n": len(universe),
        "succeeded": succeeded,
        "failed": len(failed_symbols),
        "failed_symbols": failed_symbols,
        "elapsed_sec": round((datetime.now(IST) - started).total_seconds(), 1),
    }
    logger.info(
        "atr_daily_precompute summary: as_of=%s succeeded=%s failed=%s failed_symbols=%s",
        as_of,
        succeeded,
        len(failed_symbols),
        failed_symbols[:40],
    )
    return summary


def run_atr_daily_precompute_backfill(
    start_date: str,
    end_date: str,
    *,
    trigger: str = "backfill",
    patch_rs_scanner_history: bool = True,
) -> Dict[str, Any]:
    """Run precompute for each calendar day in [start, end] (inclusive), skipping weekends/holidays."""
    ensure_atr_daily_precompute_tables()
    from backend.services import market_holiday as mh

    holidays = mh._holiday_dates_cached()
    d0 = date.fromisoformat(start_date)
    d1 = date.fromisoformat(end_date)
    days: List[str] = []
    cur = d0
    while cur <= d1:
        if cur.weekday() < 5 and cur not in holidays:
            days.append(cur.isoformat())
        cur += timedelta(days=1)

    per_day: List[Dict[str, Any]] = []
    for d in days:
        per_day.append(run_atr_daily_precompute_job(as_of_date=d, trigger=trigger))

    history_patch: Optional[Dict[str, Any]] = None
    if patch_rs_scanner_history:
        history_patch = patch_rs_scanner_history_zeros_from_precomputed(start_date, end_date)

    return {
        "ok": True,
        "start_date": start_date,
        "end_date": end_date,
        "days": days,
        "per_day": per_day,
        "history_patch": history_patch,
    }


def patch_rs_scanner_history_zeros_from_precomputed(
    start_date: str,
    end_date: str,
) -> Dict[str, Any]:
    """Replace rs_scanner_history atr14_pct=0/NULL with precomputed positives in window."""
    if SessionLocal is None:
        return {"ok": False, "error": "no SessionLocal"}
    db = SessionLocal()
    try:
        before = db.execute(
            text(
                """
                SELECT COUNT(*) FROM rs_scanner_history
                WHERE date BETWEEN CAST(:a AS DATE) AND CAST(:b AS DATE)
                  AND (atr14_pct IS NULL OR atr14_pct = 0)
                """
            ),
            {"a": start_date, "b": end_date},
        ).scalar() or 0

        result = db.execute(
            text(
                """
                UPDATE rs_scanner_history h
                SET
                    atr14_pct = p.atr14_pct,
                    daily_range_pct = COALESCE(p.daily_range_pct, h.daily_range_pct),
                    range_vs_atr_ratio = COALESCE(p.range_vs_atr_ratio, h.range_vs_atr_ratio)
                FROM atr_daily_precomputed p
                WHERE h.date = p.as_of_date
                  AND UPPER(h.symbol) = UPPER(p.symbol)
                  AND h.date BETWEEN CAST(:a AS DATE) AND CAST(:b AS DATE)
                  AND (h.atr14_pct IS NULL OR h.atr14_pct = 0)
                  AND p.computation_failed = FALSE
                  AND p.atr14_pct IS NOT NULL
                  AND p.atr14_pct > 0
                """
            ),
            {"a": start_date, "b": end_date},
        )
        updated = result.rowcount or 0

        after = db.execute(
            text(
                """
                SELECT COUNT(*) FROM rs_scanner_history
                WHERE date BETWEEN CAST(:a AS DATE) AND CAST(:b AS DATE)
                  AND (atr14_pct IS NULL OR atr14_pct = 0)
                """
            ),
            {"a": start_date, "b": end_date},
        ).scalar() or 0

        remaining = db.execute(
            text(
                """
                SELECT h.date::text AS d, h.symbol
                FROM rs_scanner_history h
                LEFT JOIN atr_daily_precomputed p
                  ON p.as_of_date = h.date AND UPPER(p.symbol) = UPPER(h.symbol)
                     AND p.computation_failed = FALSE
                     AND p.atr14_pct IS NOT NULL AND p.atr14_pct > 0
                WHERE h.date BETWEEN CAST(:a AS DATE) AND CAST(:b AS DATE)
                  AND (h.atr14_pct IS NULL OR h.atr14_pct = 0)
                ORDER BY h.date, h.symbol
                """
            ),
            {"a": start_date, "b": end_date},
        ).fetchall()
        remaining_list = [{"date": r.d, "symbol": r.symbol} for r in remaining]
        db.commit()
        return {
            "ok": True,
            "zero_or_null_before": int(before),
            "rows_updated": int(updated),
            "zero_or_null_after": int(after),
            "remaining_failures": remaining_list,
            "remaining_n": len(remaining_list),
        }
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def get_nightly_status(as_of_date: Optional[str] = None) -> Dict[str, Any]:
    """Queryable nightly job status + coverage vs arbitrage_master."""
    ensure_atr_daily_precompute_tables()
    as_of = as_of_date or today_ist()
    if SessionLocal is None:
        return {"ok": False, "error": "no SessionLocal", "as_of_date": as_of}
    db = SessionLocal()
    try:
        universe_n = db.execute(
            text(
                """
                SELECT COUNT(*) FROM arbitrage_master
                WHERE stock IS NOT NULL
                  AND currmth_future_instrument_key IS NOT NULL
                  AND TRIM(currmth_future_instrument_key) <> ''
                """
            )
        ).scalar() or 0
        fresh = db.execute(
            text(
                """
                SELECT COUNT(*) FROM atr_daily_precomputed
                WHERE as_of_date = CAST(:d AS DATE)
                  AND computation_failed = FALSE
                  AND atr14_pct IS NOT NULL AND atr14_pct > 0
                """
            ),
            {"d": as_of},
        ).scalar() or 0
        failed_n = db.execute(
            text(
                """
                SELECT COUNT(*) FROM atr_daily_precomputed
                WHERE as_of_date = CAST(:d AS DATE) AND computation_failed = TRUE
                """
            ),
            {"d": as_of},
        ).scalar() or 0
        last_run = db.execute(
            text(
                """
                SELECT id, run_started_at, run_finished_at, as_of_date::text,
                       trigger, universe_n, succeeded, failed, failed_symbols, notes
                FROM atr_daily_precompute_runs
                WHERE as_of_date = CAST(:d AS DATE)
                ORDER BY id DESC
                LIMIT 1
                """
            ),
            {"d": as_of},
        ).fetchone()
        coverage_pct = round(100.0 * float(fresh) / float(universe_n), 2) if universe_n else 0.0
        run_info = None
        if last_run:
            run_info = {
                "id": last_run.id,
                "run_started_at": (
                    last_run.run_started_at.isoformat() if last_run.run_started_at else None
                ),
                "run_finished_at": (
                    last_run.run_finished_at.isoformat() if last_run.run_finished_at else None
                ),
                "as_of_date": last_run.as_of_date,
                "trigger": last_run.trigger,
                "universe_n": last_run.universe_n,
                "succeeded": last_run.succeeded,
                "failed": last_run.failed,
                "failed_symbols": list(last_run.failed_symbols or []),
                "notes": last_run.notes,
            }
        return {
            "ok": True,
            "as_of_date": as_of,
            "arbitrage_master_currmth_n": int(universe_n),
            "fresh_atr_n": int(fresh),
            "failed_atr_n": int(failed_n),
            "coverage_pct": coverage_pct,
            "job_ran": run_info is not None,
            "last_run": run_info,
        }
    finally:
        db.close()


def scheduled_tick_should_run() -> bool:
    """True when weekday/non-holiday — used by scheduler tick."""
    return not should_skip_scheduled_market_jobs_ist()

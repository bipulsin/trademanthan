"""MomentX Stage-1 backtest engine — qualify stocks per trading day for any date range.

Reusable for month/range runs. Evaluates live Stage-1 filters (``evaluate_stage1``)
at a configurable as-of clock (default scan cutoff 14:45 IST) on each NSE session.
"""
from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, time as dt_time, timedelta
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pytz

from backend.services.momentx.backtest_store import (
    create_run,
    delete_qualified_in_range,
    fetch_run,
    insert_qualified_batch,
    mark_run,
)
from backend.services.momentx.candles import candle_date, sort_candles
from backend.services.momentx.config import CONFIG, MomentXConfig
from backend.services.momentx.filter import evaluate_stage1
from backend.services.momentx.universe import load_momentx_universe

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

# Upstox minutes/15 historical span ~1 month — fetch in overlapping windows.
_M15_WINDOW_DAYS = 28
_M15_LOOKBACK_CAL_DAYS = 14  # warm-up for RSI/ADX/volume SMA ahead of batch start

_RUN_LOCK = threading.Lock()
_ACTIVE_RUN_ID: Optional[int] = None


def iter_trading_days(d0: date, d1: date, holidays: Optional[set] = None) -> List[date]:
    hol = holidays if holidays is not None else _load_holidays(d0, d1)
    out: List[date] = []
    d = d0
    while d <= d1:
        if d.weekday() < 5 and d not in hol:
            out.append(d)
        d += timedelta(days=1)
    return out


def _load_holidays(d0: date, d1: date) -> set:
    from backend.services.market_holiday import refresh_holiday_dates_from_db

    holidays = refresh_holiday_dates_from_db() or set()
    if holidays:
        return holidays
    try:
        from backend.config import settings
        from backend.services.upstox_service import UpstoxService

        up = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
        out: set = set()
        for year in range(d0.year, d1.year + 1):
            for dstr in up.get_market_holidays(year) or []:
                try:
                    out.add(date.fromisoformat(str(dstr)[:10]))
                except ValueError:
                    continue
        return out
    except Exception as e:
        logger.info("momentx backtest holiday fallback empty: %s", e)
        return set()


def _parse_hhmm(s: str, default: Tuple[int, int] = (14, 45)) -> Tuple[int, int]:
    try:
        parts = str(s or "").strip().split(":")
        h = int(parts[0])
        m = int(parts[1]) if len(parts) > 1 else 0
        if 0 <= h <= 23 and 0 <= m <= 59:
            return h, m
    except Exception:
        pass
    return default


def _asof_dt(trade_date: date, hhmm: Tuple[int, int]) -> datetime:
    return IST.localize(datetime.combine(trade_date, dt_time(hhmm[0], hhmm[1], 0)))


def _slice_daily(rows: Sequence[Dict[str, Any]], trade_date: date) -> List[Dict[str, Any]]:
    out = []
    for c in rows or []:
        d = candle_date(c)
        if d is not None and d <= trade_date:
            out.append(c)
    return out


def _slice_m15(
    rows: Sequence[Dict[str, Any]],
    trade_date: date,
    asof: datetime,
) -> List[Dict[str, Any]]:
    """Bars on/before trade_date, and on trade_date only up to asof start time."""
    out = []
    for c in rows or []:
        ts = None
        try:
            from backend.services.upstox_service import _parse_ts_to_aware_ist

            ts = _parse_ts_to_aware_ist(c.get("timestamp"))
        except Exception:
            ts = None
        if ts is None:
            continue
        ts = ts.astimezone(IST)
        d = ts.date()
        if d < trade_date:
            out.append(c)
        elif d == trade_date and ts <= asof:
            out.append(c)
    return out


def _fetch_candles_range(
    instrument_key: str,
    interval: str,
    *,
    range_end: date,
    days_back: int,
) -> List[Dict[str, Any]]:
    ik = (instrument_key or "").strip()
    if not ik:
        return []
    try:
        from backend.config import settings
        from backend.services.upstox_rate_limiter import scheduled_candle_worker
        from backend.services.upstox_service import UpstoxService

        up = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
        with scheduled_candle_worker():
            raw = up.get_historical_candles_by_instrument_key(
                ik,
                interval=interval,
                days_back=int(days_back),
                range_end_date=range_end,
            )
        return sort_candles(raw or [])
    except Exception as e:
        logger.info("momentx bt candle fail %s %s: %s", ik, interval, e)
        return []


def _fetch_m15_covering(instrument_key: str, d0: date, d1: date) -> List[Dict[str, Any]]:
    """Fetch overlapping ≤28d windows of 15m bars covering [d0, d1]."""
    merged: Dict[str, Dict[str, Any]] = {}
    cursor_end = d1
    floor = d0
    guard = 0
    while cursor_end >= floor and guard < 24:
        guard += 1
        days_back = min(_M15_WINDOW_DAYS, (cursor_end - floor).days + 1)
        days_back = max(days_back, 1)
        chunk = _fetch_candles_range(
            instrument_key,
            "minutes/15",
            range_end=cursor_end,
            days_back=days_back,
        )
        for c in chunk:
            ts = str(c.get("timestamp") or "")
            if ts:
                merged[ts] = c
        # Step back with slight overlap so we don't miss bars at boundaries.
        cursor_end = cursor_end - timedelta(days=max(1, days_back - 2))
        if not chunk and cursor_end < floor:
            break
    return sort_candles(list(merged.values()))


def _batch_windows(days: Sequence[date], max_span_days: int = 18) -> List[Tuple[date, date, List[date]]]:
    """Split trading days into batches whose calendar span fits Upstox m15 windows."""
    if not days:
        return []
    batches: List[Tuple[date, date, List[date]]] = []
    buf: List[date] = []
    for d in days:
        if not buf:
            buf = [d]
            continue
        if (d - buf[0]).days + 1 > max_span_days:
            batches.append((buf[0], buf[-1], list(buf)))
            buf = [d]
        else:
            buf.append(d)
    if buf:
        batches.append((buf[0], buf[-1], list(buf)))
    return batches


def active_run_id() -> Optional[int]:
    with _RUN_LOCK:
        return _ACTIVE_RUN_ID


def run_stage1_backtest(
    *,
    date_from: date,
    date_to: date,
    asof_hhmm: str = "14:45",
    replace_range: bool = True,
    run_id: Optional[int] = None,
    cfg: Optional[MomentXConfig] = None,
    max_workers: Optional[int] = None,
) -> Dict[str, Any]:
    """Qualify Stage-1 symbols for each trading day in [date_from, date_to].

    Results land in ``momentx_backtest_qualified`` (working table). Safe to re-run
    later months/ranges — pass ``replace_range=True`` to clear overlapping days first.
    """
    global _ACTIVE_RUN_ID
    cfg = cfg or CONFIG
    if date_to < date_from:
        date_from, date_to = date_to, date_from
    hhmm = _parse_hhmm(asof_hhmm, default=tuple(CONFIG.scan_cutoff_hhmm))  # type: ignore[arg-type]
    asof_label = f"{hhmm[0]:02d}:{hhmm[1]:02d}"

    if run_id is None:
        run_id = create_run(
            date_from=date_from,
            date_to=date_to,
            asof_hhmm=asof_label,
            params={
                "replace_range": replace_range,
                "sma_period": cfg.sma_period,
                "rsi_min": cfg.rsi_min,
                "williams_min": cfg.williams_min,
                "adx_min": cfg.adx_min,
                "pct_change_min": cfg.pct_change_min,
                "pct_change_max": cfg.pct_change_max,
                "time_adjusted_volume": cfg.time_adjusted_volume,
            },
        )

    with _RUN_LOCK:
        if _ACTIVE_RUN_ID is not None and _ACTIVE_RUN_ID != run_id:
            mark_run(run_id, status="error", error="another backtest is running", finished=True)
            return {"ok": False, "run_id": run_id, "error": "another backtest is running"}
        _ACTIVE_RUN_ID = run_id

    mark_run(run_id, status="running", started=True)
    started = time.monotonic()
    try:
        holidays = _load_holidays(date_from, date_to)
        sessions = iter_trading_days(date_from, date_to, holidays)
        if not sessions:
            mark_run(
                run_id,
                status="done",
                trading_days=0,
                qualified_rows=0,
                universe_size=0,
                finished=True,
            )
            return {"ok": True, "run_id": run_id, "trading_days": 0, "qualified_rows": 0}

        if replace_range:
            deleted = delete_qualified_in_range(date_from, date_to)
            logger.info("momentx bt cleared %s prior rows in %s..%s", deleted, date_from, date_to)

        universe = load_momentx_universe()
        workers = min(max(1, int(max_workers or cfg.candle_workers)), 8)
        mark_run(run_id, status="running", trading_days=len(sessions), universe_size=len(universe))

        # --- Daily candles once per symbol (enough history for SMA200) ---
        daily_days_back = int(cfg.daily_days_back) + max(0, (date_to - date_from).days) + 30
        equity_keys = [u["equity_instrument_key"] for u in universe]
        daily_by_ik: Dict[str, List[Dict[str, Any]]] = {}
        logger.info(
            "momentx bt run=%s fetching daily for %s symbols days_back=%s end=%s",
            run_id,
            len(equity_keys),
            daily_days_back,
            date_to,
        )

        def _one_daily(ik: str) -> Tuple[str, List[Dict[str, Any]]]:
            return ik, _fetch_candles_range(
                ik, "days/1", range_end=date_to, days_back=daily_days_back
            )

        with ThreadPoolExecutor(max_workers=workers) as pool:
            futs = {pool.submit(_one_daily, ik): ik for ik in equity_keys}
            for fut in as_completed(futs):
                ik = futs[fut]
                try:
                    k, rows = fut.result()
                    daily_by_ik[k] = rows
                except Exception as e:
                    logger.info("momentx bt daily fail %s: %s", ik, e)
                    daily_by_ik[ik] = []

        total_qualified = 0
        day_summaries: List[Dict[str, Any]] = []

        for batch_start, batch_end, batch_days in _batch_windows(sessions):
            fetch_from = batch_start - timedelta(days=_M15_LOOKBACK_CAL_DAYS)
            logger.info(
                "momentx bt run=%s m15 batch %s..%s (%s sessions) fetch_from=%s",
                run_id,
                batch_start,
                batch_end,
                len(batch_days),
                fetch_from,
            )
            m15_by_ik: Dict[str, List[Dict[str, Any]]] = {}

            def _one_m15(ik: str) -> Tuple[str, List[Dict[str, Any]]]:
                return ik, _fetch_m15_covering(ik, fetch_from, batch_end)

            with ThreadPoolExecutor(max_workers=workers) as pool:
                futs = {pool.submit(_one_m15, ik): ik for ik in equity_keys}
                for fut in as_completed(futs):
                    ik = futs[fut]
                    try:
                        k, rows = fut.result()
                        m15_by_ik[k] = rows
                    except Exception as e:
                        logger.info("momentx bt m15 fail %s: %s", ik, e)
                        m15_by_ik[ik] = []

            for trade_date in batch_days:
                asof = _asof_dt(trade_date, hhmm)
                day_rows: List[Dict[str, Any]] = []
                for u in universe:
                    stock = u["stock"]
                    eq = u["equity_instrument_key"]
                    daily = _slice_daily(daily_by_ik.get(eq) or [], trade_date)
                    m15 = _slice_m15(m15_by_ik.get(eq) or [], trade_date, asof)
                    try:
                        ev = evaluate_stage1(daily=daily, minutes15=m15, now=asof, cfg=cfg)
                    except Exception as e:
                        logger.info("momentx bt eval error %s %s: %s", trade_date, stock, e)
                        continue
                    if ev.get("skip_reason") or not ev.get("qualified"):
                        continue
                    day_rows.append(
                        {
                            "trade_date": trade_date,
                            "stock": stock,
                            "equity_instrument_key": eq,
                            "future_symbol": u.get("future_symbol"),
                            "future_instrument_key": u.get("future_instrument_key"),
                            "lot_size": u.get("lot_size"),
                            "price": ev.get("price"),
                            "sma200": ev.get("sma200"),
                            "prev_close": ev.get("prev_close"),
                            "pct_change": ev.get("pct_change"),
                            "rsi": ev.get("rsi"),
                            "williams_r": ev.get("williams_r"),
                            "adx": ev.get("adx"),
                            "today_volume": ev.get("today_volume"),
                            "volume_sma5": ev.get("volume_sma5"),
                            "conditions": ev.get("conditions") or {},
                            "asof_at": asof,
                        }
                    )
                inserted = insert_qualified_batch(run_id, day_rows)
                total_qualified += inserted
                day_summaries.append(
                    {"trade_date": trade_date.isoformat(), "qualified": inserted}
                )
                logger.info(
                    "momentx bt day %s qualified=%s",
                    trade_date,
                    inserted,
                )
                mark_run(
                    run_id,
                    status="running",
                    trading_days=len(sessions),
                    qualified_rows=total_qualified,
                    universe_size=len(universe),
                )

        duration = round(time.monotonic() - started, 2)
        mark_run(
            run_id,
            status="done",
            trading_days=len(sessions),
            qualified_rows=total_qualified,
            universe_size=len(universe),
            finished=True,
        )
        logger.info(
            "momentx bt run=%s done days=%s qualified=%s duration=%ss",
            run_id,
            len(sessions),
            total_qualified,
            duration,
        )
        return {
            "ok": True,
            "run_id": run_id,
            "date_from": date_from.isoformat(),
            "date_to": date_to.isoformat(),
            "asof_hhmm": asof_label,
            "trading_days": len(sessions),
            "qualified_rows": total_qualified,
            "universe_size": len(universe),
            "duration_sec": duration,
            "days": day_summaries,
        }
    except Exception as e:
        logger.exception("momentx bt run=%s failed: %s", run_id, e)
        mark_run(run_id, status="error", error=str(e), finished=True)
        return {"ok": False, "run_id": run_id, "error": str(e)}
    finally:
        with _RUN_LOCK:
            if _ACTIVE_RUN_ID == run_id:
                _ACTIVE_RUN_ID = None


def start_backtest_async(
    *,
    date_from: date,
    date_to: date,
    asof_hhmm: str = "14:45",
    replace_range: bool = True,
) -> Dict[str, Any]:
    """Create a run row and process in a daemon thread (API-friendly)."""
    global _ACTIVE_RUN_ID
    with _RUN_LOCK:
        if _ACTIVE_RUN_ID is not None:
            cur = fetch_run(_ACTIVE_RUN_ID)
            return {
                "ok": False,
                "error": "backtest already running",
                "active_run": cur,
            }
        run_id = create_run(
            date_from=date_from,
            date_to=date_to,
            asof_hhmm=asof_hhmm,
            params={"replace_range": replace_range, "async": True},
        )
        _ACTIVE_RUN_ID = run_id

    def _worker() -> None:
        try:
            run_stage1_backtest(
                date_from=date_from,
                date_to=date_to,
                asof_hhmm=asof_hhmm,
                replace_range=replace_range,
                run_id=run_id,
            )
        finally:
            with _RUN_LOCK:
                if _ACTIVE_RUN_ID == run_id:
                    _ACTIVE_RUN_ID = None

    t = threading.Thread(target=_worker, name=f"momentx-bt-{run_id}", daemon=True)
    t.start()
    return {"ok": True, "queued": True, "run_id": run_id}

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
    coverage_report,
    covered_trade_dates,
    create_run,
    delete_qualified_in_range,
    fetch_qualified,
    fetch_run,
    insert_qualified_batch,
    mark_run,
    record_day_coverage,
    update_rvol_fields,
)
from backend.services.momentx.candles import candle_date, sort_candles
from backend.services.momentx.config import CONFIG, MomentXConfig
from backend.services.momentx.rvol import CandleCache, enrich_rvol_for_row
from backend.services.momentx.universe import load_momentx_universe

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

# Upstox minutes/15 historical span ~1 month — fetch in overlapping windows.
_M15_WINDOW_DAYS = 28
_M15_LOOKBACK_CAL_DAYS = 14  # warm-up for RSI/ADX/volume SMA ahead of batch start
# 5m RVOL baseline needs prior sessions + ATR warm-up.
_M5_RVOL_LOOKBACK_CAL_DAYS = 21

_RUN_LOCK = threading.Lock()
_ACTIVE_RUN_ID: Optional[int] = None

# Process-local historical candle cache for backtest fetches (keyed by ik/interval/window).
# Survives across runs in the same process so re-runs of the same range skip Upstox.
_HIST_CACHE: Dict[Tuple[str, str, str, int], List[Dict[str, Any]]] = {}
_HIST_CACHE_LOCK = threading.Lock()
_UPSTOX_TLS = threading.local()


def _upstox_client():
    """Reuse one UpstoxService per worker thread (avoids per-call construction)."""
    from backend.config import settings
    from backend.services.upstox_service import UpstoxService

    client = getattr(_UPSTOX_TLS, "client", None)
    if client is None:
        client = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
        _UPSTOX_TLS.client = client
    return client


def reclaim_orphaned_runs(*, reason: str = "worker lost (process restart)") -> int:
    """Mark DB rows stuck in ``running`` when no in-memory worker owns them."""
    n = 0
    with _RUN_LOCK:
        alive = _ACTIVE_RUN_ID
    try:
        for row in fetch_runs(limit=50):
            if str(row.get("status") or "") != "running":
                continue
            rid = int(row["id"])
            if alive is not None and rid == int(alive):
                continue
            mark_run(
                rid,
                status="error",
                error=reason,
                finished=True,
            )
            n += 1
            logger.info("momentx bt reclaimed orphaned run=%s (%s)", rid, reason)
    except Exception:
        logger.exception("momentx bt orphan reclaim failed")
    return n


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


def _session_eval_times(
    trade_date: date,
    asof_end: datetime,
    cfg: MomentXConfig,
) -> List[datetime]:
    """15m bar-close clocks from first Stage-1 scan through asof_end (inclusive)."""
    first = cfg.first_15m_scan_hhmm
    start = IST.localize(datetime.combine(trade_date, dt_time(int(first[0]), int(first[1]), 0)))
    end = asof_end
    if end.tzinfo is None:
        end = IST.localize(end)
    else:
        end = end.astimezone(IST)
    out: List[datetime] = []
    t = start
    while t <= end:
        out.append(t)
        t = t + timedelta(minutes=15)
    return out


def _find_first_qualify(
    *,
    daily: Sequence[Dict[str, Any]],
    m15_full: Sequence[Dict[str, Any]],
    trade_date: date,
    asof_end: datetime,
    cfg: MomentXConfig,
    m15_osc_full: Optional[Sequence[Dict[str, Any]]] = None,
) -> Tuple[Optional[datetime], Optional[Dict[str, Any]]]:
    """Earliest session clock where bullish or bearish core Stage-1 passes.

    Volume is recorded but not required for storage — the backtest page can
    filter on ``volume_ok`` via the shared volume-filter checkbox.
    Stale / frozen bars (duplicate fingerprints, volume regression, wrong
    candle clock) are skipped.

    ``m15_osc_full`` is futures 15m history for RSI/Williams/ADX; when omitted,
    oscillators use cash ``m15_full``.
    """
    from backend.services.momentx.filter import evaluate_best_side

    seen_fp = set()
    prev_vol: Optional[float] = None
    for t in _session_eval_times(trade_date, asof_end, cfg):
        m15 = _slice_m15(m15_full, trade_date, t)
        m15_osc = _slice_m15(m15_osc_full, trade_date, t) if m15_osc_full is not None else None
        try:
            side, ev = evaluate_best_side(
                daily=daily,
                minutes15=m15,
                minutes15_oscillators=m15_osc,
                now=t,
                cfg=cfg,
                require_volume=False,
                prior_fingerprints=seen_fp,
                prev_today_volume=prev_vol,
            )
        except Exception:
            continue
        fp = ev.get("fingerprint")
        if fp is not None:
            seen_fp.add(fp)
        if ev.get("today_volume") is not None:
            try:
                prev_vol = float(ev["today_volume"])
            except (TypeError, ValueError):
                pass
        if ev.get("skip_reason"):
            continue
        if side and ev.get("core_qualified"):
            ev = dict(ev)
            ev["side"] = side
            return t, ev
    return None, None


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
    """Bars on/before trade_date; on trade_date keep candle starts that have closed by asof."""
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
    cache_key = (ik, str(interval), range_end.isoformat(), int(days_back))
    with _HIST_CACHE_LOCK:
        hit = _HIST_CACHE.get(cache_key)
        if hit is not None:
            return hit
    try:
        from backend.services.upstox_rate_limiter import scheduled_candle_worker

        with scheduled_candle_worker():
            raw = _upstox_client().get_historical_candles_by_instrument_key(
                ik,
                interval=interval,
                days_back=int(days_back),
                range_end_date=range_end,
            )
        rows = sort_candles(raw or [])
        with _HIST_CACHE_LOCK:
            # Cap growth; drop oldest-ish by clearing when oversized.
            if len(_HIST_CACHE) > 8000:
                _HIST_CACHE.clear()
            _HIST_CACHE[cache_key] = rows
        return rows
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


def _fetch_m5_covering(instrument_key: str, d0: date, d1: date) -> List[Dict[str, Any]]:
    """Fetch 5m bars covering [d0, d1] (Upstox window ~1 month; single or overlapped)."""
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
            "minutes/5",
            range_end=cursor_end,
            days_back=days_back,
        )
        for c in chunk:
            ts = str(c.get("timestamp") or "")
            if ts:
                merged[ts] = c
        cursor_end = cursor_end - timedelta(days=max(1, days_back - 2))
        if not chunk and cursor_end < floor:
            break
    return sort_candles(list(merged.values()))


def _make_m5_fetcher(range_end: date, lookback_start: date):
    """Return a cache fetcher (ik, interval) -> bars for RVOL enrichment."""

    def _fetch(ik: str, interval: str) -> List[Dict[str, Any]]:
        if "5" in str(interval):
            return _fetch_m5_covering(ik, lookback_start, range_end)
        return _fetch_m15_covering(ik, lookback_start, range_end)

    return _fetch


def _attach_rvol_to_day_rows(
    day_rows: List[Dict[str, Any]],
    *,
    cache: CandleCache,
    fetch_5m,
    holidays: set,
    cfg: MomentXConfig,
) -> None:
    for row in day_rows:
        try:
            enrich_rvol_for_row(
                row,
                cache=cache,
                fetch_5m=fetch_5m,
                holidays=holidays,
                rvol_sessions=int(getattr(cfg, "rvol_sessions", 5) or 5),
            )
        except Exception as e:
            logger.info(
                "momentx bt rvol enrich fail %s %s: %s",
                row.get("trade_date"),
                row.get("stock"),
                e,
            )


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
    replace_range: bool = False,
    run_id: Optional[int] = None,
    cfg: Optional[MomentXConfig] = None,
    max_workers: Optional[int] = None,
    include_rvol: bool = False,
) -> Dict[str, Any]:
    """Qualify Stage-1 symbols for each trading day in [date_from, date_to].

    Results append into ``momentx_backtest_qualified``. Days already covered for
    the same asof clock are skipped unless ``replace_range=True``.

    RVOL/excursion columns are skipped by default (use ``Recompute RVOL`` or
    ``include_rvol=True``) — they need a separate 5m futures fetch per symbol and
    dominate wall-clock on short ranges.
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
                "include_rvol": include_rvol,
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
    from backend.services.upstox_rate_limiter import set_backtest_bulk_prefetch_mode

    set_backtest_bulk_prefetch_mode(True)
    try:
        holidays = _load_holidays(date_from - timedelta(days=45), date_to)
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
        else:
            already = set(covered_trade_dates(date_from, date_to, asof_hhmm=asof_label))
            if already:
                before = len(sessions)
                sessions = [d for d in sessions if d not in already]
                logger.info(
                    "momentx bt run=%s skip %s covered days; %s remaining",
                    run_id,
                    before - len(sessions),
                    len(sessions),
                )
            if not sessions:
                mark_run(
                    run_id,
                    status="done",
                    trading_days=0,
                    qualified_rows=0,
                    universe_size=0,
                    finished=True,
                )
                return {
                    "ok": True,
                    "run_id": run_id,
                    "cached": True,
                    "trading_days": 0,
                    "qualified_rows": 0,
                    "skipped_covered": True,
                }

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
            m15_fut_by_ik: Dict[str, List[Dict[str, Any]]] = {}
            future_keys = [
                u["future_instrument_key"]
                for u in universe
                if u.get("future_instrument_key")
            ]

            def _one_m15(ik: str) -> Tuple[str, List[Dict[str, Any]]]:
                return ik, _fetch_m15_covering(ik, fetch_from, batch_end)

            # Cash + futures 15m in one wave (rate limiter paces; less idle between phases).
            equity_set = set(equity_keys)
            future_set = set(future_keys)
            m15_targets = list(dict.fromkeys([*equity_keys, *future_keys]))
            with ThreadPoolExecutor(max_workers=workers) as pool:
                futs = {pool.submit(_one_m15, ik): ik for ik in m15_targets}
                for fut in as_completed(futs):
                    ik = futs[fut]
                    try:
                        k, rows = fut.result()
                    except Exception as e:
                        logger.info("momentx bt m15 fail %s: %s", ik, e)
                        k, rows = ik, []
                    if k in equity_set:
                        m15_by_ik[k] = rows
                    if k in future_set:
                        m15_fut_by_ik[k] = rows

            # RVOL is optional — Stage-1 qualify only needs daily + 15m.
            rvol_cache: Optional[CandleCache] = None
            fetch_5m = None
            if include_rvol:
                m5_lookback = batch_start - timedelta(days=_M5_RVOL_LOOKBACK_CAL_DAYS)
                rvol_cache = CandleCache()
                fetch_5m = _make_m5_fetcher(batch_end, m5_lookback)

                def _one_m5(ik: str) -> Tuple[str, List[Dict[str, Any]]]:
                    return ik, _fetch_m5_covering(ik, m5_lookback, batch_end)

                # Only prefetch front keys; roll contracts still load on demand.
                with ThreadPoolExecutor(max_workers=workers) as pool:
                    futs = {pool.submit(_one_m5, ik): ik for ik in future_keys}
                    for fut in as_completed(futs):
                        ik = futs[fut]
                        try:
                            k, rows = fut.result()
                            rvol_cache.put(k, "minutes/5", rows)
                        except Exception as e:
                            logger.info("momentx bt m5 fut fail %s: %s", ik, e)
                            rvol_cache.put(ik, "minutes/5", [])

            for trade_date in batch_days:
                asof = _asof_dt(trade_date, hhmm)
                day_rows: List[Dict[str, Any]] = []
                for u in universe:
                    stock = u["stock"]
                    eq = u["equity_instrument_key"]
                    fut_ik = u.get("future_instrument_key") or ""
                    daily = _slice_daily(daily_by_ik.get(eq) or [], trade_date)
                    m15_full = m15_by_ik.get(eq) or []
                    m15_osc_full = m15_fut_by_ik.get(fut_ik) or []
                    try:
                        first_at, ev = _find_first_qualify(
                            daily=daily,
                            m15_full=m15_full,
                            m15_osc_full=m15_osc_full,
                            trade_date=trade_date,
                            asof_end=asof,
                            cfg=cfg,
                        )
                    except Exception as e:
                        logger.info("momentx bt eval error %s %s: %s", trade_date, stock, e)
                        continue
                    if not first_at or not ev:
                        continue
                    day_rows.append(
                        {
                            "trade_date": trade_date,
                            "stock": stock,
                            "equity_instrument_key": eq,
                            "future_symbol": u.get("future_symbol"),
                            "future_instrument_key": u.get("future_instrument_key"),
                            "lot_size": u.get("lot_size"),
                            "side": ev.get("side") or "bullish",
                            "price": ev.get("price"),
                            "sma200": ev.get("sma200"),
                            "prev_close": ev.get("prev_close"),
                            "pct_change": ev.get("pct_change"),
                            "rsi": ev.get("rsi"),
                            "williams_r": ev.get("williams_r"),
                            "adx": ev.get("adx"),
                            # Volume metrics at the first filter clock (not end-of-day).
                            "today_volume": ev.get("today_volume"),
                            "volume_sma5": ev.get("volume_sma5"),
                            "first_filtered_at": first_at,
                            "conditions": ev.get("conditions") or {},
                            "asof_at": asof,
                        }
                    )
                if include_rvol and rvol_cache is not None and fetch_5m is not None:
                    _attach_rvol_to_day_rows(
                        day_rows,
                        cache=rvol_cache,
                        fetch_5m=fetch_5m,
                        holidays=holidays,
                        cfg=cfg,
                    )
                inserted = insert_qualified_batch(run_id, day_rows)
                total_qualified += inserted
                try:
                    record_day_coverage(
                        trade_date=trade_date,
                        asof_hhmm=asof_label,
                        run_id=run_id,
                        qualified_count=inserted,
                    )
                except Exception as e:
                    logger.info("momentx bt coverage write failed %s: %s", trade_date, e)
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
        set_backtest_bulk_prefetch_mode(False)
        with _RUN_LOCK:
            if _ACTIVE_RUN_ID == run_id:
                _ACTIVE_RUN_ID = None


def start_backtest_async(
    *,
    date_from: date,
    date_to: date,
    asof_hhmm: str = "14:45",
    replace_range: bool = False,
) -> Dict[str, Any]:
    """Create a run row and process in a daemon thread (API-friendly).

    If the requested range is already fully covered (append mode), returns
    ``cached=True`` without starting a live scan.
    """
    global _ACTIVE_RUN_ID
    reclaim_orphaned_runs()
    hhmm = _parse_hhmm(asof_hhmm, default=tuple(CONFIG.scan_cutoff_hhmm))  # type: ignore[arg-type]
    asof_label = f"{hhmm[0]:02d}:{hhmm[1]:02d}"
    cov = coverage_report(date_from, date_to, asof_hhmm=asof_label)
    if not replace_range and cov.get("complete"):
        return {
            "ok": True,
            "cached": True,
            "queued": False,
            "coverage": cov,
            "message": "Range already in storage — displaying saved results",
        }
    with _RUN_LOCK:
        if _ACTIVE_RUN_ID is not None:
            cur = fetch_run(_ACTIVE_RUN_ID)
            return {
                "ok": False,
                "error": "backtest already running",
                "active_run": cur,
                "coverage": cov,
            }
        run_id = create_run(
            date_from=date_from,
            date_to=date_to,
            asof_hhmm=asof_label,
            params={
                "replace_range": replace_range,
                "async": True,
                "include_rvol": False,
                "missing_days": cov.get("missing_days"),
            },
        )
        _ACTIVE_RUN_ID = run_id

    def _worker() -> None:
        try:
            run_stage1_backtest(
                date_from=date_from,
                date_to=date_to,
                asof_hhmm=asof_label,
                replace_range=replace_range,
                run_id=run_id,
                include_rvol=False,
            )
        finally:
            with _RUN_LOCK:
                if _ACTIVE_RUN_ID == run_id:
                    _ACTIVE_RUN_ID = None

    t = threading.Thread(target=_worker, name=f"momentx-bt-{run_id}", daemon=True)
    t.start()
    return {
        "ok": True,
        "queued": True,
        "cached": False,
        "run_id": run_id,
        "coverage": cov,
        "include_rvol": False,
        "message": "Stage-1 running; use Recompute RVOL after for volume metrics",
    }


def recompute_rvol_for_range(
    *,
    date_from: date,
    date_to: date,
    cfg: Optional[MomentXConfig] = None,
) -> Dict[str, Any]:
    """Fill RVOL columns on stored qualified rows without re-running Stage-1 gates."""
    cfg = cfg or CONFIG
    if date_to < date_from:
        date_from, date_to = date_to, date_from
    holidays = _load_holidays(date_from - timedelta(days=30), date_to)
    payload = fetch_qualified(date_from=date_from, date_to=date_to)
    rows = list(payload.get("rows") or [])
    if not rows:
        return {"ok": True, "updated": 0, "total": 0}

    lookback = date_from - timedelta(days=_M5_RVOL_LOOKBACK_CAL_DAYS)
    cache = CandleCache()
    fetch_5m = _make_m5_fetcher(date_to, lookback)

    # Prefetch distinct front keys.
    front_keys = sorted(
        {
            str(r.get("future_instrument_key") or "").strip()
            for r in rows
            if r.get("future_instrument_key")
        }
    )
    workers = min(max(1, int(cfg.candle_workers)), 8)

    def _one(ik: str) -> Tuple[str, List[Dict[str, Any]]]:
        return ik, _fetch_m5_covering(ik, lookback, date_to)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = {pool.submit(_one, ik): ik for ik in front_keys}
        for fut in as_completed(futs):
            ik = futs[fut]
            try:
                k, bars = fut.result()
                cache.put(k, "minutes/5", bars)
            except Exception as e:
                logger.info("momentx rvol recompute m5 fail %s: %s", ik, e)
                cache.put(ik, "minutes/5", [])

    enriched: List[Dict[str, Any]] = []
    for r in rows:
        # parse trade_date back to date for enrich
        td = r.get("trade_date")
        if isinstance(td, str):
            try:
                r = dict(r)
                r["trade_date"] = date.fromisoformat(td[:10])
            except ValueError:
                pass
        enrich_rvol_for_row(
            r,
            cache=cache,
            fetch_5m=fetch_5m,
            holidays=holidays,
            rvol_sessions=int(getattr(cfg, "rvol_sessions", 5) or 5),
        )
        enriched.append(r)

    updated = update_rvol_fields(enriched)
    return {"ok": True, "updated": updated, "total": len(rows)}


_CSV_COLUMNS = [
    "trade_date",
    "stock",
    "side",
    "future_symbol",
    "first_filtered_at",
    "today_volume",
    "volume_sma5",
    "price",
    "pct_change",
    "rsi",
    "williams_r",
    "adx",
    "sma200",
    "lot_size",
    "volume_ok",
    "rvol_cum",
    "rvol_slot",
    "cum_vol_at_T",
    "baseline_cum_vol",
    "n_sessions_used",
    "atr5_at_T",
    "px_at_T",
    "ret_30m",
    "ret_60m",
    "ret_120m",
    "eod_ret",
    "mfe_atr",
    "mae_atr",
]


def results_to_csv_text(
    *,
    date_from: date,
    date_to: date,
    volume_filter: bool = False,
) -> str:
    """Build CSV text for stored backtest rows (volume filter optional)."""
    import csv
    import io

    payload = fetch_qualified(date_from=date_from, date_to=date_to)
    rows = list(payload.get("rows") or [])
    if volume_filter:
        filtered = []
        for r in rows:
            vok = r.get("volume_ok")
            if vok is None:
                conds = r.get("conditions") or {}
                vok = conds.get("volume_ok") if isinstance(conds, dict) else None
            if vok is None or bool(vok):
                filtered.append(r)
        rows = filtered

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=_CSV_COLUMNS, extrasaction="ignore")
    writer.writeheader()
    for r in rows:
        out = {k: r.get(k) for k in _CSV_COLUMNS}
        writer.writerow(out)
    return buf.getvalue()

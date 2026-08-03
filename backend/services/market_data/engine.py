"""
Centralized market data refresh for arbitrage_master.

Single entry point for LTP + 5m VWAP/EMA(5) persistence. Algos read via ``reads`` module.

REST candle warm is **curr-month futures only** (~200 keys) on the 10m job. Stock and
next-month LTP refresh via WebSocket every 30 minutes; stock/next VWAP+EMA5 refresh via
a separate hourly REST candle job (scheduled away from the 10m marks).

Candle fetches for the 10m and hourly jobs share one process-wide Upstox candle
rate-limit bucket and are mutually excluded so they never race the same budget.
"""
from __future__ import annotations

import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

import pytz

from backend.config import settings
from backend.services.market_data.constants import (
    BATCH_QUOTE_CHUNK,
    CANDLE_DAYS_BACK,
    CANDLE_FETCH_WORKERS,
    CANDLE_INTERVAL,
    DATA_SOURCE_REST,
    DATA_SOURCE_WS,
    REFRESH_FAILED,
    REFRESH_OK,
    REFRESH_PARTIAL,
)
from backend.services.market_data.indicators import indicators_from_5m_candles
from backend.services.market_data.repository import bulk_update_market_data, load_universe_rows
from backend.services.market_data.schema import ensure_market_data_columns

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

# Legs that may receive REST historical/intraday candle fetches.
DEFAULT_CANDLE_LEGS: Tuple[str, ...] = ("currmth",)
ALL_LEGS: Tuple[str, ...] = ("stock", "currmth", "nextmth")

# Serialize candle-warm ThreadPools across 10m currmth and hourly stock+nextmth.
_CANDLE_WARM_LOCK = threading.Lock()
_ACTIVE_WARM_LOCK = threading.Lock()
_ACTIVE_WARM_EXECUTIONS: Set[str] = set()

# Hourly stock+next (~400 keys @ 5/s ≈ 80s+) needs a longer acquire wait than the
# default 90s used by the smaller 10m currmth warm.
_HOURLY_CANDLE_RL_MAX_WAIT = 300.0


def _now_ist() -> datetime:
    return datetime.now(IST)


def _rotate_universe_rows(
    rows: List[Dict[str, Any]], *, execution: str
) -> Tuple[List[Dict[str, Any]], int]:
    """Rotate starting index each cycle so rate-limit denials don't always hit the same tail.

    Universe SQL is ``ORDER BY stock`` (alphabetical). Without rotation, when the
    shared candle budget exhausts mid-batch, the same back-half symbols starve.
    """
    n = len(rows)
    if n <= 1:
        return list(rows), 0
    now = _now_ist()
    # Minute bucket + execution salt → different offset per job/cycle.
    bucket = int(now.timestamp()) // 60
    salt = sum(ord(c) for c in (execution or "")) & 0xFFFF
    offset = (bucket + salt) % n
    if offset == 0:
        return list(rows), 0
    return list(rows[offset:]) + list(rows[:offset]), offset


def _f(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        x = float(v)
        return x if x > 0 else None
    except (TypeError, ValueError):
        return None


def _leg_instrument_key(row: Dict[str, Any], leg: str) -> str:
    if leg == "stock":
        return str(row.get("stock_instrument_key") or "").strip()
    if leg == "currmth":
        return str(row.get("currmth_future_instrument_key") or "").strip()
    if leg == "nextmth":
        return str(row.get("nextmth_future_instrement_key") or "").strip()
    return ""


def _collect_instrument_keys_for_legs(
    rows: List[Dict[str, Any]], legs: Sequence[str]
) -> List[str]:
    seen: Set[str] = set()
    keys: List[str] = []
    for row in rows:
        for leg in legs:
            ks = _leg_instrument_key(row, leg)
            if ks and ks not in seen:
                seen.add(ks)
                keys.append(ks)
    return keys


def _collect_all_instrument_keys(rows: List[Dict[str, Any]]) -> List[str]:
    return _collect_instrument_keys_for_legs(rows, ALL_LEGS)


def _batch_ltp_map(upstox: Any, keys: List[str]) -> Dict[str, float]:
    out: Dict[str, float] = {}
    if not keys or not getattr(upstox, "access_token", None):
        return out
    for i in range(0, len(keys), BATCH_QUOTE_CHUNK):
        chunk = keys[i : i + BATCH_QUOTE_CHUNK]
        try:
            part = upstox.get_market_quotes_batch_by_keys(chunk) or {}
            for k, v in part.items():
                fv = _f(v)
                if fv is not None:
                    out[k] = fv
        except Exception as e:
            logger.warning("market_data batch LTP chunk failed: %s", e)
    return out


def _ws_ltp_map(keys: List[str]) -> Dict[str, float]:
    """Read LTP from live WebSocket cache (no REST)."""
    out: Dict[str, float] = {}
    if not keys:
        return out
    try:
        from backend.services.upstox_market_feed import get_ws_quote_for_instrument

        for ik in keys:
            wq = get_ws_quote_for_instrument(ik)
            if not wq:
                continue
            lp = _f(wq.get("ltp") or wq.get("last_price"))
            if lp is not None:
                out[ik] = lp
    except Exception as e:
        logger.debug("market_data ws ltp map skipped: %s", e)
    return out


def _ws_ltp_overlay(keys: List[str], ltp_map: Dict[str, float]) -> int:
    """Overlay fresher websocket LTP when feed is running."""
    n = 0
    ws = _ws_ltp_map(keys)
    for ik, lp in ws.items():
        ltp_map[ik] = lp
        n += 1
    return n


def _ensure_ws_universe(rows: List[Dict[str, Any]]) -> None:
    """Keep WS subscribed to stock + curr + next so 30m LTP snapshots stay available."""
    try:
        from backend.services.upstox_market_feed import ensure_market_feed_running

        keys = _collect_all_instrument_keys(rows)
        # Cap matches prior behavior; full FO universe is typically ~600 keys.
        ensure_market_feed_running(keys[:2000])
    except Exception:
        pass


def _fetch_5m_indicators(upstox: Any, instrument_key: str) -> Optional[Dict[str, float]]:
    if not instrument_key or not getattr(upstox, "access_token", None):
        return None
    try:
        # Candles are shared automatically via the transparent cache inside
        # get_historical_candles_by_instrument_key, so in-process consumers
        # (e.g. the Relative Strength Scanner) reuse them with no extra fetch.
        candles = upstox.get_historical_candles_by_instrument_key(
            instrument_key,
            interval=CANDLE_INTERVAL,
            days_back=CANDLE_DAYS_BACK,
        )
        return indicators_from_5m_candles(candles or [])
    except Exception as e:
        logger.debug("market_data candles %s: %s", instrument_key, e)
        return None


def _leg_update(
    *,
    ltp_map: Dict[str, float],
    ik: Optional[str],
    ind: Optional[Dict[str, float]],
    now: datetime,
) -> Dict[str, Any]:
    patch: Dict[str, Any] = {"updated": now}
    if ik:
        lp = ltp_map.get(ik)
        if lp is not None:
            patch["ltp"] = lp
    if ind:
        patch["vwap"] = ind.get("vwap")
        patch["ema5"] = ind.get("ema5")
        if ind.get("candle_close") is not None:
            patch["candle"] = {
                "open": ind.get("candle_open"),
                "high": ind.get("candle_high"),
                "low": ind.get("candle_low"),
                "close": ind.get("candle_close"),
                "volume": ind.get("candle_volume"),
            }
    return patch


def refresh_stock_next_ltp_from_ws(
    *,
    execution: str = "scheduled_ws_ltp_30m",
    stocks: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    """
    Persist stock + next-month LTP (prefer WebSocket; REST quote fallback).

    Intended cadence: every 30 minutes. Does **not** update VWAP/EMA5 — those
    are owned by ``refresh_stock_next_vwap_ema_hourly``. Arb live selection still
    uses on-demand REST quotes when the UI needs fresher LTPs.
    """
    ensure_market_data_columns()
    started = _now_ist()
    rows = load_universe_rows()
    if stocks:
        want = {str(s or "").strip().upper() for s in stocks if str(s or "").strip()}
        rows = [r for r in rows if str(r.get("stock") or "").strip().upper() in want]
    if not rows:
        return {
            "success": True,
            "rows": 0,
            "execution": execution,
            "message": "empty_universe",
        }

    _ensure_ws_universe(rows)
    keys = _collect_instrument_keys_for_legs(rows, ("stock", "nextmth"))
    ltp_map = _ws_ltp_map(keys)

    # Soft fallback: REST quotes only for keys missing from WS (does not use candle budget).
    missing = [k for k in keys if k not in ltp_map]
    rest_n = 0
    if missing:
        try:
            from backend.services.upstox_service import UpstoxService

            upstox = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
            if getattr(upstox, "access_token", None):
                fb = _batch_ltp_map(upstox, missing)
                for k, v in fb.items():
                    ltp_map[k] = v
                    rest_n += 1
        except Exception as e:
            logger.warning("stock/next WS LTP REST fallback failed: %s", e)

    now = _now_ist()
    updates: List[Dict[str, Any]] = []
    ok = 0
    for row in rows:
        stock = row.get("stock")
        sk = _leg_instrument_key(row, "stock")
        nk = _leg_instrument_key(row, "nextmth")
        upd: Dict[str, Any] = {
            "stock": stock,
            "market_data_source": DATA_SOURCE_WS if rest_n == 0 else DATA_SOURCE_REST,
            "market_data_last_updated": now,
        }
        hit = False
        if sk and sk in ltp_map:
            upd["stock_ltp"] = ltp_map[sk]
            upd["stock_last_updated"] = now
            hit = True
        if nk and nk in ltp_map:
            upd["nextmth_future_ltp"] = ltp_map[nk]
            upd["nextmth_future_last_updated"] = now
            hit = True
        if hit:
            ok += 1
            updates.append(upd)

    written = bulk_update_market_data(updates) if updates else 0
    summary = {
        "success": True,
        "execution": execution,
        "rows_total": len(rows),
        "rows_updated": written,
        "ltp_keys": len(ltp_map),
        "ws_ltp_hits": len(ltp_map) - rest_n,
        "rest_quote_fallback": rest_n,
        "status_ok": ok,
        "elapsed_sec": round((_now_ist() - started).total_seconds(), 2),
        "updated_at_ist": _now_ist().strftime("%Y-%m-%d %H:%M:%S"),
    }
    logger.info("market_data stock/next WS LTP: %s", summary)
    return summary


def refresh_stock_next_vwap_ema_hourly(
    *,
    execution: str = "scheduled_stock_next_vwap_ema_hourly",
    stocks: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    """
    REST 5m candles for stock + next-month → ``stock_vwap`` / ``stock_ema5`` /
    ``nextmth_future_vwap`` / ``nextmth_future_ema5``.

    Does **not** write LTP columns (owned by the 30m WS job). Cadence: hourly,
    scheduled between 10m marks (see smart_future_algo cron).
    """
    return refresh_arbitrage_master_market_data(
        execution=execution,
        fetch_candles=True,
        stocks=stocks,
        candle_legs=("stock", "nextmth"),
        ltp_legs=(),
    )


def refresh_curr_month_aux_candles(
    *,
    execution: str = "scheduled_aux_0905",
    stocks: Optional[Sequence[str]] = None,
    intervals: Optional[Sequence[Tuple[str, int]]] = None,
) -> Dict[str, Any]:
    """
    REST warm of curr-month intervals into shared candle_cache.

    Default: ``days/1`` (45d) for VM Bollinger / prev-close. Opening 10m range
    comes from the curr-month 5m warm (aggregated 09:15+09:20), not minutes/15.
    """
    ensure_market_data_columns()
    started = _now_ist()
    rows = load_universe_rows()
    if stocks:
        want = {str(s or "").strip().upper() for s in stocks if str(s or "").strip()}
        rows = [r for r in rows if str(r.get("stock") or "").strip().upper() in want]
    if not rows:
        return {
            "success": True,
            "rows": 0,
            "execution": execution,
            "message": "empty_universe",
        }

    try:
        from backend.services.upstox_service import UpstoxService

        upstox = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    except Exception as e:
        return {"success": False, "execution": execution, "error": str(e)}

    if not getattr(upstox, "access_token", None):
        return {"success": False, "execution": execution, "error": "upstox_not_connected"}

    keys = _collect_instrument_keys_for_legs(rows, ("currmth",))
    specs: List[Tuple[str, int]] = list(intervals) if intervals else [
        ("days/1", 45),
    ]
    ok_by: Dict[str, int] = {iv: 0 for iv, _ in specs}
    err = 0

    def _one(ik: str, interval: str, days_back: int) -> Tuple[str, bool]:
        try:
            bars = upstox.get_historical_candles_by_instrument_key(
                ik, interval=interval, days_back=days_back
            )
            return interval, bool(bars)
        except Exception:
            return interval, False

    with _ACTIVE_WARM_LOCK:
        overlapping_with = sorted(_ACTIVE_WARM_EXECUTIONS)
        overlap_detected = bool(overlapping_with)
        _ACTIVE_WARM_EXECUTIONS.add(execution)
    lock_t0 = _now_ist()
    _CANDLE_WARM_LOCK.acquire()
    lock_wait_sec = round((_now_ist() - lock_t0).total_seconds(), 2)
    try:
        with ThreadPoolExecutor(max_workers=CANDLE_FETCH_WORKERS) as pool:
            futs = []
            for ik in keys:
                for interval, days_back in specs:
                    futs.append(pool.submit(_one, ik, interval, days_back))
            for fut in as_completed(futs):
                interval, ok = fut.result()
                if ok:
                    ok_by[interval] = ok_by.get(interval, 0) + 1
                else:
                    err += 1
    finally:
        _CANDLE_WARM_LOCK.release()
        with _ACTIVE_WARM_LOCK:
            _ACTIVE_WARM_EXECUTIONS.discard(execution)

    summary = {
        "success": True,
        "execution": execution,
        "keys": len(keys),
        "ok_by_interval": ok_by,
        "errors": err,
        "concurrent_job_overlap_detected": overlap_detected or lock_wait_sec > 0.05,
        "overlapping_with": overlapping_with,
        "candle_warm_lock_wait_sec": lock_wait_sec,
        "elapsed_sec": round((_now_ist() - started).total_seconds(), 2),
        "updated_at_ist": _now_ist().strftime("%Y-%m-%d %H:%M:%S"),
    }
    logger.info("market_data curr-month aux candles: %s", summary)
    return summary


def refresh_arbitrage_master_market_data(
    *,
    execution: str = "scheduled",
    fetch_candles: bool = True,
    stocks: Optional[Sequence[str]] = None,
    candle_legs: Sequence[str] = DEFAULT_CANDLE_LEGS,
    ltp_legs: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    """
    Refresh LTP (+ optional 5m VWAP/EMA) for arbitrage_master rows.

    When ``stocks`` is set, only those underlyings are refreshed (case-insensitive).
    REST candles are limited to ``candle_legs`` (default: curr-month futures only).
    ``ltp_legs`` defaults to all three legs; pass ``("currmth",)`` on the 10m warm
    job so stock/next LTP are left to the 30m WS path.
    Safe to call from schedulers; returns summary dict for monitoring.
    """
    ensure_market_data_columns()
    started = _now_ist()
    started_at_ist = started.strftime("%Y-%m-%d %H:%M:%S")
    rows = load_universe_rows()
    if stocks:
        want = {str(s or "").strip().upper() for s in stocks if str(s or "").strip()}
        rows = [r for r in rows if str(r.get("stock") or "").strip().upper() in want]
    rotate_offset = 0
    if rows and fetch_candles:
        rows, rotate_offset = _rotate_universe_rows(rows, execution=execution)
    if not rows:
        return {
            "success": True,
            "rows": 0,
            "execution": execution,
            "message": "empty_universe",
            "started_at_ist": started_at_ist,
        }

    try:
        from backend.services.upstox_service import UpstoxService

        upstox = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    except Exception as e:
        logger.error("market_data: Upstox init failed: %s", e)
        return {
            "success": False,
            "execution": execution,
            "error": str(e),
            "started_at_ist": started_at_ist,
        }

    if not getattr(upstox, "access_token", None):
        return {
            "success": False,
            "execution": execution,
            "error": "upstox_not_connected",
            "started_at_ist": started_at_ist,
        }

    legs_for_ltp: Sequence[str] = tuple(ltp_legs) if ltp_legs is not None else ALL_LEGS
    legs_for_candles: Sequence[str] = tuple(candle_legs) if fetch_candles else ()

    # Always keep full universe on WS even when this cycle only quotes currmth.
    _ensure_ws_universe(rows)

    quote_keys = _collect_instrument_keys_for_legs(rows, legs_for_ltp)
    ltp_map = _batch_ltp_map(upstox, quote_keys)
    ws_n = _ws_ltp_overlay(quote_keys, ltp_map)

    candle_keys: List[Tuple[str, str, str]] = []
    for row in rows:
        stock = row.get("stock")
        for leg in legs_for_candles:
            ks = _leg_instrument_key(row, leg)
            if ks:
                candle_keys.append((str(stock), leg, ks))

    indicators_by_key: Dict[str, Dict[str, float]] = {}
    candle_errors = 0
    candle_failed_iks: List[str] = []
    overlap_detected = False
    overlapping_with: List[str] = []
    lock_wait_sec = 0.0
    if fetch_candles and candle_keys:
        # Preserve submission order (rotated alphabetical) — dict keeps insertion order.
        unique_iks = list(dict.fromkeys(ik for _, _, ik in candle_keys))
        is_hourly_stock_next = set(legs_for_candles) == {"stock", "nextmth"} or (
            "stock" in legs_for_candles and "nextmth" in legs_for_candles and "currmth" not in legs_for_candles
        )

        with _ACTIVE_WARM_LOCK:
            overlapping_with = sorted(_ACTIVE_WARM_EXECUTIONS)
            overlap_detected = bool(overlapping_with)
            _ACTIVE_WARM_EXECUTIONS.add(execution)
        lock_wait_t0 = _now_ist()
        _CANDLE_WARM_LOCK.acquire()
        lock_wait_sec = round((_now_ist() - lock_wait_t0).total_seconds(), 2)
        if lock_wait_sec > 0.05:
            overlap_detected = True
        try:
            if is_hourly_stock_next:
                try:
                    from backend.services.upstox_rate_limiter import (
                        set_candle_rl_max_wait_override,
                    )

                    set_candle_rl_max_wait_override(_HOURLY_CANDLE_RL_MAX_WAIT)
                except Exception:
                    pass
            try:
                with ThreadPoolExecutor(max_workers=CANDLE_FETCH_WORKERS) as pool:
                    futs = {
                        pool.submit(_fetch_5m_indicators, upstox, ik): ik for ik in unique_iks
                    }
                    for fut in as_completed(futs):
                        ik = futs[fut]
                        try:
                            ind = fut.result()
                            if ind:
                                indicators_by_key[ik] = ind
                            else:
                                candle_errors += 1
                                candle_failed_iks.append(ik)
                        except Exception:
                            candle_errors += 1
                            candle_failed_iks.append(ik)
            finally:
                if is_hourly_stock_next:
                    try:
                        from backend.services.upstox_rate_limiter import (
                            set_candle_rl_max_wait_override,
                        )

                        set_candle_rl_max_wait_override(None)
                    except Exception:
                        pass
        finally:
            _CANDLE_WARM_LOCK.release()
            with _ACTIVE_WARM_LOCK:
                _ACTIVE_WARM_EXECUTIONS.discard(execution)

    now = _now_ist()
    updates: List[Dict[str, Any]] = []
    ok_rows = 0
    partial_rows = 0
    failed_rows = 0
    want_stock_ltp = "stock" in legs_for_ltp
    want_curr_ltp = "currmth" in legs_for_ltp
    want_next_ltp = "nextmth" in legs_for_ltp
    want_stock_ind = "stock" in legs_for_candles
    want_curr_ind = "currmth" in legs_for_candles
    want_next_ind = "nextmth" in legs_for_candles

    for row in rows:
        stock = row.get("stock")
        sk = _leg_instrument_key(row, "stock")
        ck = _leg_instrument_key(row, "currmth")
        nk = _leg_instrument_key(row, "nextmth")

        stock_patch = _leg_update(
            ltp_map=ltp_map if want_stock_ltp else {},
            ik=sk or None if want_stock_ltp else None,
            ind=indicators_by_key.get(sk) if want_stock_ind else None,
            now=now,
        )
        curr_patch = _leg_update(
            ltp_map=ltp_map if want_curr_ltp else {},
            ik=ck or None if want_curr_ltp else None,
            ind=indicators_by_key.get(ck) if want_curr_ind else None,
            now=now,
        )
        next_patch = _leg_update(
            ltp_map=ltp_map if want_next_ltp else {},
            ik=nk or None if want_next_ltp else None,
            ind=indicators_by_key.get(nk) if want_next_ind else None,
            now=now,
        )

        has_ltp = any(
            p.get("ltp") is not None for p in (stock_patch, curr_patch, next_patch) if p
        )
        if has_ltp:
            ok_rows += 1
        elif sk or ck or nk:
            partial_rows += 1
        else:
            failed_rows += 1

        status = REFRESH_OK if has_ltp else (REFRESH_PARTIAL if (sk or ck) else REFRESH_FAILED)
        source = DATA_SOURCE_WS if ws_n else DATA_SOURCE_REST

        upd: Dict[str, Any] = {
            "stock": stock,
            "market_data_source": source,
            "market_data_refresh_status": status,
            "market_data_refresh_error": None,
            "market_data_last_updated": now,
        }
        if stock_patch.get("ltp") is not None:
            upd["stock_ltp"] = stock_patch["ltp"]
            upd["stock_last_updated"] = now
        if stock_patch.get("vwap") is not None:
            upd["stock_vwap"] = stock_patch["vwap"]
        if stock_patch.get("ema5") is not None:
            upd["stock_ema5"] = stock_patch["ema5"]

        if curr_patch.get("ltp") is not None:
            upd["currmth_future_ltp"] = curr_patch["ltp"]
            upd["currmth_future_last_updated"] = now
        if curr_patch.get("vwap") is not None:
            upd["currmth_future_vwap"] = curr_patch["vwap"]
        if curr_patch.get("ema5") is not None:
            upd["currmth_future_ema5"] = curr_patch["ema5"]
        cnd = curr_patch.get("candle")
        if isinstance(cnd, dict):
            upd["currmth_candle_open_5m"] = cnd.get("open")
            upd["currmth_candle_high_5m"] = cnd.get("high")
            upd["currmth_candle_low_5m"] = cnd.get("low")
            upd["currmth_candle_close_5m"] = cnd.get("close")
            upd["currmth_candle_volume_5m"] = cnd.get("volume")

        if next_patch.get("ltp") is not None:
            upd["nextmth_future_ltp"] = next_patch["ltp"]
            upd["nextmth_future_last_updated"] = now
        if next_patch.get("vwap") is not None:
            upd["nextmth_future_vwap"] = next_patch["vwap"]
        if next_patch.get("ema5") is not None:
            upd["nextmth_future_ema5"] = next_patch["ema5"]

        updates.append(upd)

    written = bulk_update_market_data(updates)
    elapsed = (_now_ist() - started).total_seconds()

    candle_keys_requested = len({ik for _, _, ik in candle_keys}) if fetch_candles else 0
    failed_ik_set = set(candle_failed_iks)
    # Map failed instrument keys → stocks (and which legs missed).
    denied_by_stock: Dict[str, List[str]] = {}
    for stock, leg, ik in candle_keys:
        if ik in failed_ik_set:
            denied_by_stock.setdefault(str(stock), [])
            if leg not in denied_by_stock[str(stock)]:
                denied_by_stock[str(stock)].append(leg)
    denied_symbols = sorted(denied_by_stock.keys())
    deny_pct = (
        round(100.0 * candle_errors / candle_keys_requested, 1)
        if candle_keys_requested
        else 0.0
    )

    rl_stats: Dict[str, Any] = {}
    try:
        from backend.services.upstox_rate_limiter import stats as candle_rl_stats

        rl_stats = candle_rl_stats() or {}
    except Exception:
        rl_stats = {}

    summary = {
        "success": True,
        "execution": execution,
        "started_at_ist": started_at_ist,
        "rows_total": len(rows),
        "rows_updated": written,
        "ltp_keys": len(ltp_map),
        "ltp_legs": list(legs_for_ltp),
        "candle_legs": list(legs_for_candles),
        "ws_ltp_overlays": ws_n,
        "candle_keys_requested": candle_keys_requested,
        "candle_indicators_ok": len(indicators_by_key),
        "candle_errors": candle_errors,
        "candle_deny_pct": deny_pct,
        "candle_denied_symbols": denied_symbols,
        "candle_denied_by_stock": denied_by_stock,
        "candle_rl": rl_stats,
        "symbol_rotate_offset": rotate_offset,
        "concurrent_job_overlap_detected": overlap_detected,
        "overlapping_with": overlapping_with,
        "candle_warm_lock_wait_sec": lock_wait_sec,
        "status_ok": ok_rows,
        "status_partial": partial_rows,
        "status_failed": failed_rows,
        "elapsed_sec": round(elapsed, 2),
        "updated_at_ist": _now_ist().strftime("%Y-%m-%d %H:%M:%S"),
    }
    logger.info("market_data refresh: %s", summary)
    try:
        from backend.services.market_data.warm_cycle_log import record_warm_cycle

        record_warm_cycle(summary)
    except Exception as log_exc:
        logger.debug("warm_cycle_log record skipped: %s", log_exc)
    return summary

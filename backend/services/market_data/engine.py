"""
Centralized market data refresh for arbitrage_master.

Single entry point for LTP + 5m VWAP/EMA(5) persistence. Algos read via ``reads`` module.
"""
from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

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


def _now_ist() -> datetime:
    return datetime.now(IST)


def _f(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        x = float(v)
        return x if x > 0 else None
    except (TypeError, ValueError):
        return None


def _collect_all_instrument_keys(rows: List[Dict[str, Any]]) -> List[str]:
    seen: Set[str] = set()
    keys: List[str] = []
    for row in rows:
        for k in (
            row.get("stock_instrument_key"),
            row.get("currmth_future_instrument_key"),
            row.get("nextmth_future_instrement_key"),
        ):
            ks = str(k or "").strip()
            if ks and ks not in seen:
                seen.add(ks)
                keys.append(ks)
    return keys


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


def _ws_ltp_overlay(keys: List[str], ltp_map: Dict[str, float]) -> int:
    """Overlay fresher websocket LTP when feed is running."""
    n = 0
    try:
        from backend.services.upstox_market_feed import get_ws_quote_for_instrument

        for ik in keys[:250]:
            wq = get_ws_quote_for_instrument(ik)
            if not wq:
                continue
            lp = _f(wq.get("ltp") or wq.get("last_price"))
            if lp is not None:
                ltp_map[ik] = lp
                n += 1
    except Exception as e:
        logger.debug("market_data ws overlay skipped: %s", e)
    return n


def _fetch_5m_indicators(upstox: Any, instrument_key: str) -> Optional[Dict[str, float]]:
    if not instrument_key or not getattr(upstox, "access_token", None):
        return None
    try:
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


def refresh_arbitrage_master_market_data(
    *,
    execution: str = "scheduled",
    fetch_candles: bool = True,
) -> Dict[str, Any]:
    """
    Refresh LTP (+ optional 5m VWAP/EMA) for all arbitrage_master rows.

    Safe to call from schedulers; returns summary dict for monitoring.
    """
    ensure_market_data_columns()
    started = _now_ist()
    rows = load_universe_rows()
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
        logger.error("market_data: Upstox init failed: %s", e)
        return {"success": False, "execution": execution, "error": str(e)}

    if not getattr(upstox, "access_token", None):
        return {
            "success": False,
            "execution": execution,
            "error": "upstox_not_connected",
        }

    all_keys = _collect_all_instrument_keys(rows)
    ltp_map = _batch_ltp_map(upstox, all_keys)
    ws_n = _ws_ltp_overlay(all_keys, ltp_map)

    # Optional: start WS feed for universe (non-blocking)
    try:
        from backend.services.upstox_market_feed import ensure_market_feed_running

        ensure_market_feed_running(all_keys[:500])
    except Exception:
        pass

    candle_keys: List[Tuple[str, str, str]] = []
    for row in rows:
        stock = row.get("stock")
        for leg, ik in (
            ("stock", row.get("stock_instrument_key")),
            ("currmth", row.get("currmth_future_instrument_key")),
            ("nextmth", row.get("nextmth_future_instrement_key")),
        ):
            ks = str(ik or "").strip()
            if ks and fetch_candles:
                candle_keys.append((str(stock), leg, ks))

    indicators_by_key: Dict[str, Dict[str, float]] = {}
    candle_errors = 0
    if fetch_candles and candle_keys:
        unique_iks = list({ik for _, _, ik in candle_keys})
        with ThreadPoolExecutor(max_workers=CANDLE_FETCH_WORKERS) as pool:
            futs = {pool.submit(_fetch_5m_indicators, upstox, ik): ik for ik in unique_iks}
            for fut in as_completed(futs):
                ik = futs[fut]
                try:
                    ind = fut.result()
                    if ind:
                        indicators_by_key[ik] = ind
                    else:
                        candle_errors += 1
                except Exception:
                    candle_errors += 1

    now = _now_ist()
    updates: List[Dict[str, Any]] = []
    ok_rows = 0
    partial_rows = 0
    failed_rows = 0

    for row in rows:
        stock = row.get("stock")
        sk = str(row.get("stock_instrument_key") or "").strip()
        ck = str(row.get("currmth_future_instrument_key") or "").strip()
        nk = str(row.get("nextmth_future_instrement_key") or "").strip()

        stock_patch = _leg_update(
            ltp_map=ltp_map,
            ik=sk or None,
            ind=indicators_by_key.get(sk),
            now=now,
        )
        curr_patch = _leg_update(
            ltp_map=ltp_map,
            ik=ck or None,
            ind=indicators_by_key.get(ck),
            now=now,
        )
        next_patch = _leg_update(
            ltp_map=ltp_map,
            ik=nk or None,
            ind=indicators_by_key.get(nk),
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

    summary = {
        "success": True,
        "execution": execution,
        "rows_total": len(rows),
        "rows_updated": written,
        "ltp_keys": len(ltp_map),
        "ws_ltp_overlays": ws_n,
        "candle_keys_requested": len({ik for _, _, ik in candle_keys}) if fetch_candles else 0,
        "candle_indicators_ok": len(indicators_by_key),
        "candle_errors": candle_errors,
        "status_ok": ok_rows,
        "status_partial": partial_rows,
        "status_failed": failed_rows,
        "elapsed_sec": round(elapsed, 2),
        "updated_at_ist": _now_ist().strftime("%Y-%m-%d %H:%M:%S"),
    }
    logger.info("market_data refresh: %s", summary)
    return summary

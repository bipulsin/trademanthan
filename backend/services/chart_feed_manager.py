"""
On-demand Upstox market feed for the generic security chart modal only.

Separate from smart_future_algo / OI heatmap feed (ensure_market_feed_running).
Starts only when at least one chart client subscribes; stops when all unsubscribe.
"""
from __future__ import annotations

import asyncio
import json
import logging
import ssl
import threading
import time
import uuid
from typing import Any, Dict, Optional, Set

import requests
import websockets

from backend.config import settings
from backend.services.upstox_market_feed import _authorize_ws_url, _normalize_ik
from backend.services.upstox_service import UpstoxService

logger = logging.getLogger(__name__)

_LOCK = threading.Lock()
_SUBSCRIBERS: Dict[str, int] = {}
_LTP_BY_KEY: Dict[str, Dict[str, Any]] = {}
_FEED_THREAD: Optional[threading.Thread] = None
_STOP = threading.Event()
_ACTIVE_SIG: Optional[tuple] = None
_LAST_ERROR: Optional[str] = None
_CHUNK = 50
_STALE_SEC = 90.0


def _active_keys() -> Set[str]:
    with _LOCK:
        return {k for k, n in _SUBSCRIBERS.items() if n > 0}


def _ingest_ltp_only(raw_key: str, feed_val: Any) -> None:
    from backend.services.upstox_market_feed import _extract_oi_ltp_from_feed_value

    ik = _normalize_ik(str(raw_key))
    oi, ltp = _extract_oi_ltp_from_feed_value(feed_val)
    if ltp is None:
        return
    with _LOCK:
        prev = _LTP_BY_KEY.get(ik, {})
        pc = prev.get("prev_close")
        if pc is None:
            pc = prev.get("ltp")
        chg = None
        chg_pct = None
        if pc and float(pc) > 0:
            chg = float(ltp) - float(pc)
            chg_pct = (chg / float(pc)) * 100.0
        _LTP_BY_KEY[ik] = {
            "ltp": float(ltp),
            "oi": oi if oi is not None else prev.get("oi"),
            "prev_close": pc,
            "change": chg,
            "change_pct": chg_pct,
            "ts_mono": time.monotonic(),
        }


async def _listen_chart_only(ws_url: str, keys: list) -> None:
    ssl_ctx = ssl.create_default_context()
    batches = [keys[i : i + _CHUNK] for i in range(0, len(keys), _CHUNK)]
    async with websockets.connect(
        ws_url,
        ssl=ssl_ctx,
        max_size=8 * 1024 * 1024,
        ping_interval=20,
        ping_timeout=120,
    ) as ws:
        for batch in batches:
            if _STOP.is_set():
                return
            msg = {
                "guid": str(uuid.uuid4()),
                "method": "sub",
                "data": {"mode": "ltpc", "instrumentKeys": batch},
            }
            await ws.send(json.dumps(msg).encode("utf-8"))
            await asyncio.sleep(0.1)
        while not _STOP.is_set():
            raw = await asyncio.wait_for(ws.recv(), timeout=180)
            if isinstance(raw, bytes):
                from backend.services.upstox_proto import MarketDataFeedV3_pb2 as feed_pb
                from google.protobuf.json_format import MessageToDict

                fr = feed_pb.FeedResponse()
                fr.ParseFromString(raw)
                d = MessageToDict(fr, preserving_proto_field_name=True)
                feeds = d.get("feeds") or {}
                active = _active_keys()
                for raw_key, feed_val in feeds.items():
                    ik = _normalize_ik(str(raw_key))
                    if ik in active:
                        _ingest_ltp_only(raw_key, feed_val)


async def _run_loop(keys: list) -> None:
    backoff = 2.0
    ux = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    while not _STOP.is_set():
        ux.reload_token_from_storage()
        if not ux.access_token:
            await asyncio.sleep(30)
            continue
        ws_url = _authorize_ws_url(ux.access_token)
        if not ws_url:
            await asyncio.sleep(min(backoff, 60))
            backoff = min(backoff * 1.5, 120)
            continue
        backoff = 2.0
        try:
            keys_now = sorted(_active_keys())
            if not keys_now:
                await asyncio.sleep(2)
                continue
            await _listen_chart_only(ws_url, keys_now)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            global _LAST_ERROR
            _LAST_ERROR = str(e)[:300]
            logger.debug("chart_feed: stream ended: %s", e)
        if _STOP.is_set():
            break
        await asyncio.sleep(min(backoff, 30))


def _thread_main() -> None:
    try:
        asyncio.run(_run_loop([]))
    except Exception as e:
        logger.error("chart_feed: thread fatal: %s", e)


def _restart_feed_if_needed() -> None:
    global _FEED_THREAD, _ACTIVE_SIG, _STOP

    keys = sorted(_active_keys())
    if not keys:
        _STOP.set()
        if _FEED_THREAD and _FEED_THREAD.is_alive():
            _FEED_THREAD.join(timeout=2.0)
        _FEED_THREAD = None
        _ACTIVE_SIG = None
        return

    sig = tuple(keys)
    if _FEED_THREAD and _FEED_THREAD.is_alive() and sig == _ACTIVE_SIG:
        return

    _STOP.set()
    if _FEED_THREAD:
        _FEED_THREAD.join(timeout=2.0)

    _STOP = threading.Event()
    _ACTIVE_SIG = sig
    _FEED_THREAD = threading.Thread(
        target=_thread_main,
        name="chart-upstox-feed",
        daemon=True,
    )
    _FEED_THREAD.start()
    logger.info("chart_feed: started for %s instrument(s)", len(keys))


def chart_subscribe(instrument_key: str) -> Dict[str, Any]:
    ik = _normalize_ik(instrument_key)
    if not ik:
        raise ValueError("instrument_key required")
    with _LOCK:
        _SUBSCRIBERS[ik] = int(_SUBSCRIBERS.get(ik) or 0) + 1
        n = _SUBSCRIBERS[ik]
    _seed_rest_quote(ik)
    _restart_feed_if_needed()
    return {"instrument_key": ik, "refcount": n}


def chart_unsubscribe(instrument_key: str) -> Dict[str, Any]:
    ik = _normalize_ik(instrument_key)
    with _LOCK:
        cur = int(_SUBSCRIBERS.get(ik) or 0)
        if cur <= 1:
            _SUBSCRIBERS.pop(ik, None)
            _LTP_BY_KEY.pop(ik, None)
            n = 0
        else:
            _SUBSCRIBERS[ik] = cur - 1
            n = _SUBSCRIBERS[ik]
    _restart_feed_if_needed()
    return {"instrument_key": ik, "refcount": n}


def _seed_rest_quote(ik: str) -> None:
    try:
        ux = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
        ux.reload_token_from_storage()
        q = ux.get_market_quote_by_key(ik)
        if not q:
            return
        ltp = q.get("last_price") or q.get("ltp") or q.get("close")
        if ltp is None:
            return
        prev = q.get("prev_close") or q.get("ohlc", {}).get("close")
        chg = q.get("net_change")
        chg_pct = q.get("percent_change")
        with _LOCK:
            _LTP_BY_KEY[ik] = {
                "ltp": float(ltp),
                "prev_close": float(prev) if prev else None,
                "change": float(chg) if chg is not None else None,
                "change_pct": float(chg_pct) if chg_pct is not None else None,
                "ts_mono": time.monotonic(),
            }
    except Exception as e:
        logger.debug("chart_feed: REST seed %s: %s", ik, e)


def get_chart_live_quote(instrument_key: str) -> Optional[Dict[str, Any]]:
    ik = _normalize_ik(instrument_key)
    with _LOCK:
        row = _LTP_BY_KEY.get(ik)
    if not row:
        from backend.services.upstox_market_feed import get_ws_quote_for_instrument

        fb = get_ws_quote_for_instrument(ik)
        if fb and fb.get("ltp") is not None:
            return {
                "instrument_key": ik,
                "ltp": fb["ltp"],
                "change": None,
                "change_pct": None,
                "source": "heatmap_feed",
            }
        return None
    age = time.monotonic() - float(row.get("ts_mono") or 0)
    if age > _STALE_SEC:
        return None
    return {
        "instrument_key": ik,
        "ltp": row.get("ltp"),
        "change": row.get("change"),
        "change_pct": row.get("change_pct"),
        "age_sec": round(age, 2),
        "source": "chart_feed",
    }


def chart_feed_status() -> Dict[str, Any]:
    with _LOCK:
        subs = dict(_SUBSCRIBERS)
    alive = _FEED_THREAD is not None and _FEED_THREAD.is_alive()
    return {
        "thread_alive": alive,
        "subscribers": subs,
        "last_error": _LAST_ERROR,
    }

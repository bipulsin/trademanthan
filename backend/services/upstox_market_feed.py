"""
Upstox Market Data Feed v3 — WebSocket + protobuf for live OI and LTP.

REST GET /v2/market-quote/quotes often returns oi=0 for F&O; the streaming feed
(mode ``full``) includes ``MarketFullFeed.oi`` and last trade price.

Flow (official sample):
  GET https://api.upstox.com/v3/feed/market-data-feed/authorize  -> wss URL
  Connect WebSocket -> JSON subscribe { method: sub, data: { mode: full, instrumentKeys: [...] } }
  Receive binary protobuf FeedResponse messages.

See: https://upstox.com/developer/api-documentation/get-market-data-feed
"""
from __future__ import annotations

import asyncio
import json
import logging
import ssl
import threading
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

import requests
import websockets
from google.protobuf.json_format import MessageToDict

from backend.config import settings
from backend.services.upstox_proto import MarketDataFeedV3_pb2 as feed_pb
from backend.services.upstox_service import UpstoxService

logger = logging.getLogger(__name__)

_CACHE_LOCK = threading.Lock()
_OI_LTP_BY_KEY: Dict[str, Dict[str, Any]] = {}
_FEED_THREAD: Optional[threading.Thread] = None
_STOP_EVENT = threading.Event()
_LAST_KEYS_SIG: Optional[Tuple[str, ...]] = None
_FEED_LAST_ERROR: Optional[str] = None
_STALE_AFTER_SEC = 120.0

_CHUNK = 100


def _normalize_ik(key: str) -> str:
    return (key or "").strip().replace(":", "|")


def _extract_oi_ltp_from_feed_value(feed_val: Any) -> Tuple[Optional[int], Optional[float]]:
    """Parse one Feed message dict (from protobuf JSON) for OI and LTP."""
    if not isinstance(feed_val, dict):
        return None, None
    oi: Optional[int] = None
    ltp: Optional[float] = None

    ff = feed_val.get("fullFeed") or feed_val.get("full_feed")
    if isinstance(ff, dict):
        mff = ff.get("marketFF") or ff.get("marketFf") or ff.get("market_ff")
        if isinstance(mff, dict):
            if mff.get("oi") is not None:
                try:
                    oi = int(float(mff["oi"]))
                except (TypeError, ValueError):
                    pass
            ltpc = mff.get("ltpc")
            if isinstance(ltpc, dict) and ltpc.get("ltp") is not None:
                try:
                    ltp = float(ltpc["ltp"])
                except (TypeError, ValueError):
                    pass

    if oi is None or ltp is None:
        flg = feed_val.get("firstLevelWithGreeks") or feed_val.get("first_level_with_greeks")
        if isinstance(flg, dict):
            if oi is None and flg.get("oi") is not None:
                try:
                    oi = int(float(flg["oi"]))
                except (TypeError, ValueError):
                    pass
            ltpc = flg.get("ltpc")
            if ltp is None and isinstance(ltpc, dict) and ltpc.get("ltp") is not None:
                try:
                    ltp = float(ltpc["ltp"])
                except (TypeError, ValueError):
                    pass

    return oi, ltp


def _ingest_feed_response_dict(d: Dict[str, Any]) -> None:
    feeds = d.get("feeds")
    if not isinstance(feeds, dict):
        return
    now = time.monotonic()
    with _CACHE_LOCK:
        for raw_key, feed_val in feeds.items():
            ik = _normalize_ik(str(raw_key))
            oi, ltp = _extract_oi_ltp_from_feed_value(feed_val)
            if oi is None and ltp is None:
                continue
            prev = _OI_LTP_BY_KEY.get(ik, {})
            row = {
                "oi": int(oi) if oi is not None else prev.get("oi"),
                "ltp": float(ltp) if ltp is not None else prev.get("ltp"),
                "ts_mono": now,
            }
            if row.get("oi") is None and row.get("ltp") is None:
                continue
            _OI_LTP_BY_KEY[ik] = row


def _decode_and_ingest(binary: bytes) -> None:
    try:
        fr = feed_pb.FeedResponse()
        fr.ParseFromString(binary)
        d = MessageToDict(fr, preserving_proto_field_name=True)
        _ingest_feed_response_dict(d)
    except Exception as e:
        logger.debug("upstox_market_feed: decode skip: %s", e)


def _authorize_ws_url(access_token: str) -> Optional[str]:
    global _FEED_LAST_ERROR
    try:
        r = requests.get(
            "https://api.upstox.com/v3/feed/market-data-feed/authorize",
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {access_token}",
            },
            timeout=20,
        )
        data = r.json() if r.content else {}
        if r.status_code != 200 or (data.get("status") or "").lower() != "success":
            _FEED_LAST_ERROR = str(data.get("message") or data.get("error") or r.text)[:300]
            logger.warning("upstox_market_feed: authorize failed: %s", _FEED_LAST_ERROR)
            return None
        inner = data.get("data") or {}
        uri = (
            inner.get("authorizedRedirectUri")
            or inner.get("authorized_redirect_uri")
            or inner.get("authorizedRedirectURL")
        )
        if not uri:
            _FEED_LAST_ERROR = "authorize: missing redirect URI"
            return None
        _FEED_LAST_ERROR = None
        return str(uri).strip()
    except Exception as e:
        _FEED_LAST_ERROR = str(e)
        logger.warning("upstox_market_feed: authorize exception: %s", e)
        return None


async def _subscribe_and_listen(ws_url: str, batches: List[List[str]]) -> None:
    ssl_context = ssl.create_default_context()
    async with websockets.connect(
        ws_url,
        ssl=ssl_context,
        max_size=16 * 1024 * 1024,
        ping_interval=20,
        ping_timeout=120,
    ) as ws:
        for batch in batches:
            if _STOP_EVENT.is_set():
                return
            msg = {
                "guid": str(uuid.uuid4()),
                "method": "sub",
                "data": {"mode": "full", "instrumentKeys": batch},
            }
            await ws.send(json.dumps(msg).encode("utf-8"))
            await asyncio.sleep(0.15)
        logger.info("upstox_market_feed: subscribed %s chunks, listening", len(batches))
        while not _STOP_EVENT.is_set():
            raw = await asyncio.wait_for(ws.recv(), timeout=180)
            if isinstance(raw, bytes):
                _decode_and_ingest(raw)


async def _run_connection_loop(batches: List[List[str]]) -> None:
    backoff = 2.0
    ux = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    ux.reload_token_from_storage()
    if not ux.access_token:
        logger.warning("upstox_market_feed: no Upstox access token; feed disabled")
        return
    while not _STOP_EVENT.is_set():
        token = ux.access_token
        if not token:
            logger.warning("upstox_market_feed: missing token, retry in 60s")
            await asyncio.sleep(60)
            ux.reload_token_from_storage()
            continue
        ws_url = _authorize_ws_url(token)
        if not ws_url:
            await asyncio.sleep(min(backoff, 60.0))
            backoff = min(backoff * 1.5, 120.0)
            continue
        backoff = 2.0
        try:
            await _subscribe_and_listen(ws_url, batches)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning("upstox_market_feed: stream ended: %s", e, exc_info=True)
        if _STOP_EVENT.is_set():
            break
        await asyncio.sleep(min(backoff, 30.0))
        backoff = min(backoff * 1.5, 60.0)
        ux.reload_token_from_storage()


def _thread_main(batches: List[List[str]]) -> None:
    try:
        asyncio.run(_run_connection_loop(batches))
    except Exception as e:
        logger.error("upstox_market_feed: thread fatal: %s", e, exc_info=True)


def ensure_market_feed_running(instrument_keys: List[str]) -> None:
    """
    Start or restart background WebSocket if instrument universe changed.
    No-op when UPSTOX_MARKET_FEED_ENABLED is false or list empty.
    """
    global _FEED_THREAD, _LAST_KEYS_SIG, _STOP_EVENT

    if not getattr(settings, "UPSTOX_MARKET_FEED_ENABLED", True):
        return
    if not getattr(settings, "UPSTOX_OI_ENABLED", True):
        return
    keys = [k.strip() for k in instrument_keys if (k or "").strip()]
    if not keys:
        return

    sig = tuple(sorted(keys))
    if _FEED_THREAD is not None and _FEED_THREAD.is_alive() and sig == _LAST_KEYS_SIG:
        return

    _STOP_EVENT.set()
    if _FEED_THREAD is not None:
        _FEED_THREAD.join(timeout=3.0)

    _STOP_EVENT = threading.Event()
    _LAST_KEYS_SIG = sig
    batches = [keys[i : i + _CHUNK] for i in range(0, len(keys), _CHUNK)]
    _FEED_THREAD = threading.Thread(
        target=_thread_main,
        args=(batches,),
        name="upstox-market-feed-v3",
        daemon=True,
    )
    _FEED_THREAD.start()
    logger.info(
        "upstox_market_feed: started WebSocket feed for %s instruments in %s chunks",
        len(keys),
        len(batches),
    )


def get_ws_quote_for_instrument(instrument_key: str) -> Optional[Dict[str, Any]]:
    """
    Latest OI/LTP from WebSocket cache if fresh (<= ~2 min).
    Returns dict: oi, ltp, age_sec — or None if unavailable/stale/disabled.
    """
    if not getattr(settings, "UPSTOX_MARKET_FEED_ENABLED", True):
        return None
    ik = _normalize_ik(instrument_key)
    with _CACHE_LOCK:
        row = _OI_LTP_BY_KEY.get(ik)
        if not row:
            return None
        ts = float(row.get("ts_mono") or 0)
        age = time.monotonic() - ts
        if age > _STALE_AFTER_SEC:
            return None
        oi = row.get("oi")
        ltp = row.get("ltp")
        if oi is None and ltp is None:
            return None
        return {
            "oi": oi,
            "ltp": ltp,
            "age_sec": round(age, 2),
        }


def feed_status() -> Dict[str, Any]:
    """Diagnostics for /scan or logs."""
    with _CACHE_LOCK:
        n = len(_OI_LTP_BY_KEY)
    alive = _FEED_THREAD is not None and _FEED_THREAD.is_alive()
    return {
        "enabled": getattr(settings, "UPSTOX_MARKET_FEED_ENABLED", True),
        "thread_alive": alive,
        "cached_instruments": n,
        "last_error": _FEED_LAST_ERROR,
    }

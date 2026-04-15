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

_CHUNK = 100


def _stale_after_sec() -> float:
    return float(getattr(settings, "UPSTOX_MARKET_FEED_STALE_SEC", 120.0) or 120.0)


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
            eff_oi = int(oi) if oi is not None else prev.get("oi")
            eff_ltp = float(ltp) if ltp is not None else prev.get("ltp")
            oi_chg_tick = 0
            if eff_oi is not None and prev.get("oi") is not None:
                try:
                    oi_chg_tick = int(eff_oi) - int(prev["oi"])
                except (TypeError, ValueError):
                    oi_chg_tick = 0
            row = {
                "oi": eff_oi,
                "ltp": eff_ltp,
                "oi_change": oi_chg_tick,
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
        if r.status_code == 401:
            _FEED_LAST_ERROR = "authorize 401 — token may be expired"
            logger.warning("upstox_market_feed: authorize 401")
            return None
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
            ux.reload_token_from_storage()
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
        if age > _stale_after_sec():
            return None
        oi = row.get("oi")
        ltp = row.get("ltp")
        if oi is None and ltp is None:
            return None
        return {
            "oi": oi,
            "ltp": ltp,
            "oi_change": row.get("oi_change", 0),
            "age_sec": round(age, 2),
        }


def feed_status() -> Dict[str, Any]:
    """Diagnostics for /scan or logs."""
    with _CACHE_LOCK:
        n = len(_OI_LTP_BY_KEY)
    alive = _FEED_THREAD is not None and _FEED_THREAD.is_alive()
    return {
        "enabled": getattr(settings, "UPSTOX_MARKET_FEED_ENABLED", True),
        "stale_after_sec": _stale_after_sec(),
        "thread_alive": alive,
        "cached_instruments": n,
        "last_error": _FEED_LAST_ERROR,
        "universe_keys": len(_LAST_KEYS_SIG or ()),
    }


def try_oiquote_from_feed_for_gate(stock: str, fut_instrument_key: str):
    """
    Build ``OIQuote`` from WebSocket cache + one REST quote for prev-close / net_change.
    Used when heatmap row is missing or REST OI is zero. Returns None if feed disabled or stale.
    """
    import time as time_mod

    from backend.services.oi_integration import OIQuote

    if not getattr(settings, "UPSTOX_MARKET_FEED_ENABLED", True):
        return None
    if not fut_instrument_key or not str(fut_instrument_key).strip():
        return None
    ik = _normalize_ik(fut_instrument_key)
    wsq = get_ws_quote_for_instrument(ik)
    if not wsq:
        return None
    oi = wsq.get("oi")
    if oi is None or int(oi) <= 0:
        return None
    oi = int(oi)
    oi_chg = int(wsq.get("oi_change") or 0)
    ltp = float(wsq.get("ltp") or 0.0)
    try:
        ux = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
        ux.reload_token_from_storage()
        q = ux.get_market_quote_by_key(ik) or {}
        if ltp <= 1e-9:
            ltp = float(q.get("last_price") or 0.0)
        net_chg = float(q.get("net_change") or 0)
        ohlc = q.get("ohlc") if isinstance(q.get("ohlc"), dict) else {}
        open_ = float(ohlc.get("open") or 0)
        if abs(net_chg) > 1e-9:
            prev = ltp - net_chg
        elif open_ > 1e-9:
            prev = open_
        else:
            prev = float(ohlc.get("close") or q.get("close_price") or 0)
        prev_oi = max(0, oi - oi_chg)
        return OIQuote(
            symbol=(stock or "").strip().upper(),
            oi=oi,
            change_in_oi=oi_chg,
            last_price=ltp,
            prev_close=float(prev),
            prev_oi=int(prev_oi),
            fetched_at=time_mod.time(),
        )
    except Exception as e:
        logger.debug("upstox_market_feed: OIQuote from feed failed %s: %s", ik, e)
        return None

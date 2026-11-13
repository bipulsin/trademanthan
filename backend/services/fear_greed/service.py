"""Fear & Greed data layer: resolve instrument, fetch candles via Upstox, cache."""
from __future__ import annotations

import logging
import threading
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pytz

from backend.config import settings
from backend.services.fear_greed.compute import DEFAULT_WEIGHTS, compute_fear_greed_from_candles
from backend.services.upstox_service import UpstoxService

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")
CACHE_TTL_SEC = 120.0
# ~252 trading days ≈ 370 calendar days; pad for holidays.
DEFAULT_LOOKBACK_TRADING_DAYS = 252
DEFAULT_CALENDAR_DAYS_BACK = 400
BENCHMARK_KEY = UpstoxService.NIFTY50_KEY

_INDEX_ALIASES: Dict[str, Tuple[str, str]] = {
    "NIFTY": ("NSE_INDEX|Nifty 50", "NIFTY"),
    "NIFTY50": ("NSE_INDEX|Nifty 50", "NIFTY"),
    "NIFTY 50": ("NSE_INDEX|Nifty 50", "NIFTY"),
    "NIFTY50 INDEX": ("NSE_INDEX|Nifty 50", "NIFTY"),
    "BANKNIFTY": ("NSE_INDEX|Nifty Bank", "BANKNIFTY"),
    "BANK NIFTY": ("NSE_INDEX|Nifty Bank", "BANKNIFTY"),
    "NIFTY BANK": ("NSE_INDEX|Nifty Bank", "BANKNIFTY"),
    "FINNIFTY": ("NSE_INDEX|Nifty Fin Service", "FINNIFTY"),
    "MIDCPNIFTY": ("NSE_INDEX|NIFTY MID SELECT", "MIDCPNIFTY"),
    "SENSEX": ("BSE_INDEX|SENSEX", "SENSEX"),
    "INDIA VIX": ("NSE_INDEX|India VIX", "INDIA VIX"),
    "INDIAVIX": ("NSE_INDEX|India VIX", "INDIA VIX"),
}

_cache_lock = threading.Lock()
_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}


def _cache_key(
    instrument: str,
    lookback: int,
    timeframe: str,
    weights: Optional[Dict[str, float]],
) -> str:
    w = ""
    if weights:
        w = ",".join(f"{k}:{weights[k]}" for k in sorted(weights))
    return f"{instrument.strip().upper()}|{lookback}|{timeframe}|{w}"


def _get_cached(key: str) -> Optional[Dict[str, Any]]:
    with _cache_lock:
        hit = _cache.get(key)
        if not hit:
            return None
        ts, payload = hit
        if time.time() - ts > CACHE_TTL_SEC:
            _cache.pop(key, None)
            return None
        return dict(payload)


def _put_cached(key: str, payload: Dict[str, Any]) -> None:
    with _cache_lock:
        _cache[key] = (time.time(), dict(payload))


def clear_fear_greed_cache() -> None:
    with _cache_lock:
        _cache.clear()


def _upstox() -> UpstoxService:
    ux = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    try:
        ux.reload_token_from_storage()
    except Exception:
        logger.debug("fear_greed: token reload skipped", exc_info=True)
    return ux


def _normalize_name(name: str) -> str:
    return " ".join(str(name or "").strip().upper().split())


def _fut_from_arbitrage(symbol: str) -> Optional[Dict[str, str]]:
    try:
        from backend.services.chart_instrument_resolver import resolve_chart_instrument

        meta = resolve_chart_instrument(symbol, "FUT")
        if meta and meta.get("instrument_key"):
            return {
                "instrument_key": str(meta["instrument_key"]),
                "display_symbol": str(meta.get("display_symbol") or symbol),
                "symbol": str(meta.get("symbol") or symbol),
            }
    except Exception as e:
        logger.debug("fear_greed: fut resolve %s: %s", symbol, e)
    return None


def _equity_key(ux: UpstoxService, symbol: str) -> Optional[str]:
    try:
        key = ux.get_instrument_key(symbol)
        if key:
            return str(key).replace(":", "|")
    except Exception:
        logger.debug("fear_greed: equity key %s failed", symbol, exc_info=True)
    return None


def _mcx_fut(symbol: str) -> Optional[Dict[str, str]]:
    try:
        from backend.services.chart_instrument_resolver import resolve_chart_instrument

        meta = resolve_chart_instrument(symbol, "FUT", exchange="MCX")
        if meta and meta.get("instrument_key"):
            return {
                "instrument_key": str(meta["instrument_key"]),
                "display_symbol": str(meta.get("display_symbol") or symbol),
                "symbol": str(meta.get("symbol") or symbol),
            }
    except Exception as e:
        logger.debug("fear_greed: MCX resolve %s: %s", symbol, e)
    return None


def resolve_instrument(name: str, ux: Optional[UpstoxService] = None) -> Dict[str, Any]:
    """Resolve a free-form instrument name to Upstox keys (spot + optional futures)."""
    raw = _normalize_name(name)
    if not raw:
        raise ValueError("instrument name required")

    ux = ux or _upstox()
    out: Dict[str, Any] = {
        "instrument": raw,
        "spot_key": None,
        "fut_key": None,
        "futures_contract": None,
        "kind": "unknown",
    }

    if raw in _INDEX_ALIASES:
        spot_key, canon = _INDEX_ALIASES[raw]
        out["instrument"] = canon
        out["spot_key"] = spot_key
        out["kind"] = "index"
        fut = _fut_from_arbitrage(canon)
        if fut:
            out["fut_key"] = fut["instrument_key"]
            out["futures_contract"] = fut["display_symbol"]
        return out

    # Stock futures first (arbitrage_master), then equity spot.
    fut = _fut_from_arbitrage(raw)
    if fut:
        out["kind"] = "stock_fut"
        out["fut_key"] = fut["instrument_key"]
        out["futures_contract"] = fut["display_symbol"]
        out["instrument"] = fut["symbol"]
        eq = _equity_key(ux, fut["symbol"])
        out["spot_key"] = eq or fut["instrument_key"]
        return out

    eq = _equity_key(ux, raw)
    if eq:
        out["kind"] = "equity"
        out["spot_key"] = eq
        out["instrument"] = raw
        return out

    mcx = _mcx_fut(raw)
    if mcx:
        out["kind"] = "commodity"
        out["fut_key"] = mcx["instrument_key"]
        out["spot_key"] = mcx["instrument_key"]
        out["futures_contract"] = mcx["display_symbol"]
        out["instrument"] = mcx["symbol"]
        return out

    # Last resort: treat as index-style key
    out["kind"] = "index"
    out["spot_key"] = f"NSE_INDEX|{raw}"
    out["instrument"] = raw
    return out


def _fetch_daily(
    ux: UpstoxService,
    instrument_key: str,
    days_back: int,
) -> List[Dict[str, Any]]:
    if not instrument_key:
        return []
    try:
        candles = ux.get_historical_candles_by_instrument_key(
            instrument_key,
            interval="days/1",
            days_back=int(days_back),
        )
        if not candles:
            return []
        # Chronological oldest → newest
        def _ts(c: Dict[str, Any]) -> str:
            return str(c.get("timestamp") or "")

        return sorted(candles, key=_ts)
    except Exception as e:
        logger.warning("fear_greed: candle fetch failed %s: %s", instrument_key, e)
        return []


def _trim_trading_days(candles: List[Dict[str, Any]], n: int) -> List[Dict[str, Any]]:
    if n <= 0 or len(candles) <= n:
        return candles
    return candles[-n:]


def _candle_day(c: Dict[str, Any]) -> str:
    return str(c.get("timestamp") or "")[:10]


def _merge_spot_with_fut_oi(
    spot: Optional[List[Dict[str, Any]]],
    fut: Optional[List[Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    """Use the longer price series; overlay futures OI/volume by calendar date."""
    if spot and fut:
        base = spot if len(spot) >= len(fut) else fut
        if base is fut:
            return list(base)
        oi_by_day = {_candle_day(c): c for c in fut if _candle_day(c)}
        merged: List[Dict[str, Any]] = []
        for c in base:
            row = dict(c)
            f = oi_by_day.get(_candle_day(c))
            if f:
                oi = f.get("oi")
                if oi is not None:
                    row["oi"] = oi
                # Prefer futures volume when spot volume is missing/zero
                vol = f.get("volume")
                if vol and not (row.get("volume") or 0):
                    row["volume"] = vol
            merged.append(row)
        return merged
    if spot:
        return list(spot)
    if fut:
        return list(fut)
    return []


def get_fear_greed(
    instrument: str,
    *,
    lookback: int = DEFAULT_LOOKBACK_TRADING_DAYS,
    timeframe: str = "1d",
    weights: Optional[Dict[str, float]] = None,
    use_cache: bool = True,
    upstox_service: Optional[UpstoxService] = None,
) -> Dict[str, Any]:
    """Compute Fear & Greed for ``instrument``; short-lived per-instrument cache."""
    name = _normalize_name(instrument)
    lookback_n = max(60, min(int(lookback or DEFAULT_LOOKBACK_TRADING_DAYS), 400))
    tf = (timeframe or "1d").strip().lower()
    weight_map = dict(DEFAULT_WEIGHTS)
    if weights:
        for k, v in weights.items():
            if k in weight_map:
                try:
                    weight_map[k] = float(v)
                except (TypeError, ValueError):
                    pass

    ck = _cache_key(name, lookback_n, tf, weight_map if weights else None)
    if use_cache:
        cached = _get_cached(ck)
        if cached is not None:
            cached["cached"] = True
            return cached

    ux = upstox_service or _upstox()
    resolved = resolve_instrument(name, ux)
    calendar_days = max(lookback_n + 40, int(lookback_n * 1.55))

    spot_key = resolved.get("spot_key")
    fut_key = resolved.get("fut_key")
    # Prefer the longer history series for price/vol/momentum. Current-month
    # futures often only have ~1–3 months of daily bars; index/equity spots
    # usually cover a full year. OI still comes from futures when present.
    spot_candles: Optional[List[Dict[str, Any]]] = None
    fut_candles: Optional[List[Dict[str, Any]]] = None

    if spot_key:
        spot_candles = _trim_trading_days(
            _fetch_daily(ux, str(spot_key), calendar_days), lookback_n
        )
    if fut_key and (not spot_key or str(fut_key) != str(spot_key)):
        fut_candles = _trim_trading_days(
            _fetch_daily(ux, str(fut_key), calendar_days), lookback_n
        )
    elif fut_key and spot_key and str(fut_key) == str(spot_key):
        fut_candles = spot_candles

    primary = _merge_spot_with_fut_oi(spot_candles, fut_candles)
    primary_key = (
        str(spot_key)
        if spot_candles and (not fut_candles or len(spot_candles) >= len(fut_candles))
        else str(fut_key or spot_key or "")
    )

    # Benchmark for relative strength (skip when instrument IS NIFTY)
    benchmark_candles: Optional[List[Dict[str, Any]]] = None
    is_nifty = str(resolved.get("spot_key") or "") == BENCHMARK_KEY or resolved.get(
        "instrument"
    ) in ("NIFTY", "NIFTY50")
    if not is_nifty:
        benchmark_candles = _trim_trading_days(
            _fetch_daily(ux, BENCHMARK_KEY, calendar_days), lookback_n
        )
    else:
        # Self-RS vs BANKNIFTY when available, else skip
        try:
            bank_key = UpstoxService.BANKNIFTY_KEY
            benchmark_candles = _trim_trading_days(
                _fetch_daily(ux, bank_key, calendar_days), lookback_n
            )
        except Exception:
            benchmark_candles = None

    if not primary or len(primary) < 60:
        payload = {
            "success": False,
            "instrument": resolved.get("instrument") or name,
            "futures_contract": resolved.get("futures_contract"),
            "score": None,
            "zone": None,
            "timestamp": datetime.now(IST).isoformat(),
            "indicators": {},
            "skipped": {"all": "insufficient candle data"},
            "weights_used": {},
            "message": "No or insufficient daily candles for instrument",
            "cached": False,
        }
        if use_cache:
            _put_cached(ck, payload)
        return payload

    calc = compute_fear_greed_from_candles(
        primary,
        spot_candles=spot_candles,
        fut_candles=fut_candles if fut_candles else (primary if fut_key else None),
        benchmark_candles=benchmark_candles,
        weights=weight_map,
    )

    # For index without separate fut series, futures_premium already skipped.
    # If primary is futures and we have spot, premium uses those.

    payload = {
        "success": True,
        "instrument": resolved.get("instrument") or name,
        "futures_contract": resolved.get("futures_contract"),
        "instrument_key": primary_key,
        "kind": resolved.get("kind"),
        "score": calc["score"],
        "zone": calc["zone"],
        "timestamp": datetime.now(IST).isoformat(),
        "indicators": calc["indicators"],
        "skipped": calc["skipped"],
        "weights_used": calc["weights_used"],
        "bars_used": calc.get("bars_used"),
        "lookback": lookback_n,
        "timeframe": tf,
        "disclaimer": "Context/filter only — not an entry or exit signal.",
        "cached": False,
    }
    if use_cache:
        _put_cached(ck, payload)
    return payload

"""NSE MCP + Upstox market-data gateway.

MCP is attempted only when both ``NSE_MCP_ENABLED`` and
``NSE_MCP_PRODUCTION_APPROVED`` are true and the instrument/data type matches
a tool verified against the live servers. Otherwise Upstox is called
immediately — unsupported futures, options, open interest, and intraday
candles do not wait for an MCP timeout.

One MCP attempt, then Upstox. Missing prices stay missing. Candle series are
never spliced across sources.
"""
from __future__ import annotations

import logging
import re
import threading
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any, Callable, Dict, List, Optional

import pytz

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

_ISIN = re.compile(r"^IN[A-Z0-9]{10}$")
_OPTION = re.compile(r"\d+(CE|PE)$")
_FUTURES_SEGMENTS = {"NSE_FO", "NFO", "BSE_FO", "MCX_FO"}

# Exact index names from the cm_get_index_quote schema / a successful call.
_INDEX_TO_NSE = {
    "nifty 50": "NIFTY 50",
    "nifty50": "NIFTY 50",
    "nifty bank": "NIFTY BANK",
    "banknifty": "NIFTY BANK",
    "india vix": "INDIA VIX",
}

_BREAKER_FAILURES = 3
_BREAKER_COOLDOWN_SEC = 60.0
_QUOTE_CACHE_TTL_SEC = 15.0
_DAILY_CACHE_TTL_SEC = 600.0
_CACHE_MAX = 256
_COALESCE_WAIT_SEC = 12.0

_lock = threading.Lock()
_inflight: Dict[str, "_Slot"] = {}
_cache: Dict[str, "_CacheEntry"] = {}
_breaker_failures = 0
_breaker_open_until = 0.0
_client_singleton = None
_client_lock = threading.Lock()


@dataclass
class Capability:
    supported: bool
    server: Optional[str] = None
    tool: Optional[str] = None
    kind: Optional[str] = None
    symbol_or_index: Optional[str] = None
    reason: str = ""


@dataclass
class MarketDataResult:
    status: str
    source: str
    source_timestamp: Optional[str]
    ingest_time: str
    freshness: str
    fallback_reason: Optional[str]
    calculation_available: bool
    quote: Optional[Dict[str, Any]] = None
    candles: Optional[List[Dict[str, Any]]] = None
    instrument_key: str = ""

    def as_dict(self) -> Dict[str, Any]:
        return {
            "status": self.status,
            "source": self.source,
            "source_timestamp": self.source_timestamp,
            "ingest_time": self.ingest_time,
            "freshness": self.freshness,
            "fallback_reason": self.fallback_reason,
            "calculation_available": self.calculation_available,
        }


@dataclass
class _Slot:
    event: threading.Event = field(default_factory=threading.Event)
    result: Optional[MarketDataResult] = None


@dataclass
class _CacheEntry:
    stored_at: float
    result: MarketDataResult


def reset_gateway_state() -> None:
    """Drop cache, breaker, and in-flight slots. Used by tests."""
    global _breaker_failures, _breaker_open_until, _client_singleton
    with _lock:
        _inflight.clear()
        _cache.clear()
        _breaker_failures = 0
        _breaker_open_until = 0.0
    with _client_lock:
        _client_singleton = None


def flags_allow_mcp() -> bool:
    from backend.config import settings

    return bool(settings.NSE_MCP_ENABLED) and bool(settings.NSE_MCP_PRODUCTION_APPROVED)


def resolve_capability(
    instrument_key: str,
    *,
    data_type: str,
    interval: Optional[str] = None,
) -> Capability:
    """Return the verified MCP tool for this request, or a reason to skip it."""
    key = (instrument_key or "").strip()
    if not key:
        return Capability(False, reason="unsupported_instrument")
    if data_type == "oi":
        return Capability(False, reason="open_interest")
    segment, tail = _split_key(key)
    if segment in _FUTURES_SEGMENTS or "FUT" in tail.upper() or _OPTION.search(tail.upper()):
        return Capability(False, reason="futures_or_options")

    if data_type == "candles":
        iv = (interval or "").strip().lower()
        if iv not in {"days/1", "day", "1d"}:
            return Capability(False, reason="intraday_candles")
        index_name = _index_name(key)
        if index_name:
            return Capability(
                True,
                server="bhavcopy",
                tool="get_index_history",
                kind="index_daily",
                symbol_or_index=index_name,
            )
        symbol = _equity_symbol(key)
        if symbol:
            return Capability(
                True,
                server="bhavcopy",
                tool="get_stock_history",
                kind="equity_daily",
                symbol_or_index=symbol,
            )
        if segment == "NSE_EQ" and _ISIN.match(tail.upper()):
            return Capability(False, reason="isin_without_symbol")
        return Capability(False, reason="unsupported_instrument")

    index_name = _index_name(key)
    if index_name:
        return Capability(
            True,
            server="cmmkt",
            tool="cm_get_index_quote",
            kind="index_quote",
            symbol_or_index=index_name,
        )
    symbol = _equity_symbol(key)
    if symbol:
        return Capability(
            True,
            server="cmmkt",
            tool="cm_get_stock_quote",
            kind="equity_quote",
            symbol_or_index=symbol,
        )
    if segment == "NSE_EQ" and _ISIN.match(tail.upper()):
        return Capability(False, reason="isin_without_symbol")
    return Capability(False, reason="unsupported_instrument")


def fetch_quote(
    instrument_key: str,
    *,
    upstox: Any = None,
    client: Any = None,
    now: Optional[datetime] = None,
    purpose: str = "trading",
) -> MarketDataResult:
    """Quote for dashboard indices or a cash symbol. Futures never touch MCP."""
    cache_key = f"quote|{instrument_key}"
    return _coalesced(
        cache_key,
        lambda: _fetch_quote_body(
            instrument_key, upstox=upstox, client=client, now=now, purpose=purpose
        ),
    )


def fetch_candles(
    instrument_key: str,
    *,
    interval: str,
    days_back: int,
    range_end_date: Optional[date] = None,
    upstox: Any = None,
    client: Any = None,
    now: Optional[datetime] = None,
    purpose: str = "trading",
) -> MarketDataResult:
    """One candle series from a single source. Intraday and futures skip MCP."""
    end = range_end_date.isoformat() if range_end_date else ""
    cache_key = f"candles|{instrument_key}|{interval}|{int(days_back)}|{end}"
    return _coalesced(
        cache_key,
        lambda: _fetch_candles_body(
            instrument_key,
            interval=interval,
            days_back=days_back,
            range_end_date=range_end_date,
            upstox=upstox,
            client=client,
            now=now,
            purpose=purpose,
        ),
    )


def candle_series_continuous(
    left: Optional[List[Dict[str, Any]]],
    right: Optional[List[Dict[str, Any]]],
) -> bool:
    """True when two daily series share a date whose closes agree."""
    if not left or not right:
        return False
    closes: Dict[str, float] = {}
    for bar in left:
        day = str(bar.get("timestamp") or "")[:10]
        try:
            closes[day] = float(bar.get("close"))
        except (TypeError, ValueError):
            return False
    overlap = False
    for bar in right:
        day = str(bar.get("timestamp") or "")[:10]
        if day not in closes:
            continue
        overlap = True
        try:
            if abs(closes[day] - float(bar.get("close"))) > 0.05:
                return False
        except (TypeError, ValueError):
            return False
    return overlap


def join_candle_series(
    older: Optional[List[Dict[str, Any]]],
    newer: Optional[List[Dict[str, Any]]],
    *,
    instrument_key: str = "",
) -> MarketDataResult:
    """Refuse a mixed-source series. Continuity failure marks the calc unavailable."""
    if not candle_series_continuous(older, newer):
        return _unavailable(
            instrument_key,
            fallback_reason="mixed_series_discontinuous",
            calculation_available=False,
        )
    return _unavailable(
        instrument_key,
        fallback_reason="mixed_series_refused",
        calculation_available=False,
    )


def _fetch_quote_body(
    instrument_key: str,
    *,
    upstox: Any,
    client: Any,
    now: Optional[datetime],
    purpose: str,
) -> MarketDataResult:
    moment = _as_ist(now)
    cap = resolve_capability(instrument_key, data_type="quote")
    skip = _skip_mcp_reason(cap)
    if skip:
        return _upstox_quote(instrument_key, upstox, moment, fallback_reason=skip)

    cached = _cache_get(f"quote|{instrument_key}", moment, quote=True)
    if cached is not None:
        return cached

    try:
        payload = _call_mcp(
            cap,
            client,
            purpose="trading" if purpose == "trading" else "historical",
            arguments=_quote_arguments(cap),
        )
        _breaker_success()
    except Exception as exc:
        reason = getattr(exc, "reason", None) or "mcp_error"
        _breaker_failure()
        logger.info("nse mcp quote fallback %s: %s", instrument_key, reason)
        return _upstox_quote(instrument_key, upstox, moment, fallback_reason=str(reason))

    parsed = _quote_from_mcp(cap, payload, moment)
    if parsed is None:
        return _upstox_quote(instrument_key, upstox, moment, fallback_reason="malformed")
    quote, source_ts, fresh = parsed
    if not fresh:
        return _upstox_quote(instrument_key, upstox, moment, fallback_reason="stale_mcp")
    result = _ok_quote(instrument_key, quote, source_ts, moment, source="nse_mcp")
    _cache_put(f"quote|{instrument_key}", result)
    return result


def _fetch_candles_body(
    instrument_key: str,
    *,
    interval: str,
    days_back: int,
    range_end_date: Optional[date],
    upstox: Any,
    client: Any,
    now: Optional[datetime],
    purpose: str,
) -> MarketDataResult:
    moment = _as_ist(now)
    cap = resolve_capability(instrument_key, data_type="candles", interval=interval)
    skip = _skip_mcp_reason(cap)
    if skip:
        return _upstox_candles(
            instrument_key,
            upstox,
            moment,
            interval=interval,
            days_back=days_back,
            range_end_date=range_end_date,
            fallback_reason=skip,
        )

    end = range_end_date or moment.date()
    cache_key = f"candles|{instrument_key}|{interval}|{int(days_back)}|{end.isoformat()}"
    cached = _cache_get(cache_key, moment, quote=False)
    if cached is not None:
        return cached

    mcp_purpose = "historical" if purpose != "trading" or (interval or "").startswith("day") else purpose
    try:
        payload = _call_mcp(
            cap,
            client,
            purpose=mcp_purpose,
            arguments=_history_arguments(cap, end=end, days_back=days_back),
        )
        _breaker_success()
    except Exception as exc:
        reason = getattr(exc, "reason", None) or "mcp_error"
        _breaker_failure()
        logger.info("nse mcp candles fallback %s: %s", instrument_key, reason)
        return _upstox_candles(
            instrument_key,
            upstox,
            moment,
            interval=interval,
            days_back=days_back,
            range_end_date=range_end_date,
            fallback_reason=str(reason),
        )

    bars = _bars_from_history(payload)
    if not bars or not _history_covers(bars, end, int(days_back)):
        return _upstox_candles(
            instrument_key,
            upstox,
            moment,
            interval=interval,
            days_back=days_back,
            range_end_date=range_end_date,
            fallback_reason="incomplete_series",
        )
    result = _ok_candles(instrument_key, bars, moment, source="nse_mcp")
    _cache_put(cache_key, result)
    return result


def _skip_mcp_reason(cap: Capability) -> Optional[str]:
    if not flags_allow_mcp():
        from backend.config import settings

        if not settings.NSE_MCP_ENABLED:
            return "nse_mcp_disabled"
        return "nse_mcp_not_approved"
    if not cap.supported:
        return cap.reason or "unsupported_instrument"
    if not _breaker_allow():
        return "circuit_open"
    return None


def _call_mcp(cap: Capability, client: Any, *, purpose: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
    from backend.config import settings

    timeout = (
        float(settings.NSE_MCP_HISTORICAL_TIMEOUT_SEC)
        if purpose == "historical"
        else float(settings.NSE_MCP_TRADING_TIMEOUT_SEC)
    )
    mcp = client if client is not None else _default_client()
    return mcp.call_tool(cap.server, cap.tool, arguments, timeout)


def _default_client():
    global _client_singleton
    with _client_lock:
        if _client_singleton is None:
            from backend.services.market_data.nse_mcp_client import NseMcpClient

            _client_singleton = NseMcpClient()
        return _client_singleton


def _quote_arguments(cap: Capability) -> Dict[str, Any]:
    if cap.kind == "index_quote":
        return {"indexName": cap.symbol_or_index}
    return {"symbol": cap.symbol_or_index}


def _history_arguments(cap: Capability, *, end: date, days_back: int) -> Dict[str, Any]:
    months = min(3, max(1, (int(days_back) + 20) // 21))
    end_s = end.strftime("%Y-%m-%d")
    if cap.kind == "index_daily":
        return {"indexName": cap.symbol_or_index, "months": months, "endDate": end_s}
    return {"symbol": cap.symbol_or_index, "months": months, "endDate": end_s}


def _quote_from_mcp(
    cap: Capability, payload: Dict[str, Any], now: datetime
) -> Optional[tuple]:
    from backend.config import settings

    if cap.kind == "equity_quote":
        stock = payload.get("stock")
        if not isinstance(stock, dict):
            return None
        ltp = _positive(stock.get("lastTradedPrice"))
        if ltp is None:
            return None
        source_raw = stock.get("latestTimestamp")
        source_dt = _parse_source_ts(source_raw)
        quote = _quote_dict(
            ltp,
            prev=_positive(stock.get("preClosePrice")),
            open_px=_positive(stock.get("openPrice")),
            high=_positive(stock.get("highPrice")),
            low=_positive(stock.get("lowPrice")),
        )
    elif cap.kind == "index_quote":
        index = payload.get("index")
        if not isinstance(index, dict):
            return None
        ltp = _positive(index.get("last"))
        if ltp is None:
            return None
        source_raw = payload.get("nseTimestamp") or index.get("timeVal")
        source_dt = _parse_source_ts(source_raw)
        quote = _quote_dict(
            ltp,
            prev=_positive(index.get("previousClose")),
            open_px=_positive(index.get("open")),
            high=_positive(index.get("high")),
            low=_positive(index.get("low")),
        )
    else:
        return None
    if source_dt is None:
        return quote, None, False
    age = (now - source_dt).total_seconds()
    fresh = 0 <= age <= float(settings.NSE_MCP_QUOTE_MAX_AGE_SEC)
    return quote, source_dt.isoformat(), fresh


def _quote_dict(
    ltp: float,
    *,
    prev: Optional[float],
    open_px: Optional[float],
    high: Optional[float],
    low: Optional[float],
) -> Dict[str, Any]:
    ohlc: Dict[str, float] = {}
    if open_px is not None:
        ohlc["open"] = open_px
    if high is not None:
        ohlc["high"] = high
    if low is not None:
        ohlc["low"] = low
    if prev is not None:
        ohlc["close"] = prev
    out: Dict[str, Any] = {"last_price": ltp, "ohlc": ohlc}
    if prev is not None:
        out["close_price"] = prev
    if open_px is not None:
        out["open"] = open_px
    if high is not None:
        out["high"] = high
    if low is not None:
        out["low"] = low
    return out


def _bars_from_history(payload: Dict[str, Any]) -> Optional[List[Dict[str, Any]]]:
    rows = payload.get("data")
    if not isinstance(rows, list) or not rows:
        return None
    bars: List[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            return None
        close = _positive(row.get("close"))
        open_px = _positive(row.get("open"))
        high = _positive(row.get("high"))
        low = _positive(row.get("low"))
        day = str(row.get("date") or "")[:10]
        if close is None or open_px is None or high is None or low is None or len(day) != 10:
            return None
        bar: Dict[str, Any] = {
            "timestamp": f"{day}T00:00:00+05:30",
            "open": open_px,
            "high": high,
            "low": low,
            "close": close,
        }
        if "volume" in row and row.get("volume") is not None:
            try:
                bar["volume"] = float(row["volume"])
            except (TypeError, ValueError):
                return None
        bars.append(bar)
    bars.sort(key=lambda c: str(c.get("timestamp") or ""))
    return bars


def _history_covers(bars: List[Dict[str, Any]], end: date, days_back: int) -> bool:
    first = date.fromisoformat(str(bars[0]["timestamp"])[:10])
    last = date.fromisoformat(str(bars[-1]["timestamp"])[:10])
    start_needed = end - timedelta(days=max(int(days_back), 1))
    if first > start_needed + timedelta(days=4):
        return False
    if last > end:
        return False
    if (end - last).days > 4:
        return False
    return True


def _upstox_quote(
    instrument_key: str,
    upstox: Any,
    now: datetime,
    *,
    fallback_reason: str,
) -> MarketDataResult:
    svc = upstox if upstox is not None else _upstox_service()
    try:
        quote = svc.get_market_quote_by_key(instrument_key) if svc is not None else None
    except Exception as exc:
        logger.info("upstox quote failed %s: %s", instrument_key, exc)
        quote = None
    if not isinstance(quote, dict) or _positive(quote.get("last_price")) is None:
        reason = f"{fallback_reason}+upstox_failed" if fallback_reason else "upstox_failed"
        return _unavailable(instrument_key, fallback_reason=reason)
    source_ts = quote.get("timestamp") or quote.get("last_trade_time")
    return MarketDataResult(
        status="ok",
        source="upstox",
        source_timestamp=str(source_ts) if source_ts else None,
        ingest_time=now.isoformat(),
        freshness="unknown" if not source_ts else "fresh",
        fallback_reason=fallback_reason,
        calculation_available=True,
        quote=quote,
        instrument_key=instrument_key,
    )


def _upstox_candles(
    instrument_key: str,
    upstox: Any,
    now: datetime,
    *,
    interval: str,
    days_back: int,
    range_end_date: Optional[date],
    fallback_reason: str,
) -> MarketDataResult:
    svc = upstox if upstox is not None else _upstox_service()
    try:
        candles = (
            svc.get_historical_candles_by_instrument_key(
                instrument_key,
                interval=interval,
                days_back=days_back,
                range_end_date=range_end_date,
            )
            if svc is not None
            else None
        )
    except Exception as exc:
        logger.info("upstox candles failed %s: %s", instrument_key, exc)
        candles = None
    if not candles:
        reason = f"{fallback_reason}+upstox_failed" if fallback_reason else "upstox_failed"
        return _unavailable(instrument_key, fallback_reason=reason)
    last_ts = candles[-1].get("timestamp") if isinstance(candles[-1], dict) else None
    return MarketDataResult(
        status="ok",
        source="upstox",
        source_timestamp=str(last_ts) if last_ts else None,
        ingest_time=now.isoformat(),
        freshness="unknown" if not last_ts else "fresh",
        fallback_reason=fallback_reason,
        calculation_available=True,
        candles=list(candles),
        instrument_key=instrument_key,
    )


def _upstox_service():
    try:
        from backend.config import settings
        from backend.services.upstox_service import UpstoxService

        return UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    except Exception as exc:
        logger.info("upstox service unavailable: %s", exc)
        return None


def _ok_quote(instrument_key: str, quote: dict, source_ts: Optional[str], now: datetime, *, source: str) -> MarketDataResult:
    return MarketDataResult(
        status="ok",
        source=source,
        source_timestamp=source_ts,
        ingest_time=now.isoformat(),
        freshness="fresh",
        fallback_reason=None,
        calculation_available=True,
        quote=quote,
        instrument_key=instrument_key,
    )


def _ok_candles(instrument_key: str, bars: List[dict], now: datetime, *, source: str) -> MarketDataResult:
    last_ts = bars[-1].get("timestamp")
    return MarketDataResult(
        status="ok",
        source=source,
        source_timestamp=str(last_ts) if last_ts else None,
        ingest_time=now.isoformat(),
        freshness="fresh",
        fallback_reason=None,
        calculation_available=True,
        candles=bars,
        instrument_key=instrument_key,
    )


def _unavailable(
    instrument_key: str,
    *,
    fallback_reason: str,
    calculation_available: bool = False,
) -> MarketDataResult:
    return MarketDataResult(
        status="unavailable",
        source="unavailable",
        source_timestamp=None,
        ingest_time=_as_ist(None).isoformat(),
        freshness="unknown",
        fallback_reason=fallback_reason,
        calculation_available=calculation_available,
        quote=None,
        candles=None,
        instrument_key=instrument_key,
    )


def _breaker_allow() -> bool:
    global _breaker_open_until
    with _lock:
        return time.monotonic() >= _breaker_open_until


def _breaker_success() -> None:
    global _breaker_failures
    with _lock:
        _breaker_failures = 0


def _breaker_failure() -> None:
    global _breaker_failures, _breaker_open_until
    with _lock:
        _breaker_failures += 1
        if _breaker_failures >= _BREAKER_FAILURES:
            _breaker_open_until = time.monotonic() + _BREAKER_COOLDOWN_SEC
            _breaker_failures = 0


def _cache_get(key: str, now: datetime, *, quote: bool) -> Optional[MarketDataResult]:
    if not flags_allow_mcp():
        return None
    with _lock:
        entry = _cache.get(key)
    if entry is None or entry.result.source != "nse_mcp":
        return None
    ttl = _QUOTE_CACHE_TTL_SEC if quote else _DAILY_CACHE_TTL_SEC
    if time.monotonic() - entry.stored_at > ttl:
        return None
    if quote:
        source_dt = _parse_source_ts(entry.result.source_timestamp)
        if source_dt is None:
            return None
        from backend.config import settings

        age = (now - source_dt).total_seconds()
        if age > float(settings.NSE_MCP_QUOTE_MAX_AGE_SEC):
            return None
    return entry.result


def _cache_put(key: str, result: MarketDataResult) -> None:
    if result.source != "nse_mcp" or result.status != "ok":
        return
    with _lock:
        if len(_cache) >= _CACHE_MAX:
            oldest = min(_cache.items(), key=lambda kv: kv[1].stored_at)[0]
            _cache.pop(oldest, None)
        _cache[key] = _CacheEntry(stored_at=time.monotonic(), result=result)


def _coalesced(key: str, producer: Callable[[], MarketDataResult]) -> MarketDataResult:
    with _lock:
        slot = _inflight.get(key)
        if slot is None:
            slot = _Slot()
            _inflight[key] = slot
            leader = True
        else:
            leader = False
    if not leader:
        if not slot.event.wait(_COALESCE_WAIT_SEC):
            return _unavailable(key, fallback_reason="coalesce_timeout")
        return slot.result or _unavailable(key, fallback_reason="coalesce_timeout")
    try:
        slot.result = producer()
        return slot.result
    except Exception as exc:
        logger.info("market data gateway %s: %s", key, exc)
        slot.result = _unavailable(key, fallback_reason="gateway_error")
        return slot.result
    finally:
        slot.event.set()
        with _lock:
            if _inflight.get(key) is slot:
                _inflight.pop(key, None)


def _split_key(key: str) -> tuple:
    if "|" not in key:
        return "", key.strip()
    segment, tail = key.split("|", 1)
    return segment.strip().upper(), tail.strip()


def _index_name(key: str) -> Optional[str]:
    segment, tail = _split_key(key)
    if segment and segment != "NSE_INDEX":
        return None
    token = (tail or key).strip().lower()
    return _INDEX_TO_NSE.get(token) or _INDEX_TO_NSE.get(token.replace(" ", ""))


def _equity_symbol(key: str) -> Optional[str]:
    segment, tail = _split_key(key)
    if segment and segment != "NSE_EQ":
        return None
    symbol = (tail or "").strip().upper()
    if not symbol or _ISIN.match(symbol) or _OPTION.search(symbol) or "FUT" in symbol:
        return None
    if not re.fullmatch(r"[A-Z][A-Z0-9&-]{0,24}", symbol):
        return None
    return symbol


def _positive(value: Any) -> Optional[float]:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number <= 0:
        return None
    return number


def _as_ist(now: Optional[datetime]) -> datetime:
    if now is None:
        return datetime.now(IST)
    if now.tzinfo is None:
        return IST.localize(now)
    return now.astimezone(IST)


def _parse_source_ts(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(IST) if value.tzinfo else IST.localize(value)
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
        return parsed.astimezone(IST) if parsed.tzinfo else IST.localize(parsed)
    except ValueError:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%d-%b-%Y %H:%M:%S", "%d-%b-%Y %H:%M"):
        try:
            return IST.localize(datetime.strptime(text, fmt))
        except ValueError:
            continue
    return None

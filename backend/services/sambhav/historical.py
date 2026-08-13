"""Sambhav V1 historical 10-minute candles — Upstox V3, no 1-minute download.

Candle boundary convention (inspected against Upstox V3, 2025-01-01..2025-01-31)
==============================================================================
GET /v3/historical-candle/{instrument_key}/minutes/10/{to_date}/{from_date}

The API timestamp is the candle **open** (start) in Asia/Kolkata:

    09:15, 09:25, 09:35, …, 15:15, 15:25

38 bars per regular NSE session (09:15–15:30 IST). ``candle_end`` is stored as
``candle_start + 10 minutes``. The 15:25 bar is the last regular bar (covers
15:25–15:35; cash session ends 15:30).

We persist the API timestamp as ``candle_start`` and do **not** re-bucket.
The same ``candle_start`` is used for historical import, feature generation,
backtesting, and live prediction.

1-minute data may be added in a future Sambhav V2 feature-enhancement study.
"""

from __future__ import annotations

import logging
import time
from datetime import date, datetime, timedelta
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import quote

import requests

from backend.services.sambhav.candles import in_session, to_ist, validate_ohlc
from backend.services.sambhav.config import (
    HISTORICAL_BACKOFF_CAP_SECONDS,
    HISTORICAL_INTERVAL,
    HISTORICAL_MAX_RETRIES,
    HISTORICAL_REQUEST_DELAY_SECONDS,
    HISTORICAL_REQUEST_TIMEOUT_SECONDS,
    HISTORICAL_SOURCE,
    IMPORT_CHUNK_DAYS,
    INSTRUMENT_KEY,
    IST,
    SESSION_END,
    SESSION_START,
    TF_MINUTES,
)

logger = logging.getLogger(__name__)

SleepFn = Callable[[float], None]
HttpGetFn = Callable[..., Any]


class SambhavAuthError(RuntimeError):
    """Upstox authentication failed — stop the import; do not tight-retry."""


class SambhavFetchError(RuntimeError):
    """Non-retryable historical fetch failure."""


def historical_request_delay_seconds() -> float:
    """Single source of truth for the inter-request delay."""
    return max(0.0, float(HISTORICAL_REQUEST_DELAY_SECONDS))


def chunk_date_range(
    from_date: date,
    to_date: date,
    *,
    chunk_days: int = IMPORT_CHUNK_DAYS,
) -> List[Tuple[date, date]]:
    """Split [from_date, to_date] inclusive into Upstox-safe calendar chunks.

    V3 minutes/10 allows at most ~1 month per request. Never emit a multi-year
    (or otherwise oversize) single chunk.
    """
    if from_date > to_date:
        raise ValueError("from_date must be <= to_date")
    span = max(1, int(chunk_days))
    # Hard cap: V3 minutes 1–15 is 1 month. Never send a larger window.
    span = min(span, 31)
    out: List[Tuple[date, date]] = []
    cursor = from_date
    while cursor <= to_date:
        end = min(cursor + timedelta(days=span - 1), to_date)
        out.append((cursor, end))
        cursor = end + timedelta(days=1)
    return out


def expected_10m_starts(session_date: date) -> List[datetime]:
    """09:15-aligned 10m open timestamps for one NSE regular session."""
    open_dt = IST.localize(datetime.combine(session_date, SESSION_START))
    close_dt = IST.localize(datetime.combine(session_date, SESSION_END))
    out: List[datetime] = []
    t = open_dt
    while t < close_dt:
        out.append(t)
        t += timedelta(minutes=TF_MINUTES)
    return out


def is_expected_10m_boundary(ts: datetime) -> bool:
    """True when ``ts`` is an inspected Upstox 10m open (09:15 + 10k, < 15:30)."""
    ts = to_ist(ts)
    if ts is None or not in_session(ts):
        return False
    if ts.second != 0 or ts.microsecond != 0:
        return False
    open_dt = IST.localize(datetime.combine(ts.date(), SESSION_START))
    delta_min = int((ts - open_dt).total_seconds() // 60)
    return delta_min >= 0 and delta_min % TF_MINUTES == 0


def parse_upstox_v3_candle_row(row: Any) -> Optional[Dict[str, Any]]:
    """Parse one Upstox V2/V3 ``data.candles`` row: [ts, o, h, l, c, vol, oi?]."""
    if isinstance(row, dict):
        ts = to_ist(row.get("timestamp") or row.get("candle_ts") or row.get("ts"))
        if ts is None:
            return None
        rec = {
            "timestamp": ts,
            "open": float(row.get("open") or 0),
            "high": float(row.get("high") or 0),
            "low": float(row.get("low") or 0),
            "close": float(row.get("close") or 0),
            "volume": float(row.get("volume") or 0),
        }
        oi = row.get("oi")
        if oi is None:
            oi = row.get("open_interest")
        if oi is not None:
            try:
                rec["oi"] = float(oi)
            except (TypeError, ValueError):
                rec["oi"] = None
        return rec
    if not isinstance(row, (list, tuple)) or len(row) < 6:
        return None
    ts = to_ist(row[0])
    if ts is None:
        return None
    rec = {
        "timestamp": ts,
        "open": float(row[1]),
        "high": float(row[2]),
        "low": float(row[3]),
        "close": float(row[4]),
        "volume": float(row[5]),
    }
    if len(row) >= 7:
        try:
            rec["oi"] = float(row[6])
        except (TypeError, ValueError):
            rec["oi"] = None
    return rec


def parse_upstox_10m_response(payload: Any) -> List[Dict[str, Any]]:
    """Extract structured 10m candles from a V3 historical JSON body or row list."""
    rows: Any = payload
    if isinstance(payload, dict):
        data = payload.get("data") if isinstance(payload.get("data"), dict) else payload
        rows = (data or {}).get("candles") if isinstance(data, dict) else None
        if rows is None and isinstance(payload.get("candles"), list):
            rows = payload["candles"]
    out: List[Dict[str, Any]] = []
    for row in rows or []:
        parsed = parse_upstox_v3_candle_row(row)
        if parsed is not None:
            out.append(parsed)
    return out


def validate_10m_candle(c: Dict[str, Any]) -> Tuple[bool, str]:
    """Validate timestamp, session, OHLC, volume. Does not aggregate."""
    ts = to_ist(c.get("timestamp") or c.get("candle_start") or c.get("candle_ts"))
    if ts is None:
        return False, "missing_timestamp"
    if not in_session(ts):
        return False, "outside_session"
    if not is_expected_10m_boundary(ts):
        return False, "timestamp_anomaly"
    try:
        o = float(c.get("open"))
        h = float(c.get("high"))
        l = float(c.get("low"))
        cl = float(c.get("close"))
        vol = float(c.get("volume") or 0)
    except (TypeError, ValueError):
        return False, "invalid_numeric"
    if not validate_ohlc(o, h, l, cl):
        return False, "invalid_ohlc"
    if vol < 0:
        return False, "invalid_volume"
    return True, "ok"


def filter_valid_10m_candles(
    candles: Sequence[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """Drop overnight/invalid bars; sort chronologically; count reject reasons.

    Duplicate timestamps are collapsed (first occurrence kept) and counted.
    """
    accepted: List[Dict[str, Any]] = []
    reasons: Dict[str, int] = {}
    seen: set = set()
    for c in candles or []:
        ok, reason = validate_10m_candle(c)
        if not ok:
            reasons[reason] = reasons.get(reason, 0) + 1
            continue
        ts = to_ist(c.get("timestamp") or c.get("candle_start"))
        key = ts.isoformat() if ts else None
        if key in seen:
            reasons["duplicate"] = reasons.get("duplicate", 0) + 1
            continue
        seen.add(key)
        accepted.append(c)
    accepted.sort(key=lambda r: to_ist(r.get("timestamp") or r.get("candle_start")))
    # Chronological check (should already be sorted; flag inversions from source)
    for i in range(1, len(accepted)):
        prev = to_ist(accepted[i - 1].get("timestamp"))
        cur = to_ist(accepted[i].get("timestamp"))
        if prev and cur and cur < prev:
            reasons["order_anomaly"] = reasons.get("order_anomaly", 0) + 1
    return accepted, reasons


def to_10m_row(c: Dict[str, Any], *, source: str = HISTORICAL_SOURCE) -> Dict[str, Any]:
    """Map a parsed API candle onto sambhav_10m_candles fields (no re-aggregation)."""
    start = to_ist(c.get("timestamp") or c.get("candle_start"))
    if start is None:
        raise ValueError("candle missing timestamp")
    oi = c.get("oi")
    if oi is None:
        oi = c.get("open_interest")
    try:
        oi_f = float(oi) if oi is not None else None
    except (TypeError, ValueError):
        oi_f = None
    return {
        "candle_start": start,
        "candle_end": start + timedelta(minutes=TF_MINUTES),
        "open": float(c["open"]),
        "high": float(c["high"]),
        "low": float(c["low"]),
        "close": float(c["close"]),
        "volume": float(c.get("volume") or 0),
        "open_interest": oi_f,
        "source": source,
        "n_1m": 0,
        "is_complete": True,
    }


class HistoricalThrottle:
    """Conservative inter-request delay + exponential backoff. No tight loops."""

    def __init__(
        self,
        *,
        delay_seconds: Optional[float] = None,
        backoff_cap: Optional[float] = None,
        sleep_fn: SleepFn = time.sleep,
    ):
        self.delay_seconds = (
            historical_request_delay_seconds() if delay_seconds is None else max(0.0, float(delay_seconds))
        )
        self.backoff_cap = (
            float(HISTORICAL_BACKOFF_CAP_SECONDS) if backoff_cap is None else max(0.0, float(backoff_cap))
        )
        self._sleep = sleep_fn
        self._last_mono: Optional[float] = None
        self.waits: List[float] = []

    def before_request(self) -> float:
        waited = 0.0
        if self._last_mono is not None and self.delay_seconds > 0:
            elapsed = time.monotonic() - self._last_mono
            remain = self.delay_seconds - elapsed
            if remain > 0:
                self._sleep(remain)
                waited = remain
                self.waits.append(remain)
        self._last_mono = time.monotonic()
        return waited

    def backoff(self, attempt: int, *, status_code: int = 429) -> float:
        """Exponential backoff. ``attempt`` is 0-based. Never a tight retry loop."""
        base = self.delay_seconds if self.delay_seconds > 0 else 2.0
        wait = min(base * (2 ** int(max(0, attempt))), self.backoff_cap)
        wait = max(wait, base)
        if status_code == 429:
            wait = min(max(wait, base * (2 ** int(max(0, attempt)))), self.backoff_cap)
        self._sleep(wait)
        self.waits.append(wait)
        self._last_mono = time.monotonic()
        return wait


def _v3_historical_url(base_url: str, instrument_key: str, to_s: str, from_s: str) -> str:
    key_enc = quote(instrument_key, safe="")
    root = (base_url or "https://api.upstox.com/v3").rstrip("/")
    return f"{root}/historical-candle/{key_enc}/{HISTORICAL_INTERVAL}/{to_s}/{from_s}"


def classify_http_status(status_code: int) -> str:
    if status_code == 200:
        return "ok"
    if status_code in (401, 403):
        return "auth"
    if status_code == 429:
        return "rate_limit"
    if 500 <= status_code <= 599:
        return "server"
    return "error"


def handle_historical_response(
    status_code: int,
    payload: Any,
) -> Tuple[str, List[Dict[str, Any]], str]:
    """Return (kind, candles, message). kind: ok|auth|rate_limit|server|error|empty."""
    kind = classify_http_status(status_code)
    if kind == "ok":
        if isinstance(payload, dict) and payload.get("status") not in (None, "success"):
            return "error", [], str(payload.get("message") or payload.get("status") or "api_error")
        candles = parse_upstox_10m_response(payload)
        return "ok", candles, "ok"
    msg = ""
    if isinstance(payload, dict):
        msg = str(payload.get("message") or payload.get("error") or kind)
    else:
        msg = kind
    return kind, [], msg


def fetch_10m_chunk_with_retry(
    *,
    url: str,
    headers: Dict[str, str],
    throttle: HistoricalThrottle,
    http_get: Optional[HttpGetFn] = None,
    max_retries: int = HISTORICAL_MAX_RETRIES,
    timeout: float = HISTORICAL_REQUEST_TIMEOUT_SECONDS,
    reload_auth: Optional[Callable[[], bool]] = None,
) -> List[Dict[str, Any]]:
    """GET one historical chunk with throttle, 429 backoff, 5xx retry, auth stop."""
    get = http_get or requests.get
    last_err = "unknown"
    retries = max(1, int(max_retries))
    for attempt in range(retries):
        throttle.before_request()
        try:
            resp = get(url, headers=headers, timeout=timeout)
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
            last_err = str(exc)
            if attempt < retries - 1:
                throttle.backoff(attempt, status_code=503)
                continue
            raise SambhavFetchError(f"network error: {exc}") from exc

        status = getattr(resp, "status_code", 0)
        try:
            payload = resp.json() if hasattr(resp, "json") else None
        except ValueError:
            payload = None
        kind, candles, msg = handle_historical_response(status, payload)
        last_err = msg or kind
        if kind == "ok":
            return candles
        if kind == "auth":
            if reload_auth and attempt < retries - 1 and reload_auth():
                logger.warning("sambhav historical auth reload, retrying once")
                continue
            raise SambhavAuthError(f"Upstox authentication failed ({status}): {msg}")
        if kind in ("rate_limit", "server") and attempt < retries - 1:
            throttle.backoff(attempt, status_code=status)
            continue
        raise SambhavFetchError(f"historical fetch failed ({status}): {msg}")
    raise SambhavFetchError(f"historical fetch exhausted retries: {last_err}")


def resolve_nifty_instrument_key(upstox: Any = None) -> str:
    """Prefer instrument master / UpstoxService.NIFTY50_KEY; never hard-code secrets."""
    if upstox is not None:
        key = getattr(upstox, "NIFTY50_KEY", None)
        if key:
            return str(key)
    try:
        from backend.services.symbol_isin_mapping import get_instrument_key

        key = get_instrument_key("NIFTY")
        if key:
            return str(key)
    except Exception:
        logger.debug("sambhav instrument master lookup skipped", exc_info=True)
    return INSTRUMENT_KEY


def fetch_upstox_v3_10m(
    upstox: Any,
    from_d: date,
    to_d: date,
    *,
    instrument_key: Optional[str] = None,
    throttle: Optional[HistoricalThrottle] = None,
    http_get: Optional[HttpGetFn] = None,
) -> List[Dict[str, Any]]:
    """Authenticated V3 10-minute historical fetch for one date chunk."""
    ik = instrument_key or resolve_nifty_instrument_key(upstox)
    from_s = from_d.strftime("%Y-%m-%d")
    to_s = to_d.strftime("%Y-%m-%d")
    base = getattr(upstox, "base_url", "https://api.upstox.com/v3")
    url = _v3_historical_url(base, ik, to_s, from_s)
    headers = upstox.get_headers() if hasattr(upstox, "get_headers") else {}
    thr = throttle or HistoricalThrottle()
    reload_auth = getattr(upstox, "reload_token_from_storage", None)
    logger.info("sambhav V3 10m GET %s .. %s key=%s", from_s, to_s, ik)
    return fetch_10m_chunk_with_retry(
        url=url,
        headers=headers,
        throttle=thr,
        http_get=http_get,
        reload_auth=reload_auth if callable(reload_auth) else None,
    )


def iter_trading_days(
    from_date: date,
    to_date: date,
    *,
    holiday_dates: Optional[Iterable[date]] = None,
) -> List[date]:
    """Weekdays in range minus known NSE holidays. Does not invent sessions."""
    holidays = set(holiday_dates or [])
    out: List[date] = []
    d = from_date
    while d <= to_date:
        if d.weekday() < 5 and d not in holidays:
            out.append(d)
        d += timedelta(days=1)
    return out


def load_nse_holiday_dates(from_date: date, to_date: date) -> set:
    """Holiday dates from DB calendar + known NSE circular fallback."""
    holidays: set = set()
    try:
        from backend.services.market_holiday import refresh_holiday_dates_from_db

        holidays |= {h for h in refresh_holiday_dates_from_db() if from_date <= h <= to_date}
    except Exception:
        logger.debug("sambhav holiday table unavailable", exc_info=True)
    try:
        from backend.services.upstox_service import NSE_KNOWN_HOLIDAYS

        for year in range(from_date.year, to_date.year + 1):
            for s in NSE_KNOWN_HOLIDAYS.get(year, []):
                try:
                    hd = date.fromisoformat(s)
                except ValueError:
                    continue
                if from_date <= hd <= to_date:
                    holidays.add(hd)
    except Exception:
        logger.debug("sambhav NSE_KNOWN_HOLIDAYS unavailable", exc_info=True)
    return holidays

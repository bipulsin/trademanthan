"""Universe, calendar, contract resolution, and candle cache for OpenHighLow."""
from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple
from urllib.parse import quote

from backend.services.open_high_low.bars import Bar, parse_ts
from backend.services.open_high_low.config import EXCLUDE_SYMBOLS, LOT_SIZE_FALLBACK
from backend.services.upstox_service import NSE_KNOWN_HOLIDAYS

logger = logging.getLogger(__name__)

_EXPIRED_CANDLE = (
    "https://api.upstox.com/v2/expired-instruments/historical-candle/"
    "{key}/{interval}/{to_date}/{from_date}"
)
_EXPIRED_FUTURE = "https://api.upstox.com/v2/expired-instruments/future/contract"
_EXPIRED_INTERVAL = {"5m": "5minute", "day": "day"}


def repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def data_dir() -> Path:
    candidates = (
        Path("/home/ubuntu/trademanthan/data/open_high_low"),
        repo_root() / "data" / "open_high_low",
    )
    for path in candidates:
        try:
            if path.is_dir():
                return path
            if path.parent.is_dir():
                path.mkdir(parents=True, exist_ok=True)
                return path
        except OSError:
            continue
    out = candidates[-1]
    out.mkdir(parents=True, exist_ok=True)
    return out


def cache_dir() -> Path:
    p = data_dir() / "cache"
    p.mkdir(parents=True, exist_ok=True)
    return p


def known_holidays() -> Set[date]:
    out: Set[date] = set()
    for days in NSE_KNOWN_HOLIDAYS.values():
        for raw in days:
            out.add(date.fromisoformat(raw))
    return out


def is_trading_day(d: date, holidays: Optional[Set[date]] = None) -> bool:
    hol = holidays if holidays is not None else known_holidays()
    return d.weekday() < 5 and d not in hol


def trading_days(start: date, end: date, holidays: Optional[Set[date]] = None) -> List[date]:
    hol = holidays if holidays is not None else known_holidays()
    out: List[date] = []
    cur = start
    while cur <= end:
        if is_trading_day(cur, hol):
            out.append(cur)
        cur += timedelta(days=1)
    return out


def previous_trading_day(d: date, holidays: Optional[Set[date]] = None) -> date:
    hol = holidays if holidays is not None else known_holidays()
    cur = d - timedelta(days=1)
    for _ in range(15):
        if is_trading_day(cur, hol):
            return cur
        cur -= timedelta(days=1)
    return cur


def monthly_expiry(year: int, month: int, holidays: Optional[Set[date]] = None) -> date:
    """Last Tuesday of the month, walked back over weekends and NSE holidays."""
    import calendar

    hol = holidays if holidays is not None else known_holidays()
    last_day = calendar.monthrange(year, month)[1]
    expiry = date(year, month, last_day)
    while expiry.weekday() != 1:
        expiry -= timedelta(days=1)
    for _ in range(10):
        if expiry.weekday() < 5 and expiry not in hol:
            return expiry
        expiry -= timedelta(days=1)
    return expiry


def front_month_expiry(session: date, holidays: Optional[Set[date]] = None) -> date:
    """Current-month future still ahead including the expiry session itself."""
    hol = holidays if holidays is not None else known_holidays()
    exp = monthly_expiry(session.year, session.month, hol)
    if session <= exp:
        return exp
    y, m = session.year, session.month
    if m == 12:
        y, m = y + 1, 1
    else:
        m += 1
    return monthly_expiry(y, m, hol)


def _parse_fut_symbol_expiry(fut_symbol: str) -> Optional[date]:
    """Parse ``RELIANCE FUT 23 NOV 26`` → date(2026, 11, 23)."""
    import calendar

    parts = str(fut_symbol or "").upper().split()
    # Expect ... FUT DD MON YY
    if len(parts) < 4:
        return None
    try:
        dd = int(parts[-3])
        mon = parts[-2][:3]
        yy = int(parts[-1])
        month = list(calendar.month_abbr).index(mon.title()) if mon.title() in calendar.month_abbr else 0
        if month <= 0:
            # try full / upper abbreviations
            mon_map = {m.upper(): i for i, m in enumerate(calendar.month_abbr) if m}
            month = mon_map.get(mon, 0)
        year = 2000 + yy if yy < 100 else yy
        if month <= 0:
            return None
        return date(year, month, dd)
    except (TypeError, ValueError, IndexError):
        return None


def load_universe(
    exclude: frozenset = EXCLUDE_SYMBOLS,
) -> List[Dict[str, str]]:
    """arbitrage_master symbols excluding index underlyings (stock futures only).

    Includes live current/next-month FO keys (``currmth_*`` / typo ``nextmth_future_instrement_key``)
    so unexpired contracts do not depend on the expired-instruments API.
    """
    from sqlalchemy import text

    from backend.database import SessionLocal

    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT UPPER(TRIM(stock)) AS symbol,
                       MAX(NULLIF(TRIM(stock_instrument_key), '')) AS underlying_key,
                       MAX(NULLIF(TRIM(currmth_future_instrument_key), '')) AS currmth_key,
                       MAX(NULLIF(TRIM(currmth_future_symbol), '')) AS currmth_symbol,
                       MAX(NULLIF(TRIM(nextmth_future_instrement_key), '')) AS nextmth_key,
                       MAX(NULLIF(TRIM(nextmth_future_symbol), '')) AS nextmth_symbol
                FROM arbitrage_master
                WHERE stock IS NOT NULL AND TRIM(stock) <> ''
                GROUP BY UPPER(TRIM(stock))
                ORDER BY 1
                """
            )
        ).fetchall()
    finally:
        db.close()
    out: List[Dict[str, str]] = []
    for row in rows:
        sym = str(row[0] or "").strip().upper()
        if not sym or sym in exclude:
            continue
        out.append(
            {
                "symbol": sym,
                "underlying_key": str(row[1] or "").strip(),
                "currmth_key": str(row[2] or "").strip(),
                "currmth_symbol": str(row[3] or "").strip(),
                "nextmth_key": str(row[4] or "").strip(),
                "nextmth_symbol": str(row[5] or "").strip(),
            }
        )
    return out


def _contract_cache_path() -> Path:
    return cache_dir() / "contracts.json"


def _load_contract_cache() -> Dict[str, Any]:
    path = _contract_cache_path()
    if not path.is_file():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return raw if isinstance(raw, dict) else {}


def _remember_contract(symbol: str, expiry: date, payload: Dict[str, Any]) -> None:
    path = _contract_cache_path()
    cur = _load_contract_cache()
    cur[f"{symbol.upper()}|{expiry.isoformat()}"] = payload
    path.write_text(json.dumps(cur), encoding="utf-8")


def contract_from_response(data: Any, expiry: date) -> Optional[Dict[str, Any]]:
    rows: List[Any] = []
    if isinstance(data, dict):
        body = data.get("data")
        if isinstance(body, list):
            rows = body
        elif isinstance(body, dict):
            rows = [body]
    for row in rows:
        if not isinstance(row, dict):
            continue
        key = str(row.get("instrument_key") or "").strip()
        if key.count("|") < 2:
            continue
        lot = row.get("lot_size") or row.get("minimum_lot")
        try:
            lot_n = int(float(lot or 0))
        except (TypeError, ValueError):
            lot_n = 0
        return {"instrument_key": key, "lot_size": lot_n, "expiry": expiry.isoformat()}
    return None


def _lot_for_key(instrument_key: str, fallback: int = 0) -> int:
    try:
        from backend.services.chart_instrument_resolver import lookup_future_lot_size

        n = lookup_future_lot_size(instrument_key=instrument_key)
        if n:
            return int(n)
    except Exception:
        pass
    return int(fallback or 0)


def _today_ist() -> date:
    from datetime import datetime

    import pytz

    return datetime.now(pytz.timezone("Asia/Kolkata")).date()


def resolve_contract(
    upstox: Any,
    symbol: str,
    expiry: date,
    underlying_key: str,
    *,
    universe_row: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """Resolve FO instrument key + lot for one monthly expiry.

    - Expired expiries (``expiry < today IST``): Upstox expired-instruments API
      (``NSE_FO|token|dd-mm-yyyy``).
    - Live current/next month: ``arbitrage_master.currmth_*`` /
      ``nextmth_future_instrement_key`` when the symbol expiry matches.
    """
    cached = _load_contract_cache().get(f"{symbol.upper()}|{expiry.isoformat()}")
    if isinstance(cached, dict) and cached.get("instrument_key"):
        return cached
    empty = {"instrument_key": "", "lot_size": 0, "expiry": expiry.isoformat()}
    today = _today_ist()
    row = universe_row or {}

    # Live keys from arbitrage_master when expiry is still ahead (incl. today).
    if expiry >= today:
        for key_field, sym_field in (
            ("currmth_key", "currmth_symbol"),
            ("nextmth_key", "nextmth_symbol"),
        ):
            ik = str(row.get(key_field) or "").strip()
            if not ik:
                continue
            parsed = _parse_fut_symbol_expiry(str(row.get(sym_field) or ""))
            if parsed == expiry or (not parsed and key_field == "currmth_key" and expiry == front_month_expiry(today)):
                lot = _lot_for_key(ik)
                found = {"instrument_key": ik, "lot_size": lot, "expiry": expiry.isoformat()}
                _remember_contract(symbol, expiry, found)
                return found
        # Fallback: if only one live key matches front month of today
        cm_key = str(row.get("currmth_key") or "").strip()
        cm_exp = _parse_fut_symbol_expiry(str(row.get("currmth_symbol") or ""))
        if cm_key and cm_exp == expiry:
            found = {
                "instrument_key": cm_key,
                "lot_size": _lot_for_key(cm_key),
                "expiry": expiry.isoformat(),
            }
            _remember_contract(symbol, expiry, found)
            return found

    # Expired path
    underlying = (underlying_key or "").strip()
    if not underlying:
        logger.info("ohl %s has no underlying key; skip %s", symbol, expiry)
        return empty
    try:
        data = upstox.make_api_request(
            _EXPIRED_FUTURE,
            params={"instrument_key": underlying, "expiry_date": expiry.isoformat()},
            timeout=20,
            max_retries=2,
        )
    except Exception as exc:
        logger.info("ohl expired contract %s %s: %s", symbol, expiry, exc)
        return empty
    found = contract_from_response(data, expiry)
    if not found:
        logger.info("ohl no expired future %s %s via %s", symbol, expiry, underlying)
        return empty
    if not found.get("lot_size"):
        found["lot_size"] = _lot_for_key(str(found.get("instrument_key") or ""))
    _remember_contract(symbol, expiry, found)
    return found


# Back-compat alias used by older call sites / tests
def resolve_expired_future(
    upstox: Any,
    symbol: str,
    expiry: date,
    underlying_key: str,
) -> Dict[str, Any]:
    return resolve_contract(upstox, symbol, expiry, underlying_key)


def _payload_rows(data: Any) -> List[Dict[str, Any]]:
    if not isinstance(data, dict):
        return []
    if data.get("status") not in (None, "success"):
        return []
    body = data.get("data")
    candles = None
    if isinstance(body, dict):
        candles = body.get("candles")
    elif isinstance(body, list):
        candles = body
    if not candles:
        return []
    if candles and isinstance(candles[0], dict):
        return list(candles)
    from backend.services.upstox_service import _candles_rows_to_structured

    return _candles_rows_to_structured(candles)


def _cache_file(interval: str, instrument_key: str, start: date, end: date) -> Path:
    safe = instrument_key.replace("|", "_").replace(":", "_").replace("/", "_")
    return cache_dir() / interval / f"{safe}_{start.isoformat()}_{end.isoformat()}.json"


def _rows_to_bars(rows: Sequence[Dict[str, Any]], contract: str) -> List[Bar]:
    out: List[Bar] = []
    for row in rows or []:
        ts = parse_ts(row.get("timestamp"))
        if ts is None:
            continue
        try:
            out.append(
                Bar(
                    ts=ts,
                    open=float(row["open"]),
                    high=float(row["high"]),
                    low=float(row["low"]),
                    close=float(row["close"]),
                    volume=float(row.get("volume") or 0),
                    contract=contract,
                )
            )
        except (KeyError, TypeError, ValueError):
            continue
    out.sort(key=lambda b: b.ts)
    return out


def _dedupe_rows(rows: Sequence[Dict[str, Any]], start: date, end: date) -> List[Dict[str, Any]]:
    dedup: Dict[str, Dict[str, Any]] = {}
    for c in rows:
        ts = parse_ts(c.get("timestamp"))
        if ts is None or not (start <= ts.date() <= end):
            continue
        # Prefer rows that still carry volume when duplicates collide.
        key = ts.isoformat()
        prev = dedup.get(key)
        if prev is None:
            dedup[key] = c
            continue
        try:
            vol_new = float(c.get("volume") or 0)
            vol_old = float(prev.get("volume") or 0)
        except (TypeError, ValueError):
            vol_new, vol_old = 0.0, 0.0
        if vol_new >= vol_old:
            dedup[key] = c
    return [dedup[k] for k in sorted(dedup)]


def _is_thin_payload(rows: Sequence[Dict[str, Any]], *, expect_intraday: bool) -> bool:
    """Refuse to cache empty / obviously incomplete candle payloads."""
    if not rows:
        return True
    if expect_intraday and len(rows) < 3:
        return True
    return False


def _fetch_expired_rows(
    upstox: Any, instrument_key: str, interval: str, start: date, end: date
) -> List[Dict[str, Any]]:
    url = _EXPIRED_CANDLE.format(
        key=quote(instrument_key, safe=""),
        interval=interval,
        to_date=end.isoformat(),
        from_date=start.isoformat(),
    )
    try:
        data = upstox.make_api_request(url, method="GET", timeout=30, max_retries=5)
    except Exception as exc:
        logger.info("ohl expired candles %s: %s", instrument_key, exc)
        return []
    return _payload_rows(data)


def _fetch_regular_rows(
    upstox: Any,
    instrument_key: str,
    start: date,
    end: date,
    *,
    interval: str,
) -> List[Dict[str, Any]]:
    """Live / historical V3 path with range_end_date for minutes/5 and days/1."""
    span = 28 if interval.startswith("minutes") else 90
    rows: List[Dict[str, Any]] = []
    cursor = start
    while cursor <= end:
        chunk_end = min(end, cursor + timedelta(days=span))
        days_back = (chunk_end - cursor).days + 2
        try:
            raw = upstox.get_historical_candles_by_instrument_key(
                instrument_key,
                interval=interval,
                days_back=days_back,
                range_end_date=chunk_end,
            )
        except Exception as exc:
            logger.info("ohl regular candles %s %s..%s: %s", instrument_key, cursor, chunk_end, exc)
            raw = []
        rows.extend(raw or [])
        cursor = chunk_end + timedelta(days=1)
    return _dedupe_rows(rows, start, end)


def _fetch_expired_chunked(
    upstox: Any,
    instrument_key: str,
    start: date,
    end: date,
    *,
    timeframe: str,
) -> List[Dict[str, Any]]:
    interval = _EXPIRED_INTERVAL.get(timeframe, "5minute")
    span = 28 if timeframe == "5m" else 80
    rows: List[Dict[str, Any]] = []
    cursor = start
    while cursor <= end:
        chunk_end = min(end, cursor + timedelta(days=span))
        chunk = _fetch_expired_rows(upstox, instrument_key, interval, cursor, chunk_end)
        rows.extend(chunk)
        cursor = chunk_end + timedelta(days=1)
    return _dedupe_rows(rows, start, end)


def fetch_contract_bars(
    upstox: Any,
    instrument_key: str,
    start: date,
    end: date,
    timeframe: str,
    *,
    use_cache: bool = True,
) -> List[Bar]:
    """5m or daily candles for one FO key. Expired keys use expired-candle API."""
    cache = _cache_file(timeframe, instrument_key, start, end)
    if use_cache and cache.is_file():
        try:
            payload = json.loads(cache.read_text(encoding="utf-8"))
            if isinstance(payload, list) and payload:
                return _rows_to_bars(payload, instrument_key)
        except Exception as exc:
            logger.info("ohl cache read miss %s: %s", cache.name, exc)

    expired_key = instrument_key.count("|") >= 2
    api_interval = "minutes/5" if timeframe == "5m" else "days/1"
    if expired_key:
        ordered = _fetch_expired_chunked(upstox, instrument_key, start, end, timeframe=timeframe)
    else:
        ordered = _fetch_regular_rows(
            upstox, instrument_key, start, end, interval=api_interval
        )
        if not ordered:
            ordered = _fetch_expired_chunked(
                upstox, instrument_key, start, end, timeframe=timeframe
            )

    if _is_thin_payload(ordered, expect_intraday=(timeframe == "5m")):
        logger.info(
            "ohl thin/empty response not cached %s %s %s..%s n=%s",
            timeframe,
            instrument_key,
            start,
            end,
            len(ordered or []),
        )
        return _rows_to_bars(ordered or [], instrument_key)

    if use_cache and ordered:
        try:
            cache.parent.mkdir(parents=True, exist_ok=True)
            cache.write_text(json.dumps(ordered), encoding="utf-8")
        except Exception as exc:
            logger.info("ohl cache write failed: %s", exc)
    return _rows_to_bars(ordered, instrument_key)


def daily_hl_for(
    daily_bars: Sequence[Bar],
    session: date,
    holidays: Optional[Set[date]] = None,
) -> Optional[Tuple[float, float]]:
    """Previous trading day's H/L from the same-contract daily series."""
    prev = previous_trading_day(session, holidays)
    for b in daily_bars:
        if b.session_date == prev:
            return b.high, b.low
    return None


def daily_atr_asof(
    daily_bars: Sequence[Bar],
    session: date,
    *,
    period: int = 14,
    holidays: Optional[Set[date]] = None,
) -> Optional[float]:
    """Daily ATR(period) as of the previous session's close on this contract.

    Uses bars with ``session_date <= previous_trading_day(session)``, sorted
    ascending. Returns None when history is insufficient for Wilder ATR.
    """
    from backend.services.open_high_low.indicators import wilder_atr

    prev = previous_trading_day(session, holidays)
    hist = [b for b in daily_bars if b.session_date <= prev]
    hist.sort(key=lambda b: b.ts)
    if len(hist) < period + 1:
        return None
    return wilder_atr(
        [b.high for b in hist],
        [b.low for b in hist],
        [b.close for b in hist],
        period=period,
    )


def resolve_lot(resolved: Dict[str, Any], fallback: int = LOT_SIZE_FALLBACK) -> int:
    try:
        n = int(resolved.get("lot_size") or 0)
    except (TypeError, ValueError):
        n = 0
    return n if n > 0 else max(1, int(fallback))

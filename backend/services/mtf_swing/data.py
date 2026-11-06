"""Universe, lot size, and Upstox historical futures candles for MTF Swing.

Continuous series are stitched from monthly futures. The engine removes the roll
gap (see bars.back_adjust_to_last / roll_gap) so the roll is not a signal or a fill.
"""
from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence
from urllib.parse import quote

import pytz

from backend.services.mtf_swing.bars import Bar, parse_ts
from backend.services.mtf_swing.config import INTERVAL, WARMUP_DAYS
from backend.services.upstox_service import NSE_KNOWN_HOLIDAYS

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

_MONTHS = ("JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC")
_EXPIRED_CANDLE = (
    "https://api.upstox.com/v2/expired-instruments/historical-candle/"
    "{key}/{interval}/{to_date}/{from_date}"
)
_EXPIRED_FUTURE = "https://api.upstox.com/v2/expired-instruments/future/contract"
_EXPIRED_INTERVAL = {"15m": "15minute", "60m": "30minute"}


def repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def data_dir() -> Path:
    return repo_root() / "data" / "mtf_swing"


def trades_path() -> Path:
    return data_dir() / "trades.csv"


def discards_path() -> Path:
    return data_dir() / "discards.csv"


def progress_path() -> Path:
    return data_dir() / "progress.json"


def state_path(timeframe: str) -> Path:
    return data_dir() / f"state_{timeframe}.json"


def known_holidays() -> set:
    out = set()
    for days in NSE_KNOWN_HOLIDAYS.values():
        for raw in days:
            out.add(date.fromisoformat(raw))
    return out


def load_universe_symbols() -> List[str]:
    """Every symbol in arbitrage_master, including index rows when present."""
    from sqlalchemy import text

    from backend.database import SessionLocal

    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT DISTINCT UPPER(TRIM(stock)) AS symbol
                FROM arbitrage_master
                WHERE stock IS NOT NULL AND TRIM(stock) <> ''
                ORDER BY 1
                """
            )
        ).fetchall()
    finally:
        db.close()
    return [str(r[0]).strip().upper() for r in rows if r and str(r[0] or "").strip()]


def monthly_expiry(year: int, month: int, holidays: set) -> date:
    """Last Tuesday of the month, walked back over weekends and NSE holidays.

    Same rule as UpstoxService.get_monthly_expiry / multi_leg last-Tuesday.
    """
    import calendar

    last_day = calendar.monthrange(year, month)[1]
    expiry = date(year, month, last_day)
    while expiry.weekday() != 1:
        expiry -= timedelta(days=1)
    for _ in range(10):
        if expiry.weekday() < 5 and expiry not in holidays:
            return expiry
        expiry -= timedelta(days=1)
    return expiry


def aggregate_30m_to_60m(bars: Sequence[Bar]) -> List[Bar]:
    """Pair consecutive 30-minute bars into 60-minute bars. The last odd bar is kept."""
    ordered = sorted(bars, key=lambda b: b.ts)
    out: List[Bar] = []
    i = 0
    while i < len(ordered):
        a = ordered[i]
        if i + 1 < len(ordered) and ordered[i + 1].session_date == a.session_date:
            b = ordered[i + 1]
            out.append(
                Bar(
                    ts=a.ts,
                    open=a.open,
                    high=max(a.high, b.high),
                    low=min(a.low, b.low),
                    close=b.close,
                    contract=a.contract,
                )
            )
            i += 2
        else:
            out.append(a)
            i += 1
    return out


def future_instrument_key(symbol: str, expiry: date) -> str:
    mon = _MONTHS[expiry.month - 1]
    return f"NSE_FO|{symbol.upper()}{expiry.strftime('%y')}{mon}FUT"


def contracts_covering(symbol: str, start: date, end: date, holidays: set) -> List[Dict[str, Any]]:
    """Front-month contracts that are active on any session in [start, end]."""
    cursor = date(start.year, start.month, 1) - timedelta(days=40)
    stop = end + timedelta(days=40)
    expiries: List[date] = []
    y, m = cursor.year, cursor.month
    while date(y, m, 1) <= stop:
        expiries.append(monthly_expiry(y, m, holidays))
        if m == 12:
            y, m = y + 1, 1
        else:
            m += 1
    expiries = sorted(set(expiries))
    out = []
    prev_exp: Optional[date] = None
    for exp in expiries:
        active_from = (prev_exp + timedelta(days=1)) if prev_exp else start
        active_to = exp
        if active_to >= start and active_from <= end:
            lo = max(start, active_from)
            hi = min(end, active_to)
            if lo <= hi:
                out.append(
                    {
                        "symbol": symbol.upper(),
                        "expiry": exp.isoformat(),
                        "instrument_key": future_instrument_key(symbol, exp),
                        "active_from": lo.isoformat(),
                        "active_to": hi.isoformat(),
                    }
                )
        prev_exp = exp
    return out


def lot_size_for(symbol: str, instrument_key: str) -> int:
    """1-lot quantity from the instrument master. 0 when the contract is unknown."""
    try:
        from backend.services.chart_instrument_resolver import lookup_future_lot_size
        from backend.services.smart_futures_picker.position_sizing import (
            get_futures_lot_size_by_instrument_key,
        )
    except Exception as exc:
        logger.warning("mtf_swing lot size imports failed: %s", exc)
        return 0
    for key in (instrument_key, instrument_key.replace("|", ":"), instrument_key.replace(":", "|")):
        try:
            n = lookup_future_lot_size(instrument_key=key)
        except Exception:
            n = None
        if n:
            return int(n)
        try:
            n2 = int(get_futures_lot_size_by_instrument_key(key) or 0)
        except Exception:
            n2 = 0
        if n2 > 0:
            return n2
    try:
        from backend.services.stock_option_signals import _fut_contracts_for_symbol

        for row in _fut_contracts_for_symbol(symbol) or []:
            ik = str(row.get("instrument_key") or "")
            lot = row.get("lot_size") or row.get("minimum_lot")
            if lot:
                return int(lot)
            if ik:
                n = lookup_future_lot_size(instrument_key=ik)
                if n:
                    return int(n)
    except Exception as exc:
        logger.info("mtf_swing lot fallback for %s failed: %s", symbol, exc)
    return 0


def _cache_file(timeframe: str, instrument_key: str, start: date, end: date) -> Path:
    safe = instrument_key.replace("|", "_").replace(":", "_").replace("/", "_")
    return data_dir() / "cache" / timeframe / f"{safe}_{start.isoformat()}_{end.isoformat()}.json"


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


def resolve_expired_future(upstox: Any, symbol: str, expiry: date) -> Dict[str, Any]:
    """Instrument key and lot size for one expired monthly future, via UpstoxService."""
    fallback = {
        "instrument_key": future_instrument_key(symbol, expiry),
        "lot_size": 0,
        "expiry": expiry.isoformat(),
    }
    for underlying in (f"NSE_FO|{symbol.upper()}", f"NSE_EQ|{symbol.upper()}"):
        try:
            data = upstox.make_api_request(
                _EXPIRED_FUTURE,
                params={"instrument_key": underlying, "expiry_date": expiry.isoformat()},
                timeout=20,
                max_retries=2,
            )
        except Exception as exc:
            logger.info("mtf_swing expired contract %s %s: %s", symbol, expiry, exc)
            continue
        rows = []
        if isinstance(data, dict):
            body = data.get("data")
            if isinstance(body, list):
                rows = body
            elif isinstance(body, dict):
                rows = [body]
        for row in rows:
            if not isinstance(row, dict):
                continue
            key = str(row.get("instrument_key") or row.get("instrument_token") or "").strip()
            if not key:
                continue
            lot = row.get("lot_size") or row.get("minimum_lot") or row.get("freeze_quantity")
            try:
                lot_n = int(lot or 0)
            except (TypeError, ValueError):
                lot_n = 0
            return {"instrument_key": key, "lot_size": lot_n, "expiry": expiry.isoformat()}
    return fallback


def _fetch_expired_rows(upstox: Any, instrument_key: str, interval: str, start: date, end: date) -> List[Dict[str, Any]]:
    url = _EXPIRED_CANDLE.format(
        key=quote(instrument_key, safe=""),
        interval=interval,
        to_date=end.isoformat(),
        from_date=start.isoformat(),
    )
    try:
        data = upstox.make_api_request(url, method="GET", timeout=30, max_retries=2)
    except Exception as exc:
        logger.info("mtf_swing expired candles %s: %s", instrument_key, exc)
        return []
    return _payload_rows(data)


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
                    contract=contract,
                )
            )
        except (KeyError, TypeError, ValueError):
            continue
    return out


def _dedupe_rows(rows: Sequence[Dict[str, Any]], start: date, end: date) -> List[Dict[str, Any]]:
    dedup: Dict[str, Dict[str, Any]] = {}
    for c in rows:
        ts = parse_ts(c.get("timestamp"))
        if ts is None or not (start <= ts.date() <= end):
            continue
        dedup[ts.isoformat()] = c
    return [dedup[k] for k in sorted(dedup)]


def _fetch_regular_rows(upstox: Any, instrument_key: str, start: date, end: date, timeframe: str) -> List[Dict[str, Any]]:
    interval = INTERVAL.get(timeframe, "minutes/15")
    span = 28 if timeframe == "15m" else 55
    rows: List[Dict[str, Any]] = []
    cursor = start
    while cursor <= end:
        chunk_end = min(end, cursor + timedelta(days=span))
        days_back = (chunk_end - cursor).days + 2
        raw = upstox.get_historical_candles_by_instrument_key(
            instrument_key,
            interval=interval,
            days_back=days_back,
            range_end_date=chunk_end,
        )
        rows.extend(raw or [])
        cursor = chunk_end + timedelta(days=1)
    return _dedupe_rows(rows, start, end)


def _fetch_expired_chunked(upstox: Any, instrument_key: str, start: date, end: date, timeframe: str) -> List[Dict[str, Any]]:
    interval = _EXPIRED_INTERVAL.get(timeframe, "15minute")
    span = 28 if timeframe == "15m" else 80
    rows: List[Dict[str, Any]] = []
    cursor = start
    while cursor <= end:
        chunk_end = min(end, cursor + timedelta(days=span))
        rows.extend(_fetch_expired_rows(upstox, instrument_key, interval, cursor, chunk_end))
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
    """Chunked Upstox candles. Regular historical first, then the expired-instrument API.

    60-minute expired history is 30-minute candles paired into 60-minute bars.
    """
    cache = _cache_file(timeframe, instrument_key, start, end)
    if use_cache and cache.is_file():
        try:
            payload = json.loads(cache.read_text(encoding="utf-8"))
            return _rows_to_bars(payload, instrument_key)
        except Exception as exc:
            logger.info("mtf_swing cache miss %s: %s", cache.name, exc)
    ordered = _fetch_regular_rows(upstox, instrument_key, start, end, timeframe)
    aggregated = False
    if not ordered:
        ordered = _fetch_expired_chunked(upstox, instrument_key, start, end, timeframe)
        aggregated = timeframe == "60m"
    if not ordered:
        return []
    bars = _rows_to_bars(ordered, instrument_key)
    if aggregated:
        bars = aggregate_30m_to_60m(bars)
        ordered = [
            {
                "timestamp": b.ts.isoformat(),
                "open": b.open,
                "high": b.high,
                "low": b.low,
                "close": b.close,
            }
            for b in bars
        ]
    if use_cache and ordered:
        try:
            cache.parent.mkdir(parents=True, exist_ok=True)
            cache.write_text(json.dumps(ordered), encoding="utf-8")
        except Exception as exc:
            logger.info("mtf_swing cache write failed: %s", exc)
    return bars


def stitch_symbol_bars(
    upstox: Any,
    symbol: str,
    start: date,
    end: date,
    timeframe: str,
    holidays: set,
) -> tuple:
    """Raw front-month bars plus lot size by contract key.

    Rolls are not back-adjusted here. The engine shifts working orders when the
    contract changes so the roll gap is not a fill.
    """
    merged: Dict[datetime, Bar] = {}
    lots: Dict[str, int] = {}
    for contract in contracts_covering(symbol, start, end, holidays):
        expiry = date.fromisoformat(contract["expiry"])
        resolved = resolve_expired_future(upstox, symbol, expiry)
        key = str(resolved.get("instrument_key") or contract["instrument_key"])
        lot = int(resolved.get("lot_size") or 0)
        if lot <= 0:
            lot = lot_size_for(symbol, key)
        if lot > 0:
            lots[key] = lot
        c_start = date.fromisoformat(contract["active_from"])
        c_end = date.fromisoformat(contract["active_to"])
        bars = fetch_contract_bars(upstox, key, c_start, c_end, timeframe)
        for b in bars:
            if c_start <= b.session_date <= c_end:
                tagged = b if b.contract == key else Bar(b.ts, b.open, b.high, b.low, b.close, key)
                merged[tagged.ts] = tagged
    return [merged[k] for k in sorted(merged)], lots


def window_for_month(month_start: date, month_end: date, *, tail_days: int = 0) -> tuple:
    start = month_start - timedelta(days=WARMUP_DAYS)
    end = month_end + timedelta(days=tail_days)
    return start, end

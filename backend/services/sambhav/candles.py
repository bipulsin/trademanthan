"""1m → 10m aggregation with NSE 09:15-aligned boundaries (hist/live/backtest identical)."""

from __future__ import annotations

import logging
import math
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from sqlalchemy import text
from sqlalchemy.orm import Session

from backend.services.sambhav.config import (
    EXPECTED_1M_PER_10M,
    INSTRUMENT_KEY,
    IST,
    SESSION_END,
    SESSION_START,
    TF_MINUTES,
)
from backend.services.sambhav.tables import ensure_sambhav_tables

logger = logging.getLogger(__name__)


def to_ist(ts: Any) -> Optional[datetime]:
    """Parse timestamp to timezone-aware IST."""
    if ts is None:
        return None
    if isinstance(ts, datetime):
        if ts.tzinfo is None:
            return IST.localize(ts)
        return ts.astimezone(IST)
    if isinstance(ts, (int, float)):
        v = float(ts)
        if v > 1_000_000_000_000:
            v /= 1000.0
        return datetime.fromtimestamp(v, tz=IST)
    s = str(ts).strip()
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            return IST.localize(dt)
        return dt.astimezone(IST)
    except ValueError:
        return None


def session_open_on(d: date) -> datetime:
    return IST.localize(datetime.combine(d, SESSION_START))


def session_close_on(d: date) -> datetime:
    return IST.localize(datetime.combine(d, SESSION_END))


def in_session(dt: datetime) -> bool:
    dt = to_ist(dt)
    if dt is None:
        return False
    return SESSION_START <= dt.time() < SESSION_END


def candle_start_10m(dt: datetime) -> Optional[datetime]:
    """
    NSE 09:15-aligned 10m bucket start for a 1m bar timestamp.

    09:15–09:24 → 09:15; 09:25–09:34 → 09:25; … ; 15:25–15:29 → 15:25.
    Returns None if outside session.
    """
    dt = to_ist(dt)
    if dt is None or not in_session(dt):
        return None
    open_dt = session_open_on(dt.date())
    minutes_from_open = int((dt - open_dt).total_seconds() // 60)
    if minutes_from_open < 0:
        return None
    bucket = minutes_from_open // TF_MINUTES
    start = open_dt + timedelta(minutes=bucket * TF_MINUTES)
    # Drop buckets whose start is at/after session end
    if start.time() >= SESSION_END:
        return None
    return start


def validate_ohlc(o: float, h: float, l: float, c: float) -> bool:
    if any(x is None or (isinstance(x, float) and (math.isnan(x) or math.isinf(x))) for x in (o, h, l, c)):
        return False
    if o <= 0 or h <= 0 or l <= 0 or c <= 0:
        return False
    if h < max(o, c) or l > min(o, c) or h < l:
        return False
    return True


def aggregate_1m_to_10m(
    candles_1m: Sequence[Dict[str, Any]],
    *,
    require_complete: bool = True,
) -> List[Dict[str, Any]]:
    """
    Deterministic 1m → 10m aggregation (09:15 aligned).

    Each output row:
      candle_start, candle_end, open, high, low, close, volume, n_1m, is_complete
    """
    buckets: Dict[datetime, List[Tuple[datetime, Dict[str, Any]]]] = {}
    for c in candles_1m or []:
        dt = to_ist(c.get("timestamp") or c.get("candle_ts") or c.get("ts"))
        if dt is None:
            continue
        start = candle_start_10m(dt)
        if start is None:
            continue
        o = float(c.get("open") or 0)
        h = float(c.get("high") or 0)
        l = float(c.get("low") or 0)
        cl = float(c.get("close") or 0)
        if not validate_ohlc(o, h, l, cl):
            continue
        buckets.setdefault(start, []).append((dt, c))

    out: List[Dict[str, Any]] = []
    for start in sorted(buckets.keys()):
        rows = sorted(buckets[start], key=lambda x: x[0])
        first, last = rows[0][1], rows[-1][1]
        highs = [float(r[1].get("high") or 0) for r in rows]
        lows = [float(r[1].get("low") or 0) for r in rows]
        vols = [float(r[1].get("volume") or 0) for r in rows]
        n = len(rows)
        complete = n >= EXPECTED_1M_PER_10M
        if require_complete and not complete:
            continue
        end = start + timedelta(minutes=TF_MINUTES)
        out.append(
            {
                "candle_start": start,
                "candle_end": end,
                "open": float(first.get("open") or 0),
                "high": max(highs) if highs else 0.0,
                "low": min(lows) if lows else 0.0,
                "close": float(last.get("close") or 0),
                "volume": float(sum(vols)),
                "n_1m": n,
                "is_complete": complete,
            }
        )
    return out


def upsert_10m_candles(
    db: Session,
    rows: Iterable[Dict[str, Any]],
    instrument_key: str = INSTRUMENT_KEY,
) -> int:
    ensure_sambhav_tables()
    n = 0
    sql = text(
        """
        INSERT INTO sambhav_10m_candles (
            instrument_key, candle_start, candle_end,
            open, high, low, close, volume, n_1m, is_complete
        ) VALUES (
            :ik, :cs, :ce, :o, :h, :l, :c, :v, :n1, :comp
        )
        ON CONFLICT (instrument_key, candle_start) DO UPDATE SET
            candle_end = EXCLUDED.candle_end,
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            n_1m = EXCLUDED.n_1m,
            is_complete = EXCLUDED.is_complete
        """
    )
    for r in rows:
        db.execute(
            sql,
            {
                "ik": instrument_key,
                "cs": r["candle_start"],
                "ce": r["candle_end"],
                "o": r["open"],
                "h": r["high"],
                "l": r["low"],
                "c": r["close"],
                "v": r["volume"],
                "n1": int(r.get("n_1m") or 0),
                "comp": bool(r.get("is_complete")),
            },
        )
        n += 1
    db.commit()
    return n


def build_10m_candles(
    db: Session,
    *,
    instrument_key: str = INSTRUMENT_KEY,
    from_ts: Optional[datetime] = None,
    to_ts: Optional[datetime] = None,
    require_complete: bool = True,
) -> Dict[str, Any]:
    """Load 1m from DB, aggregate, upsert 10m. Returns summary counts."""
    ensure_sambhav_tables()
    params: Dict[str, Any] = {"ik": instrument_key}
    clauses = ["instrument_key = :ik"]
    if from_ts is not None:
        clauses.append("candle_ts >= :from_ts")
        params["from_ts"] = to_ist(from_ts)
    if to_ts is not None:
        clauses.append("candle_ts < :to_ts")
        params["to_ts"] = to_ist(to_ts)
    where = " AND ".join(clauses)
    rows = db.execute(
        text(
            f"""
            SELECT candle_ts, open, high, low, close, volume
            FROM sambhav_raw_candles
            WHERE {where}
            ORDER BY candle_ts ASC
            """
        ),
        params,
    ).fetchall()
    candles = [
        {
            "timestamp": r.candle_ts,
            "open": float(r.open),
            "high": float(r.high),
            "low": float(r.low),
            "close": float(r.close),
            "volume": float(r.volume or 0),
        }
        for r in rows
    ]
    agg = aggregate_1m_to_10m(candles, require_complete=require_complete)
    written = upsert_10m_candles(db, agg, instrument_key=instrument_key)
    return {
        "raw_1m": len(candles),
        "agg_10m": len(agg),
        "upserted": written,
        "require_complete": require_complete,
    }


def load_10m_df_rows(
    db: Session,
    *,
    instrument_key: str = INSTRUMENT_KEY,
    from_ts: Optional[datetime] = None,
    to_ts: Optional[datetime] = None,
    complete_only: bool = True,
) -> List[Dict[str, Any]]:
    ensure_sambhav_tables()
    params: Dict[str, Any] = {"ik": instrument_key}
    clauses = ["instrument_key = :ik"]
    if complete_only:
        clauses.append("is_complete = TRUE")
    if from_ts is not None:
        clauses.append("candle_start >= :from_ts")
        params["from_ts"] = to_ist(from_ts)
    if to_ts is not None:
        clauses.append("candle_start <= :to_ts")
        params["to_ts"] = to_ist(to_ts)
    where = " AND ".join(clauses)
    rows = db.execute(
        text(
            f"""
            SELECT candle_start, candle_end, open, high, low, close, volume, n_1m, is_complete
            FROM sambhav_10m_candles
            WHERE {where}
            ORDER BY candle_start ASC
            """
        ),
        params,
    ).fetchall()
    return [
        {
            "candle_start": to_ist(r.candle_start),
            "candle_end": to_ist(r.candle_end),
            "open": float(r.open),
            "high": float(r.high),
            "low": float(r.low),
            "close": float(r.close),
            "volume": float(r.volume or 0),
            "n_1m": int(r.n_1m or 0),
            "is_complete": bool(r.is_complete),
        }
        for r in rows
    ]

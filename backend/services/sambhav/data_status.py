"""Sambhav V1 dataset status — 10-minute candles only (no 1-minute metrics)."""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from backend.services.sambhav.candles import to_ist, validate_ohlc
from backend.services.sambhav.config import (
    EXPECTED_10M_PER_SESSION,
    INSTRUMENT_DISPLAY,
    INSTRUMENT_KEY,
    IST,
)
from backend.services.sambhav.historical import (
    expected_10m_starts,
    is_expected_10m_boundary,
    iter_trading_days,
    load_nse_holiday_dates,
)
from backend.services.sambhav.importer import get_import_state
from backend.services.sambhav.tables import ensure_sambhav_tables

logger = logging.getLogger(__name__)


def _as_date(v: Any) -> Optional[date]:
    if v is None:
        return None
    if isinstance(v, datetime):
        ts = to_ist(v)
        return ts.date() if ts else v.date()
    if isinstance(v, date):
        return v
    try:
        return date.fromisoformat(str(v)[:10])
    except ValueError:
        return None


def compute_data_status(
    db: Session,
    *,
    instrument_key: str = INSTRUMENT_KEY,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> Dict[str, Any]:
    """Quality report for native 10m candles. Does not fabricate or forward-fill."""
    ensure_sambhav_tables()
    params: Dict[str, Any] = {"ik": instrument_key}
    clauses = ["instrument_key = :ik"]
    if start_date is not None:
        clauses.append("candle_start >= :from_ts")
        params["from_ts"] = IST.localize(datetime.combine(start_date, datetime.min.time()))
    if end_date is not None:
        clauses.append("candle_start < :to_ts")
        params["to_ts"] = IST.localize(datetime.combine(end_date, datetime.min.time())).replace(
            hour=23, minute=59, second=59
        )
    where = " AND ".join(clauses)
    rows = db.execute(
        text(
            f"""
            SELECT candle_start, open, high, low, close, volume
            FROM sambhav_10m_candles
            WHERE {where}
            ORDER BY candle_start ASC
            """
        ),
        params,
    ).fetchall()

    import_state = get_import_state(db, instrument_key)
    import_st = (import_state.get("status") or "").lower()

    if not rows:
        status = "IMPORTING" if import_st in ("running", "importing") else "NOT_IMPORTED"
        rng_start = start_date.isoformat() if start_date else None
        rng_end = end_date.isoformat() if end_date else None
        return {
            "instrument": INSTRUMENT_DISPLAY,
            "instrument_key": instrument_key,
            "interval": "10m",
            "start_date": rng_start,
            "end_date": rng_end,
            "candle_count": 0,
            "10m_candle_count": 0,
            "trading_days": 0,
            "missing_candles": 0,
            "missing_10m_candles": 0,
            "duplicates": 0,
            "invalid_ohlc": 0,
            "timestamp_anomalies": 0,
            "missing_sessions": [],
            "incomplete_sessions": [],
            "status": status,
            "phase": "DATA COLLECTION",
            "import_state": import_state,
        }

    starts: List[datetime] = []
    invalid_ohlc = 0
    anomalies = 0
    seen: Dict[str, int] = {}
    by_day: Dict[date, List[datetime]] = {}
    for r in rows:
        ts = to_ist(r.candle_start)
        if ts is None:
            anomalies += 1
            continue
        key = ts.isoformat()
        seen[key] = seen.get(key, 0) + 1
        starts.append(ts)
        by_day.setdefault(ts.date(), []).append(ts)
        if not is_expected_10m_boundary(ts):
            anomalies += 1
        if not validate_ohlc(float(r.open), float(r.high), float(r.low), float(r.close)):
            invalid_ohlc += 1

    duplicates = sum(n - 1 for n in seen.values() if n > 1)
    first_d = _as_date(starts[0]) if starts else start_date
    last_d = _as_date(starts[-1]) if starts else end_date
    window_from = start_date or first_d
    window_to = end_date or last_d
    holidays = load_nse_holiday_dates(window_from, window_to) if window_from and window_to else set()
    expected_days = iter_trading_days(window_from, window_to, holiday_dates=holidays) if window_from and window_to else []

    missing_sessions: List[str] = []
    incomplete_sessions: List[Dict[str, Any]] = []
    missing_candles = 0
    for d in expected_days:
        actual = by_day.get(d, [])
        expected = expected_10m_starts(d)
        have = {t.isoformat() for t in actual}
        miss = [t for t in expected if t.isoformat() not in have]
        if not actual:
            missing_sessions.append(d.isoformat())
            missing_candles += len(expected)
        elif miss:
            incomplete_sessions.append(
                {
                    "date": d.isoformat(),
                    "actual": len(actual),
                    "expected": len(expected),
                    "missing": len(miss),
                }
            )
            missing_candles += len(miss)

    if import_st in ("running", "importing"):
        status = "IMPORTING"
    elif invalid_ohlc > 0 or duplicates > 0 or anomalies > 0:
        status = "FAIL"
    elif missing_sessions or incomplete_sessions:
        status = "WARNING"
    else:
        status = "PASS"

    return {
        "instrument": INSTRUMENT_DISPLAY,
        "instrument_key": instrument_key,
        "interval": "10m",
        "start_date": first_d.isoformat() if first_d else None,
        "end_date": last_d.isoformat() if last_d else None,
        "requested_start_date": start_date.isoformat() if start_date else None,
        "requested_end_date": end_date.isoformat() if end_date else None,
        "candle_count": len(rows),
        "10m_candle_count": len(rows),
        "trading_days": len(by_day),
        "expected_trading_days": len(expected_days),
        "expected_10m_per_session": EXPECTED_10M_PER_SESSION,
        "missing_candles": missing_candles,
        "missing_10m_candles": missing_candles,
        "duplicates": duplicates,
        "invalid_ohlc": invalid_ohlc,
        "timestamp_anomalies": anomalies,
        "missing_sessions": missing_sessions,
        "incomplete_sessions": incomplete_sessions,
        "status": status,
        "phase": "DATA COLLECTION",
        "import_state": import_state,
    }


def calibration_status_payload(
    *,
    buckets: Any,
    metrics: Any = None,
    model_id: Any = None,
    created_at: Any = None,
) -> Dict[str, Any]:
    """Honest calibration status: n=0 / missing ECE → INSUFFICIENT DATA, never OK."""
    n = 0
    ece = None
    bucket_status = None
    if isinstance(buckets, dict):
        try:
            n = int(buckets.get("n") or 0)
        except (TypeError, ValueError):
            n = 0
        ece = buckets.get("ece")
        bucket_status = buckets.get("status")
    if n <= 0:
        return {
            "status": "INSUFFICIENT DATA",
            "ece": None,
            "n": 0,
            "model_id": model_id,
            "created_at": created_at,
            "calibration_buckets": {"status": "INSUFFICIENT DATA", "n": 0, "ece": None, "buckets": []},
            "metrics": metrics,
            "note": "No calibration observations. Status is INSUFFICIENT DATA, not OK.",
        }
    status = bucket_status or "OK"
    if ece is None:
        status = "INSUFFICIENT DATA"
    elif isinstance(ece, (int, float)) and float(ece) > 0.15:
        status = "CALIBRATION POOR"
    return {
        "status": status,
        "ece": ece,
        "n": n,
        "model_id": model_id,
        "created_at": created_at,
        "calibration_buckets": buckets,
        "metrics": metrics,
        "note": "If ECE is high, UI should show CALIBRATION POOR.",
    }

"""Sambhav V1 dataset status — REGULAR sessions only (holidays/special excluded)."""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from backend.services.sambhav.candles import to_ist, validate_ohlc
from backend.services.sambhav.config import (
    DATASET_VERSION_V1,
    EXPECTED_10M_PER_SESSION,
    INSTRUMENT_DISPLAY,
    INSTRUMENT_KEY,
    SESSION_TYPE_EXCLUDED_HOLIDAY,
    SESSION_TYPE_EXCLUDED_MUHURAT,
    SESSION_TYPE_EXCLUDED_SPECIAL,
    SESSION_TYPE_REGULAR,
    SESSION_TYPE_UNKNOWN,
)
from backend.services.sambhav.dataset import get_active_dataset_version
from backend.services.sambhav.historical import is_expected_10m_boundary
from backend.services.sambhav.importer import get_import_state
from backend.services.sambhav.sessions import (
    classify_and_persist_sessions,
    validate_regular_session_bars,
)
from backend.services.sambhav.tables import ensure_sambhav_tables

logger = logging.getLogger(__name__)


def compute_data_status(
    db: Session,
    *,
    instrument_key: str = INSTRUMENT_KEY,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    refresh_sessions: bool = True,
) -> Dict[str, Any]:
    """
    Sambhav V1 quality report.

    Holidays, muhurat and special sessions are EXCLUDED — not counted as missing.
    Only REGULAR sessions are judged for PASS/FAIL.
    """
    ensure_sambhav_tables()
    import_state = get_import_state(db, instrument_key)
    import_st = (import_state.get("status") or "").lower()

    if refresh_sessions:
        classify_and_persist_sessions(
            db,
            instrument_key=instrument_key,
            start_date=start_date,
            end_date=end_date,
        )

    total_n = db.execute(
        text("SELECT COUNT(*) FROM sambhav_10m_candles WHERE instrument_key = :ik"),
        {"ik": instrument_key},
    ).scalar()
    if not total_n:
        status = "IMPORTING" if import_st in ("running", "importing") else "NOT_IMPORTED"
        return {
            "instrument": INSTRUMENT_DISPLAY,
            "instrument_key": instrument_key,
            "interval": "10m",
            "dataset_label": "SAMBHAV V1 DATASET",
            "status": status,
            "data_integrity": status,
            "phase": "DATA COLLECTION",
            "candle_count": 0,
            "regular_session_count": 0,
            "regular_candle_count": 0,
            "missing_candles": 0,
            "regular_missing_candles": 0,
            "duplicates": 0,
            "invalid_ohlc": 0,
            "timestamp_anomalies": 0,
            "excluded_sessions": [],
            "excluded_holiday_count": 0,
            "note": (
                "Special sessions, Muhurat sessions and NSE holidays are intentionally "
                "excluded from Sambhav V1 analysis."
            ),
            "import_state": import_state,
            "dataset_version": None,
        }

    # Session tallies
    type_rows = db.execute(
        text(
            """
            SELECT session_type, COUNT(*) AS n, COALESCE(SUM(candle_count),0) AS bars
            FROM sambhav_sessions
            WHERE instrument_key = :ik
            GROUP BY session_type
            """
        ),
        {"ik": instrument_key},
    ).mappings().all()
    by_type = {r["session_type"]: {"sessions": int(r["n"]), "bars": int(r["bars"])} for r in type_rows}

    regular_dates = [
        r.session_date
        for r in db.execute(
            text(
                """
                SELECT session_date FROM sambhav_sessions
                WHERE instrument_key = :ik AND session_type = :st AND included_in_sambhav_v1 = TRUE
                ORDER BY session_date
                """
            ),
            {"ik": instrument_key, "st": SESSION_TYPE_REGULAR},
        ).fetchall()
    ]

    # Load candles only for REGULAR days (V1 scope)
    regular_missing = 0
    incomplete_regular: List[Dict[str, Any]] = []
    invalid_ohlc = 0
    anomalies = 0
    seen: Dict[str, int] = {}
    regular_candle_count = 0
    first_d: Optional[date] = None
    last_d: Optional[date] = None

    for d in regular_dates:
        rows = db.execute(
            text(
                """
                SELECT candle_start, open, high, low, close
                FROM sambhav_10m_candles
                WHERE instrument_key = :ik
                  AND (candle_start AT TIME ZONE 'Asia/Kolkata')::date = :d
                ORDER BY candle_start
                """
            ),
            {"ik": instrument_key, "d": d},
        ).fetchall()
        times = []
        ohlc = []
        for r in rows:
            ts = to_ist(r.candle_start)
            if ts is None:
                anomalies += 1
                continue
            key = ts.isoformat()
            seen[key] = seen.get(key, 0) + 1
            times.append(ts)
            ohlc.append((r.open, r.high, r.low, r.close))
            if not is_expected_10m_boundary(ts):
                anomalies += 1
            if not validate_ohlc(float(r.open), float(r.high), float(r.low), float(r.close)):
                invalid_ohlc += 1
        check = validate_regular_session_bars(d, times, ohlc)
        regular_candle_count += len(times)
        if first_d is None:
            first_d = d
        last_d = d
        if not check["ok"]:
            incomplete_regular.append(check)
            regular_missing += int(check["missing"])

    duplicates = sum(n - 1 for n in seen.values() if n > 1)

    excluded_sessions = []
    for st in (
        SESSION_TYPE_EXCLUDED_SPECIAL,
        SESSION_TYPE_EXCLUDED_MUHURAT,
        SESSION_TYPE_UNKNOWN,
    ):
        rows = db.execute(
            text(
                """
                SELECT session_date, session_type, candle_count, notes
                FROM sambhav_sessions
                WHERE instrument_key = :ik AND session_type = :st
                ORDER BY session_date
                """
            ),
            {"ik": instrument_key, "st": st},
        ).mappings().all()
        for r in rows:
            excluded_sessions.append(
                {
                    "date": str(r["session_date"]),
                    "session_type": r["session_type"],
                    "candle_count": int(r["candle_count"]),
                    "notes": r["notes"],
                }
            )

    holiday_n = int(by_type.get(SESSION_TYPE_EXCLUDED_HOLIDAY, {}).get("sessions", 0))
    unknown_n = int(by_type.get(SESSION_TYPE_UNKNOWN, {}).get("sessions", 0))
    regular_n = len(regular_dates)
    expected_regular_candles = regular_n * EXPECTED_10M_PER_SESSION

    rng = db.execute(
        text(
            """
            SELECT MIN((candle_start AT TIME ZONE 'Asia/Kolkata')::date),
                   MAX((candle_start AT TIME ZONE 'Asia/Kolkata')::date)
            FROM sambhav_10m_candles WHERE instrument_key = :ik
            """
        ),
        {"ik": instrument_key},
    ).fetchone()
    period_start = str(rng[0]) if rng and rng[0] else (first_d.isoformat() if first_d else None)
    period_end = str(rng[1]) if rng and rng[1] else (last_d.isoformat() if last_d else None)

    if import_st in ("running", "importing"):
        status = "IMPORTING"
    elif invalid_ohlc > 0 or duplicates > 0 or anomalies > 0 or incomplete_regular or unknown_n > 0:
        status = "FAIL" if (invalid_ohlc or duplicates or anomalies or incomplete_regular) else "WARNING"
    else:
        status = "PASS"

    active = get_active_dataset_version(db)
    dataset_version = (active or {}).get("dataset_version") or DATASET_VERSION_V1

    return {
        "instrument": INSTRUMENT_DISPLAY,
        "instrument_key": instrument_key,
        "interval": "10m",
        "source": "Upstox V3",
        "dataset_label": "SAMBHAV V1 DATASET",
        "dataset_version": dataset_version,
        "start_date": period_start,
        "end_date": period_end,
        "period": f"{period_start} → {period_end}" if period_start and period_end else None,
        "candle_count": int(total_n or 0),
        "total_candle_count": int(total_n or 0),
        "trading_days": regular_n + len(excluded_sessions),
        "regular_session_count": regular_n,
        "regular_trading_sessions": regular_n,
        "expected_regular_candles": expected_regular_candles,
        "regular_candle_count": regular_candle_count,
        "regular_candles": regular_candle_count,
        "expected_10m_per_session": EXPECTED_10M_PER_SESSION,
        # V1 primary metrics — holidays/special NOT counted as missing
        "missing_candles": regular_missing,
        "regular_missing_candles": regular_missing,
        "missing_10m_candles": regular_missing,
        "duplicates": duplicates,
        "invalid_ohlc": invalid_ohlc,
        "timestamp_anomalies": anomalies,
        "incomplete_regular_sessions": incomplete_regular,
        "incomplete_sessions": incomplete_regular,
        "missing_sessions": [],  # holidays are excluded, not missing
        "excluded_sessions": excluded_sessions,
        "excluded_session_count": len(excluded_sessions),
        "excluded_holiday_count": holiday_n,
        "session_type_counts": {k: v["sessions"] for k, v in by_type.items()},
        "status": status,
        "data_integrity": status,
        "phase": "DATA COLLECTION",
        "model_status": "MODEL NOT VALIDATED",
        "note": (
            "Special sessions, Muhurat sessions and NSE holidays are intentionally "
            "excluded from Sambhav V1 analysis."
        ),
        "import_state": import_state,
        "active_dataset": active,
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

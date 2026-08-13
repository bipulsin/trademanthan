"""Sambhav session classification — REGULAR vs excluded (holidays/muhurat/special).

Raw candles in sambhav_10m_candles are never deleted. Classification only
controls what enters the Sambhav V1 ML dataset.
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from sqlalchemy import text
from sqlalchemy.orm import Session

from backend.services.sambhav.candles import to_ist, validate_ohlc
from backend.services.sambhav.config import (
    EXPECTED_10M_PER_SESSION,
    INSTRUMENT_KEY,
    SESSION_TYPE_EXCLUDED_HOLIDAY,
    SESSION_TYPE_EXCLUDED_MUHURAT,
    SESSION_TYPE_EXCLUDED_SPECIAL,
    SESSION_TYPE_REGULAR,
    SESSION_TYPE_UNKNOWN,
    V1_EXCLUDED_MUHURAT_DATES,
    V1_EXCLUDED_SPECIAL_DATES,
)
from backend.services.sambhav.historical import (
    expected_10m_starts,
    is_expected_10m_boundary,
    load_nse_holiday_dates,
)
from backend.services.sambhav.holidays import sambhav_holiday_dates
from backend.services.sambhav.tables import ensure_sambhav_tables

logger = logging.getLogger(__name__)


def included_in_sambhav_v1(session_type: str) -> bool:
    return session_type == SESSION_TYPE_REGULAR


def classify_session_type(
    session_date: date,
    *,
    candle_count: int = 0,
    grid_ok: bool = False,
    holiday_dates: Optional[Set[date]] = None,
) -> Tuple[str, str]:
    """Return (session_type, notes). Does not invent candles."""
    holidays = holiday_dates or set()
    iso = session_date.isoformat()

    if session_date in V1_EXCLUDED_MUHURAT_DATES:
        return SESSION_TYPE_EXCLUDED_MUHURAT, "Diwali muhurat / special short session"
    if session_date in V1_EXCLUDED_SPECIAL_DATES:
        return SESSION_TYPE_EXCLUDED_SPECIAL, "Non-regular special session (preserved, excluded from V1)"
    if session_date in holidays:
        return SESSION_TYPE_EXCLUDED_HOLIDAY, "NSE holiday — excluded from V1 (no missing-candle)"

    if session_date.weekday() >= 5:
        if candle_count > 0:
            return SESSION_TYPE_EXCLUDED_SPECIAL, "Weekend bars present — excluded from V1"
        return SESSION_TYPE_EXCLUDED_SPECIAL, "Weekend — not a regular session"

    if candle_count == EXPECTED_10M_PER_SESSION and grid_ok:
        return SESSION_TYPE_REGULAR, "Regular NSE session — included in Sambhav V1"
    if candle_count == 0:
        return SESSION_TYPE_UNKNOWN, "Weekday with no candles — review before use"
    return SESSION_TYPE_UNKNOWN, f"Weekday with {candle_count} bars (expected {EXPECTED_10M_PER_SESSION}) — review"


def validate_regular_session_bars(
    session_date: date,
    timestamps: Sequence[datetime],
    ohlc_rows: Optional[Sequence[Tuple[float, float, float, float]]] = None,
) -> Dict[str, Any]:
    """Validate one candidate regular session. Never fabricates bars."""
    expected = expected_10m_starts(session_date)
    expected_keys = {t.isoformat() for t in expected}
    have = sorted((to_ist(t) for t in timestamps if to_ist(t) is not None), key=lambda x: x)
    have_keys = {t.isoformat() for t in have}
    missing = [t for t in expected if t.isoformat() not in have_keys]
    extras = [t for t in have if t.isoformat() not in expected_keys]
    ordered = all(have[i] <= have[i + 1] for i in range(len(have) - 1)) if have else True
    boundaries_ok = all(is_expected_10m_boundary(t) for t in have)
    grid_ok = (
        len(have) == EXPECTED_10M_PER_SESSION
        and not missing
        and not extras
        and ordered
        and boundaries_ok
        and bool(have)
        and have[0].strftime("%H:%M") == "09:15"
        and have[-1].strftime("%H:%M") == "15:25"
    )
    invalid_ohlc = 0
    if ohlc_rows:
        for o, h, l, c in ohlc_rows:
            if not validate_ohlc(float(o), float(h), float(l), float(c)):
                invalid_ohlc += 1
    return {
        "date": session_date.isoformat(),
        "actual": len(have),
        "expected": EXPECTED_10M_PER_SESSION,
        "missing": len(missing),
        "extras": len(extras),
        "ordered": ordered,
        "grid_ok": grid_ok,
        "invalid_ohlc": invalid_ohlc,
        "ok": grid_ok and invalid_ohlc == 0,
    }


def _day_candle_map(
    db: Session,
    instrument_key: str,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> Dict[date, List[Any]]:
    params: Dict[str, Any] = {"ik": instrument_key}
    clauses = ["instrument_key = :ik"]
    if start_date is not None:
        clauses.append("(candle_start AT TIME ZONE 'Asia/Kolkata')::date >= :sd")
        params["sd"] = start_date
    if end_date is not None:
        clauses.append("(candle_start AT TIME ZONE 'Asia/Kolkata')::date <= :ed")
        params["ed"] = end_date
    where = " AND ".join(clauses)
    rows = db.execute(
        text(
            f"""
            SELECT candle_start, open, high, low, close
            FROM sambhav_10m_candles
            WHERE {where}
            ORDER BY candle_start ASC
            """
        ),
        params,
    ).fetchall()
    by_day: Dict[date, List[Any]] = {}
    for r in rows:
        ts = to_ist(r.candle_start)
        if ts is None:
            continue
        by_day.setdefault(ts.date(), []).append(r)
    return by_day


def classify_and_persist_sessions(
    db: Session,
    *,
    instrument_key: str = INSTRUMENT_KEY,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> Dict[str, Any]:
    """Classify all sessions in range and upsert into sambhav_sessions. Never deletes candles."""
    ensure_sambhav_tables()
    by_day = _day_candle_map(db, instrument_key, start_date, end_date)
    if not by_day and start_date is None:
        return {"ok": True, "sessions_written": 0, "by_type": {}}

    first = start_date or min(by_day)
    last = end_date or max(by_day)
    holidays = load_nse_holiday_dates(first, last) | sambhav_holiday_dates(first, last)

    records: List[Dict[str, Any]] = []
    # Dates with candles
    for d, rows in sorted(by_day.items()):
        times = [to_ist(r.candle_start) for r in rows]
        times = [t for t in times if t is not None]
        ohlc = [(r.open, r.high, r.low, r.close) for r in rows]
        check = validate_regular_session_bars(d, times, ohlc)
        stype, notes = classify_session_type(
            d,
            candle_count=len(times),
            grid_ok=bool(check["grid_ok"]) and check["invalid_ohlc"] == 0,
            holiday_dates=holidays,
        )
        # Force known exclusions even if 38-bar coincidence
        if d in V1_EXCLUDED_MUHURAT_DATES:
            stype, notes = SESSION_TYPE_EXCLUDED_MUHURAT, "Diwali muhurat — excluded from V1"
        elif d in V1_EXCLUDED_SPECIAL_DATES:
            stype, notes = SESSION_TYPE_EXCLUDED_SPECIAL, "Special session — excluded from V1"
        records.append(
            {
                "ik": instrument_key,
                "sd": d,
                "st": stype,
                "inc": included_in_sambhav_v1(stype),
                "cc": len(times),
                "notes": notes,
            }
        )

    # Holidays with no candles — still classify as EXCLUDED_HOLIDAY
    d = first
    from datetime import timedelta

    while d <= last:
        if d in holidays and d not in by_day:
            records.append(
                {
                    "ik": instrument_key,
                    "sd": d,
                    "st": SESSION_TYPE_EXCLUDED_HOLIDAY,
                    "inc": False,
                    "cc": 0,
                    "notes": "NSE holiday — excluded from V1 (no candles expected)",
                }
            )
        d += timedelta(days=1)

    for rec in records:
        db.execute(
            text(
                """
                INSERT INTO sambhav_sessions (
                    instrument_key, session_date, session_type, included_in_sambhav_v1,
                    candle_count, notes, updated_at
                ) VALUES (
                    :ik, :sd, :st, :inc, :cc, :notes, CURRENT_TIMESTAMP
                )
                ON CONFLICT (instrument_key, session_date) DO UPDATE SET
                    session_type = EXCLUDED.session_type,
                    included_in_sambhav_v1 = EXCLUDED.included_in_sambhav_v1,
                    candle_count = EXCLUDED.candle_count,
                    notes = EXCLUDED.notes,
                    updated_at = CURRENT_TIMESTAMP
                """
            ),
            rec,
        )
    db.commit()

    by_type: Dict[str, int] = {}
    for r in records:
        by_type[r["st"]] = by_type.get(r["st"], 0) + 1
    logger.info("sambhav sessions classified: %s", by_type)
    return {
        "ok": True,
        "sessions_written": len(records),
        "by_type": by_type,
        "start_date": first.isoformat(),
        "end_date": last.isoformat(),
    }


def list_v1_regular_dates(db: Session, instrument_key: str = INSTRUMENT_KEY) -> List[date]:
    ensure_sambhav_tables()
    rows = db.execute(
        text(
            """
            SELECT session_date FROM sambhav_sessions
            WHERE instrument_key = :ik
              AND included_in_sambhav_v1 = TRUE
              AND session_type = :st
            ORDER BY session_date
            """
        ),
        {"ik": instrument_key, "st": SESSION_TYPE_REGULAR},
    ).fetchall()
    return [r.session_date for r in rows]

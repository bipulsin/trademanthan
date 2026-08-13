"""Sambhav historical importer.

V1 uses Upstox V3 10-minute candles directly (no 1-minute download, no
1-minute → 10-minute aggregation).

1-minute data may be added in a future Sambhav V2 feature-enhancement study.
The 1m helpers below are retained but are not called by the V1 import path.
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta
from typing import Any, Callable, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from backend.services.sambhav.candles import to_ist, upsert_10m_candles, validate_ohlc
from backend.services.sambhav.config import (
    HISTORICAL_SOURCE,
    IMPORT_CHUNK_DAYS,
    IMPORT_CHUNK_DAYS_1M,
    INSTRUMENT_KEY,
    IST,
)
from backend.services.sambhav.historical import (
    SambhavAuthError,
    SambhavFetchError,
    chunk_date_range,
    fetch_upstox_v3_10m,
    filter_valid_10m_candles,
    HistoricalThrottle,
    resolve_nifty_instrument_key,
    to_10m_row,
)
from backend.services.sambhav.tables import ensure_sambhav_tables

logger = logging.getLogger(__name__)

ProgressCb = Callable[[Dict[str, Any]], None]


def _get_upstox():
    from backend.config import settings
    from backend.services.upstox_service import UpstoxService

    return UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)


def _set_import_state(
    db: Session,
    instrument_key: str,
    *,
    status: str,
    detail: str = "",
    last_imported_ts: Optional[datetime] = None,
    last_from: Optional[date] = None,
    last_to: Optional[date] = None,
) -> None:
    ensure_sambhav_tables()
    db.execute(
        text(
            """
            INSERT INTO sambhav_import_state (
                instrument_key, last_imported_ts, last_from_date, last_to_date, status, detail, updated_at
            ) VALUES (
                :ik, :lit, :lf, :lt, :st, :detail, CURRENT_TIMESTAMP
            )
            ON CONFLICT (instrument_key) DO UPDATE SET
                last_imported_ts = COALESCE(EXCLUDED.last_imported_ts, sambhav_import_state.last_imported_ts),
                last_from_date = COALESCE(EXCLUDED.last_from_date, sambhav_import_state.last_from_date),
                last_to_date = COALESCE(EXCLUDED.last_to_date, sambhav_import_state.last_to_date),
                status = EXCLUDED.status,
                detail = EXCLUDED.detail,
                updated_at = CURRENT_TIMESTAMP
            """
        ),
        {
            "ik": instrument_key,
            "lit": last_imported_ts,
            "lf": last_from,
            "lt": last_to,
            "st": status,
            "detail": detail[:4000] if detail else "",
        },
    )
    db.commit()


def get_import_state(db: Session, instrument_key: str = INSTRUMENT_KEY) -> Dict[str, Any]:
    ensure_sambhav_tables()
    row = db.execute(
        text(
            """
            SELECT instrument_key, last_imported_ts, last_from_date, last_to_date, status, detail, updated_at
            FROM sambhav_import_state WHERE instrument_key = :ik
            """
        ),
        {"ik": instrument_key},
    ).fetchone()
    if not row:
        return {"instrument_key": instrument_key, "status": "idle"}
    detail = row.detail or ""
    parsed: Any = None
    if detail.startswith("{") or detail.startswith("["):
        try:
            parsed = json.loads(detail)
        except json.JSONDecodeError:
            parsed = None
    return {
        "instrument_key": row.instrument_key,
        "last_imported_ts": row.last_imported_ts.isoformat() if row.last_imported_ts else None,
        "last_from_date": str(row.last_from_date) if row.last_from_date else None,
        "last_to_date": str(row.last_to_date) if row.last_to_date else None,
        "status": row.status,
        "detail": detail,
        "progress": parsed if isinstance(parsed, dict) else None,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
    }


def upsert_raw_candles(
    db: Session,
    candles: List[Dict[str, Any]],
    instrument_key: str = INSTRUMENT_KEY,
    source: str = "upstox",
) -> int:
    """Unused by V1. 1-minute data may be added in a future Sambhav V2 feature-enhancement study."""
    ensure_sambhav_tables()
    sql = text(
        """
        INSERT INTO sambhav_raw_candles (
            instrument_key, candle_ts, open, high, low, close, volume, source
        ) VALUES (
            :ik, :ts, :o, :h, :l, :c, :v, :src
        )
        ON CONFLICT (instrument_key, candle_ts) DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            source = EXCLUDED.source
        """
    )
    n = 0
    max_ts: Optional[datetime] = None
    for c in candles or []:
        ts = to_ist(c.get("timestamp") or c.get("candle_ts"))
        if ts is None:
            continue
        o = float(c.get("open") or 0)
        h = float(c.get("high") or 0)
        l = float(c.get("low") or 0)
        cl = float(c.get("close") or 0)
        if not validate_ohlc(o, h, l, cl):
            continue
        vol = float(c.get("volume") or 0)
        db.execute(
            sql,
            {"ik": instrument_key, "ts": ts, "o": o, "h": h, "l": l, "c": cl, "v": vol, "src": source},
        )
        n += 1
        if max_ts is None or ts > max_ts:
            max_ts = ts
    if n:
        db.commit()
    return n


def _fetch_chunk(upstox, instrument_key: str, from_d: date, to_d: date) -> List[Dict[str, Any]]:
    """Unused by V1. 1-minute data may be added in a future Sambhav V2 feature-enhancement study."""
    from_s = from_d.strftime("%Y-%m-%d")
    to_s = to_d.strftime("%Y-%m-%d")
    candles = upstox._fetch_historical_v2_candles(instrument_key, "1minute", to_s, from_s)
    if not candles:
        candles = upstox._fetch_historical_v3_candles(instrument_key, "minutes/1", to_s, from_s)
    return candles or []


def import_historical_1m(
    db: Session,
    *,
    from_date: date,
    to_date: Optional[date] = None,
    instrument_key: str = INSTRUMENT_KEY,
    chunk_days: int = IMPORT_CHUNK_DAYS_1M,
    rebuild_10m: bool = True,
) -> Dict[str, Any]:
    """Unused by V1. 1-minute data may be added in a future Sambhav V2 feature-enhancement study."""
    raise RuntimeError(
        "Sambhav V1 does not download 1-minute historical candles. "
        "Use import_historical_10m (Upstox V3 minutes/10). "
        "1-minute data may be added in a future Sambhav V2 feature-enhancement study."
    )


def import_historical_10m(
    db: Session,
    *,
    from_date: date,
    to_date: Optional[date] = None,
    instrument_key: Optional[str] = None,
    chunk_days: int = IMPORT_CHUNK_DAYS,
    resume: bool = True,
    progress_cb: Optional[ProgressCb] = None,
    upstox: Any = None,
    throttle: Optional[HistoricalThrottle] = None,
    http_get: Any = None,
) -> Dict[str, Any]:
    """
    Chunked, restartable import of NIFTY 10-minute history via Upstox V3.

    Does not download 1-minute candles and does not aggregate.
    """
    ensure_sambhav_tables()
    if to_date is None:
        to_date = datetime.now(IST).date()
    if from_date > to_date:
        raise ValueError("from_date must be <= to_date")

    ux = upstox or _get_upstox()
    ik = instrument_key or resolve_nifty_instrument_key(ux)
    chunks = chunk_date_range(from_date, to_date, chunk_days=chunk_days)
    thr = throttle or HistoricalThrottle()

    start_idx = 0
    if resume:
        state = get_import_state(db, ik)
        last_to = state.get("last_to_date")
        st = (state.get("status") or "").lower()
        if last_to and st in ("running", "importing", "error"):
            try:
                last_d = date.fromisoformat(str(last_to)[:10])
            except ValueError:
                last_d = None
            if last_d is not None:
                for i, (_a, b) in enumerate(chunks):
                    if b <= last_d:
                        start_idx = i + 1

    total_written = 0
    total_received = 0
    reject_counts: Dict[str, int] = {}
    errors: List[str] = []
    last_ts: Optional[datetime] = None
    completed = start_idx
    auth_stopped = False

    def _progress(**extra: Any) -> Dict[str, Any]:
        payload = {
            "status": extra.pop("status", "IMPORTING"),
            "current_chunk": extra.get("current_chunk", completed),
            "completed_chunks": extra.get("completed_chunks", completed),
            "total_chunks": len(chunks),
            "chunk_from": extra.get("chunk_from"),
            "chunk_to": extra.get("chunk_to"),
            "candles_imported": total_written,
            "candles_received": total_received,
            "errors": list(errors),
            "reject_counts": dict(reject_counts),
            "interval": "10m",
            "instrument_key": ik,
        }
        payload.update(extra)
        if progress_cb:
            progress_cb(payload)
        return payload

    _set_import_state(
        db,
        ik,
        status="running",
        detail=json.dumps(_progress(status="IMPORTING", current_chunk=start_idx)),
    )
    _progress(status="IMPORTING", current_chunk=start_idx)

    try:
        for i in range(start_idx, len(chunks)):
            cursor, chunk_end = chunks[i]
            chunk_no = i + 1
            _progress(
                status="IMPORTING",
                current_chunk=chunk_no,
                completed_chunks=completed,
                chunk_from=str(cursor),
                chunk_to=str(chunk_end),
            )
            try:
                raw = fetch_upstox_v3_10m(
                    ux,
                    cursor,
                    chunk_end,
                    instrument_key=ik,
                    throttle=thr,
                    http_get=http_get,
                )
                total_received += len(raw or [])
                valid, reasons = filter_valid_10m_candles(raw or [])
                for k, v in reasons.items():
                    reject_counts[k] = reject_counts.get(k, 0) + v
                rows = [to_10m_row(c, source=HISTORICAL_SOURCE) for c in valid]
                written = upsert_10m_candles(db, rows, instrument_key=ik) if rows else 0
                total_written += written
                completed = chunk_no
                if rows:
                    last_ts = rows[-1]["candle_start"]
                detail = _progress(
                    status="IMPORTING",
                    current_chunk=chunk_no,
                    completed_chunks=completed,
                    chunk_from=str(cursor),
                    chunk_to=str(chunk_end),
                    chunk_written=written,
                    chunk_received=len(raw or []),
                )
                _set_import_state(
                    db,
                    ik,
                    status="running",
                    detail=json.dumps(detail, default=str),
                    last_imported_ts=last_ts,
                    last_from=cursor,
                    last_to=chunk_end,
                )
                logger.info(
                    "sambhav 10m import chunk %s/%s %s..%s received=%s written=%s rejected=%s",
                    chunk_no,
                    len(chunks),
                    cursor,
                    chunk_end,
                    len(raw or []),
                    written,
                    reasons,
                )
            except SambhavAuthError as exc:
                auth_stopped = True
                msg = f"{cursor}..{chunk_end}: AUTH {exc}"
                errors.append(msg)
                logger.error("sambhav 10m import stopped on auth error: %s", msg)
                _set_import_state(db, ik, status="error", detail=msg)
                break
            except (SambhavFetchError, Exception) as exc:
                msg = f"{cursor}..{chunk_end}: {exc}"
                errors.append(msg)
                logger.exception("sambhav 10m import chunk failed: %s", msg)
                _set_import_state(db, ik, status="error", detail=msg)
                # Continue to the next chunk unless it was an auth failure.

        final_status = "done" if not errors else "error"
        result: Dict[str, Any] = {
            "ok": len(errors) == 0,
            "instrument_key": ik,
            "interval": "10m",
            "source": HISTORICAL_SOURCE,
            "from_date": str(from_date),
            "to_date": str(to_date),
            "chunks": len(chunks),
            "completed_chunks": completed,
            "upserted_10m": total_written,
            "received": total_received,
            "reject_counts": reject_counts,
            "errors": errors,
            "auth_stopped": auth_stopped,
            "resumed_from_chunk": start_idx + 1 if start_idx else None,
        }
        _set_import_state(
            db,
            ik,
            status=final_status,
            detail=json.dumps({**result, "status": "DONE" if result["ok"] else "ERROR"}, default=str),
            last_imported_ts=last_ts,
            last_from=from_date,
            last_to=to_date,
        )
        _progress(
            status="DONE" if result["ok"] else "ERROR",
            current_chunk=completed,
            completed_chunks=completed,
            chunk_from=str(from_date),
            chunk_to=str(to_date),
            result=result,
        )
        return result
    except Exception as exc:
        _set_import_state(db, ik, status="error", detail=str(exc))
        raise


def last_stored_candle_date(
    db: Session,
    instrument_key: str = INSTRUMENT_KEY,
) -> Optional[date]:
    """IST calendar date of the latest stored 10m candle (source table)."""
    ensure_sambhav_tables()
    row = db.execute(
        text(
            """
            SELECT MAX((candle_start AT TIME ZONE 'Asia/Kolkata')::date)
            FROM sambhav_10m_candles WHERE instrument_key = :ik
            """
        ),
        {"ik": instrument_key},
    ).fetchone()
    return row[0] if row and row[0] else None


def import_incremental_10m(
    db: Session,
    *,
    to_date: Optional[date] = None,
    instrument_key: Optional[str] = None,
    progress_cb: Optional[ProgressCb] = None,
    upstox: Any = None,
) -> Dict[str, Any]:
    """
    Append-only incremental import after the last stored candle.

    Does NOT re-download the full historical range. Classifies new sessions
    after a successful append.
    """
    ensure_sambhav_tables()
    ik = instrument_key or INSTRUMENT_KEY
    last_d = last_stored_candle_date(db, ik)
    end = to_date or datetime.now(IST).date()
    if last_d is None:
        return {
            "ok": False,
            "error": "no_existing_history",
            "message": "No stored candles — run a bounded historical import first (not a full re-download from this helper).",
        }
    from_date = last_d  # re-fetch last day for completeness, then append newer days
    if from_date > end:
        return {
            "ok": True,
            "skipped": True,
            "reason": "already_up_to_date",
            "last_stored_date": str(last_d),
            "to_date": str(end),
            "upserted_10m": 0,
        }
    result = import_historical_10m(
        db,
        from_date=from_date,
        to_date=end,
        instrument_key=ik,
        resume=False,
        progress_cb=progress_cb,
        upstox=upstox,
    )
    try:
        from backend.services.sambhav.sessions import classify_and_persist_sessions

        classification = classify_and_persist_sessions(
            db, instrument_key=ik, start_date=from_date, end_date=end
        )
        result["classification"] = classification
    except Exception as exc:
        logger.exception("sambhav incremental classification failed")
        result["classification_error"] = str(exc)
    result["mode"] = "incremental"
    result["last_stored_date_before"] = str(last_d)
    return result

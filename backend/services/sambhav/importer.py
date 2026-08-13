"""Historical 1m NIFTY importer — chunked, restartable, deduped via UNIQUE upsert."""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from backend.services.sambhav.candles import to_ist, validate_ohlc
from backend.services.sambhav.config import (
    IMPORT_CHUNK_DAYS,
    INSTRUMENT_KEY,
    IST,
)
from backend.services.sambhav.tables import ensure_sambhav_tables

logger = logging.getLogger(__name__)


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
            "detail": detail[:2000] if detail else "",
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
    return {
        "instrument_key": row.instrument_key,
        "last_imported_ts": row.last_imported_ts.isoformat() if row.last_imported_ts else None,
        "last_from_date": str(row.last_from_date) if row.last_from_date else None,
        "last_to_date": str(row.last_to_date) if row.last_to_date else None,
        "status": row.status,
        "detail": row.detail,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
    }


def upsert_raw_candles(
    db: Session,
    candles: List[Dict[str, Any]],
    instrument_key: str = INSTRUMENT_KEY,
    source: str = "upstox",
) -> int:
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
    """Fetch 1m candles for [from_d, to_d] inclusive via V2 then V3 fallback."""
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
    chunk_days: int = IMPORT_CHUNK_DAYS,
    rebuild_10m: bool = True,
) -> Dict[str, Any]:
    """
    Chunked restartable import of NIFTY 1m history into sambhav_raw_candles.
    """
    ensure_sambhav_tables()
    if to_date is None:
        to_date = datetime.now(IST).date()
    if from_date > to_date:
        raise ValueError("from_date must be <= to_date")

    upstox = _get_upstox()
    _set_import_state(db, instrument_key, status="running", detail=f"import {from_date}..{to_date}")

    total = 0
    chunks = 0
    errors: List[str] = []
    cursor = from_date
    last_ts: Optional[datetime] = None

    try:
        while cursor <= to_date:
            chunk_end = min(cursor + timedelta(days=chunk_days - 1), to_date)
            try:
                candles = _fetch_chunk(upstox, instrument_key, cursor, chunk_end)
                written = upsert_raw_candles(db, candles, instrument_key=instrument_key)
                total += written
                chunks += 1
                if candles:
                    for c in candles:
                        ts = to_ist(c.get("timestamp"))
                        if ts and (last_ts is None or ts > last_ts):
                            last_ts = ts
                _set_import_state(
                    db,
                    instrument_key,
                    status="running",
                    detail=f"ok {cursor}..{chunk_end} wrote={written}",
                    last_imported_ts=last_ts,
                    last_from=cursor,
                    last_to=chunk_end,
                )
                logger.info(
                    "sambhav import chunk %s..%s candles=%s written=%s",
                    cursor,
                    chunk_end,
                    len(candles),
                    written,
                )
            except Exception as exc:
                msg = f"{cursor}..{chunk_end}: {exc}"
                errors.append(msg)
                logger.exception("sambhav import chunk failed: %s", msg)
                _set_import_state(db, instrument_key, status="error", detail=msg)
            cursor = chunk_end + timedelta(days=1)

        result: Dict[str, Any] = {
            "ok": len(errors) == 0,
            "instrument_key": instrument_key,
            "from_date": str(from_date),
            "to_date": str(to_date),
            "chunks": chunks,
            "upserted_1m": total,
            "errors": errors,
        }

        if rebuild_10m and total > 0:
            from backend.services.sambhav.candles import build_10m_candles

            agg = build_10m_candles(db, instrument_key=instrument_key, require_complete=True)
            result["agg_10m"] = agg

        _set_import_state(
            db,
            instrument_key,
            status="done" if not errors else "error",
            detail=f"upserted_1m={total} errors={len(errors)}",
            last_imported_ts=last_ts,
            last_from=from_date,
            last_to=to_date,
        )
        return result
    except Exception as exc:
        _set_import_state(db, instrument_key, status="error", detail=str(exc))
        raise

"""Postgres persistence for Open Low High screen rows + day freeze meta."""
from __future__ import annotations

import json
import logging
from datetime import date, datetime
from typing import Any, Dict, List, Optional

import pytz
from sqlalchemy import text

from backend.database import SessionLocal

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

# Base table (legacy columns). New fields are added via _MIGRATE_COLS so
# CREATE TABLE IF NOT EXISTS does not skip ALTERs on an existing table.
_DDL = """
CREATE TABLE IF NOT EXISTS open_low_high_screen (
    id BIGSERIAL PRIMARY KEY,
    trade_date DATE NOT NULL,
    symbol VARCHAR(64) NOT NULL,
    side VARCHAR(16) NOT NULL,
    contract VARCHAR(128),
    expiry DATE,
    instrument_key VARCHAR(128),
    prev_close DOUBLE PRECISION,
    prev_high DOUBLE PRECISION,
    prev_low DOUBLE PRECISION,
    first_open DOUBLE PRECISION,
    first_high DOUBLE PRECISION,
    first_low DOUBLE PRECISION,
    first_close DOUBLE PRECISION,
    second_close DOUBLE PRECISION,
    vwap_0915 DOUBLE PRECISION,
    sector_badge VARCHAR(8),
    sector_star BOOLEAN NOT NULL DEFAULT FALSE,
    sector_badge_display VARCHAR(16),
    nifty_sector_label TEXT,
    frozen BOOLEAN NOT NULL DEFAULT FALSE,
    frozen_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT ux_open_low_high_screen_date_symbol_side UNIQUE (trade_date, symbol, side)
)
"""

_INDEX_DDL = [
    """
    CREATE INDEX IF NOT EXISTS ix_open_low_high_screen_trade_date
        ON open_low_high_screen (trade_date DESC)
    """,
    """
    CREATE INDEX IF NOT EXISTS ix_open_low_high_screen_instrument
        ON open_low_high_screen (trade_date, instrument_key)
    """,
    """
    CREATE INDEX IF NOT EXISTS ix_open_low_high_screen_active
        ON open_low_high_screen (trade_date, active)
    """,
]

_META_DDL = """
CREATE TABLE IF NOT EXISTS open_low_high_day_meta (
    trade_date DATE PRIMARY KEY,
    status VARCHAR(32) NOT NULL DEFAULT 'waiting',
    frozen BOOLEAN NOT NULL DEFAULT FALSE,
    frozen_at TIMESTAMPTZ,
    phase_a_at TIMESTAMPTZ,
    phase_b_at TIMESTAMPTZ,
    bullish_count INTEGER NOT NULL DEFAULT 0,
    bearish_count INTEGER NOT NULL DEFAULT 0,
    notes TEXT,
    meta_json TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
"""

_MIGRATE_COLS = [
    "ALTER TABLE open_low_high_screen ADD COLUMN IF NOT EXISTS active BOOLEAN NOT NULL DEFAULT FALSE",
    "ALTER TABLE open_low_high_screen ADD COLUMN IF NOT EXISTS phase_b_ok BOOLEAN NOT NULL DEFAULT FALSE",
    "ALTER TABLE open_low_high_screen ADD COLUMN IF NOT EXISTS sl DOUBLE PRECISION",
    "ALTER TABLE open_low_high_screen ADD COLUMN IF NOT EXISTS entry DOUBLE PRECISION",
    "ALTER TABLE open_low_high_screen ADD COLUMN IF NOT EXISTS t1 DOUBLE PRECISION",
    "ALTER TABLE open_low_high_screen ADD COLUMN IF NOT EXISTS t2 DOUBLE PRECISION",
]


def ensure_tables() -> None:
    db = SessionLocal()
    try:
        db.execute(text(_DDL))
        db.execute(text(_META_DDL))
        for stmt in _MIGRATE_COLS:
            db.execute(text(stmt))
        for stmt in _INDEX_DDL:
            db.execute(text(stmt))
        db.commit()
    except Exception:
        db.rollback()
        logger.exception("open_low_high ensure_tables failed")
        raise
    finally:
        db.close()


def _aware(dt: Optional[datetime] = None) -> datetime:
    t = dt or datetime.now(IST)
    if t.tzinfo is None:
        return IST.localize(t)
    return t.astimezone(IST)


def _row_to_dict(r: Any) -> Dict[str, Any]:
    if r is None:
        return {}
    if hasattr(r, "_mapping"):
        d = dict(r._mapping)
    elif isinstance(r, dict):
        d = dict(r)
    else:
        d = dict(r)
    for k, v in list(d.items()):
        if isinstance(v, datetime):
            d[k] = _aware(v).isoformat()
        elif isinstance(v, date):
            d[k] = v.isoformat()
    # Normalize booleans / derived card fields for API consumers.
    if "active" in d:
        d["active"] = bool(d.get("active"))
    if "phase_b_ok" in d:
        d["phase_b_ok"] = bool(d.get("phase_b_ok"))
    # SL defaults to 09:15 open when column empty (legacy rows).
    if d.get("sl") is None and d.get("first_open") is not None:
        d["sl"] = d.get("first_open")
    return d


def upsert_screen_rows(trade_date: date, rows: List[Dict[str, Any]], *, frozen: bool) -> int:
    """Replace today's screen set with ``rows`` (delete+insert keeps uniqueness clean)."""
    ensure_tables()
    db = SessionLocal()
    frozen_at = _aware() if frozen else None
    try:
        db.execute(
            text("DELETE FROM open_low_high_screen WHERE trade_date = :d"),
            {"d": trade_date},
        )
        n = 0
        for row in rows:
            active = bool(row.get("active") if row.get("active") is not None else row.get("phase_b_ok"))
            phase_b_ok = bool(row.get("phase_b_ok") if row.get("phase_b_ok") is not None else active)
            sl = row.get("sl")
            if sl is None:
                sl = row.get("first_open")
            entry = row.get("entry")
            if entry is None:
                entry = row.get("second_close") if row.get("second_close") is not None else row.get("first_close")
            db.execute(
                text(
                    """
                    INSERT INTO open_low_high_screen (
                        trade_date, symbol, side, contract, expiry, instrument_key,
                        prev_close, prev_high, prev_low,
                        first_open, first_high, first_low, first_close,
                        second_close, vwap_0915,
                        active, phase_b_ok, sl, entry, t1, t2,
                        sector_badge, sector_star, sector_badge_display, nifty_sector_label,
                        frozen, frozen_at, updated_at
                    ) VALUES (
                        :trade_date, :symbol, :side, :contract, :expiry, :instrument_key,
                        :prev_close, :prev_high, :prev_low,
                        :first_open, :first_high, :first_low, :first_close,
                        :second_close, :vwap_0915,
                        :active, :phase_b_ok, :sl, :entry, :t1, :t2,
                        :sector_badge, :sector_star, :sector_badge_display, :nifty_sector_label,
                        :frozen, :frozen_at, NOW()
                    )
                    """
                ),
                {
                    "trade_date": trade_date,
                    "symbol": str(row.get("symbol") or "").upper(),
                    "side": str(row.get("side") or "").upper(),
                    "contract": row.get("contract"),
                    "expiry": row.get("expiry"),
                    "instrument_key": row.get("instrument_key"),
                    "prev_close": row.get("prev_close"),
                    "prev_high": row.get("prev_high"),
                    "prev_low": row.get("prev_low"),
                    "first_open": row.get("first_open"),
                    "first_high": row.get("first_high"),
                    "first_low": row.get("first_low"),
                    "first_close": row.get("first_close"),
                    "second_close": row.get("second_close"),
                    "vwap_0915": row.get("vwap_0915"),
                    "active": active,
                    "phase_b_ok": phase_b_ok,
                    "sl": sl,
                    "entry": entry,
                    "t1": row.get("t1"),
                    "t2": row.get("t2"),
                    "sector_badge": row.get("sector_badge"),
                    "sector_star": bool(row.get("sector_star")),
                    "sector_badge_display": row.get("sector_badge_display"),
                    "nifty_sector_label": row.get("nifty_sector_label"),
                    "frozen": bool(frozen),
                    "frozen_at": frozen_at,
                },
            )
            n += 1
        db.commit()
        return n
    except Exception:
        db.rollback()
        logger.exception("open_low_high upsert_screen_rows failed")
        raise
    finally:
        db.close()


def upsert_day_meta(
    trade_date: date,
    *,
    status: str,
    frozen: bool,
    bullish_count: int = 0,
    bearish_count: int = 0,
    notes: Optional[str] = None,
    meta: Optional[Dict[str, Any]] = None,
    phase_a_at: Optional[datetime] = None,
    phase_b_at: Optional[datetime] = None,
) -> None:
    ensure_tables()
    db = SessionLocal()
    frozen_at = _aware() if frozen else None
    try:
        db.execute(
            text(
                """
                INSERT INTO open_low_high_day_meta (
                    trade_date, status, frozen, frozen_at,
                    phase_a_at, phase_b_at,
                    bullish_count, bearish_count, notes, meta_json, updated_at
                ) VALUES (
                    :d, :status, :frozen, :frozen_at,
                    :phase_a_at, :phase_b_at,
                    :bullish_count, :bearish_count, :notes, :meta_json, NOW()
                )
                ON CONFLICT (trade_date) DO UPDATE SET
                    status = EXCLUDED.status,
                    frozen = EXCLUDED.frozen,
                    frozen_at = COALESCE(EXCLUDED.frozen_at, open_low_high_day_meta.frozen_at),
                    phase_a_at = COALESCE(EXCLUDED.phase_a_at, open_low_high_day_meta.phase_a_at),
                    phase_b_at = COALESCE(EXCLUDED.phase_b_at, open_low_high_day_meta.phase_b_at),
                    bullish_count = EXCLUDED.bullish_count,
                    bearish_count = EXCLUDED.bearish_count,
                    notes = COALESCE(EXCLUDED.notes, open_low_high_day_meta.notes),
                    meta_json = COALESCE(EXCLUDED.meta_json, open_low_high_day_meta.meta_json),
                    updated_at = NOW()
                """
            ),
            {
                "d": trade_date,
                "status": status,
                "frozen": bool(frozen),
                "frozen_at": frozen_at,
                "phase_a_at": phase_a_at,
                "phase_b_at": phase_b_at,
                "bullish_count": int(bullish_count),
                "bearish_count": int(bearish_count),
                "notes": notes,
                "meta_json": json.dumps(meta) if meta is not None else None,
            },
        )
        db.commit()
    except Exception:
        db.rollback()
        logger.exception("open_low_high upsert_day_meta failed")
        raise
    finally:
        db.close()


def get_day_meta(trade_date: date) -> Optional[Dict[str, Any]]:
    ensure_tables()
    db = SessionLocal()
    try:
        row = db.execute(
            text("SELECT * FROM open_low_high_day_meta WHERE trade_date = :d"),
            {"d": trade_date},
        ).mappings().first()
        if not row:
            return None
        d = _row_to_dict(row)
        raw = d.get("meta_json")
        if isinstance(raw, str) and raw:
            try:
                d["meta"] = json.loads(raw)
            except Exception:
                d["meta"] = None
        return d
    finally:
        db.close()


def is_day_frozen(trade_date: date) -> bool:
    meta = get_day_meta(trade_date)
    if meta and meta.get("frozen"):
        return True
    db = SessionLocal()
    try:
        ensure_tables()
        row = db.execute(
            text(
                """
                SELECT COUNT(*) AS n FROM open_low_high_screen
                WHERE trade_date = :d AND frozen = TRUE
                """
            ),
            {"d": trade_date},
        ).mappings().first()
        return int((row or {}).get("n") or 0) > 0
    finally:
        db.close()


def list_screen_for_date(trade_date: date) -> List[Dict[str, Any]]:
    """Screen rows for a date: actives first, then symbol within each side group (caller splits)."""
    ensure_tables()
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT * FROM open_low_high_screen
                WHERE trade_date = :d
                ORDER BY side ASC,
                         CASE WHEN active THEN 0 ELSE 1 END ASC,
                         symbol ASC
                """
            ),
            {"d": trade_date},
        ).mappings().all()
        return [_row_to_dict(r) for r in rows]
    finally:
        db.close()


def list_trade_log(
    *,
    limit_days: int = 60,
    side: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """All stored screen rows (newest dates first) for the Trade Log tab."""
    ensure_tables()
    db = SessionLocal()
    try:
        params: Dict[str, Any] = {"lim": max(1, int(limit_days))}
        side_clause = ""
        if side:
            side_clause = "AND side = :side"
            params["side"] = str(side).upper()
        rows = db.execute(
            text(
                f"""
                SELECT * FROM open_low_high_screen
                WHERE trade_date >= (CURRENT_DATE - (:lim::int))
                {side_clause}
                ORDER BY trade_date DESC,
                         side ASC,
                         CASE WHEN active THEN 0 ELSE 1 END ASC,
                         symbol ASC
                """
            ),
            params,
        ).mappings().all()
        return [_row_to_dict(r) for r in rows]
    finally:
        db.close()

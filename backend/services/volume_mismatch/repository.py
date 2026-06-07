"""Persistence for volume_mismatch_signals."""
from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any, Dict, List, Optional

import pytz
from sqlalchemy import text
from sqlalchemy.orm import Session

from backend.services.volume_mismatch.tables import ensure_volume_mismatch_signals_table

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")


def _row_to_dict(row) -> Dict[str, Any]:
    if row is None:
        return {}
    if hasattr(row, "_mapping"):
        return dict(row._mapping)
    return dict(row)


def upsert_signal(db: Session, trade_date: date, payload: Dict[str, Any]) -> None:
    ensure_volume_mismatch_signals_table(db)
    db.execute(
        text(
            """
            INSERT INTO volume_mismatch_signals (
                symbol, future_symbol, instrument_token, trade_date, direction,
                gap_percent, first_15m_volume, relative_volume, net_volume, score,
                entry_price, preferred_entry, stop_loss, target1, target2,
                first_15m_high, first_15m_low, first_15m_open, first_15m_close,
                bb_upper, bb_middle, bb_lower,
                current_price, entry_status,
                created_at, updated_at
            ) VALUES (
                :symbol, :future_symbol, :instrument_token, :trade_date, :direction,
                :gap_percent, :first_15m_volume, :relative_volume, :net_volume, :score,
                :entry_price, :preferred_entry, :stop_loss, :target1, :target2,
                :first_15m_high, :first_15m_low, :first_15m_open, :first_15m_close,
                :bb_upper, :bb_middle, :bb_lower,
                :current_price, :entry_status,
                NOW(), NOW()
            )
            ON CONFLICT (trade_date, symbol) DO UPDATE SET
                future_symbol = EXCLUDED.future_symbol,
                instrument_token = EXCLUDED.instrument_token,
                direction = EXCLUDED.direction,
                gap_percent = EXCLUDED.gap_percent,
                first_15m_volume = EXCLUDED.first_15m_volume,
                relative_volume = EXCLUDED.relative_volume,
                net_volume = EXCLUDED.net_volume,
                score = EXCLUDED.score,
                entry_price = EXCLUDED.entry_price,
                preferred_entry = EXCLUDED.preferred_entry,
                stop_loss = EXCLUDED.stop_loss,
                target1 = EXCLUDED.target1,
                target2 = EXCLUDED.target2,
                first_15m_high = EXCLUDED.first_15m_high,
                first_15m_low = EXCLUDED.first_15m_low,
                first_15m_open = EXCLUDED.first_15m_open,
                first_15m_close = EXCLUDED.first_15m_close,
                bb_upper = EXCLUDED.bb_upper,
                bb_middle = EXCLUDED.bb_middle,
                bb_lower = EXCLUDED.bb_lower,
                current_price = EXCLUDED.current_price,
                entry_status = CASE
                    WHEN volume_mismatch_signals.entry_status IN ('TRIGGERED', 'EXPIRED')
                    THEN volume_mismatch_signals.entry_status
                    ELSE EXCLUDED.entry_status
                END,
                updated_at = NOW()
            """
        ),
        {
            "symbol": payload.get("symbol"),
            "future_symbol": payload.get("future_symbol"),
            "instrument_token": payload.get("instrument_key") or payload.get("instrument_token"),
            "trade_date": trade_date,
            "direction": payload.get("direction"),
            "gap_percent": payload.get("gap_percent"),
            "first_15m_volume": payload.get("first_15m_volume"),
            "relative_volume": payload.get("relative_volume"),
            "net_volume": payload.get("net_volume"),
            "score": payload.get("score"),
            "entry_price": payload.get("entry_price"),
            "preferred_entry": payload.get("preferred_entry"),
            "stop_loss": payload.get("stop_loss"),
            "target1": payload.get("target1"),
            "target2": payload.get("target2"),
            "first_15m_high": payload.get("first_15m_high"),
            "first_15m_low": payload.get("first_15m_low"),
            "first_15m_open": payload.get("first_15m_open"),
            "first_15m_close": payload.get("first_15m_close"),
            "bb_upper": payload.get("bb_upper"),
            "bb_middle": payload.get("bb_middle"),
            "bb_lower": payload.get("bb_lower"),
            "current_price": payload.get("current_price"),
            "entry_status": payload.get("entry_status") or "WAITING",
        },
    )


def fetch_signals_for_date(
    db: Session,
    trade_date: date,
    *,
    direction: Optional[str] = None,
    entry_status: Optional[str] = None,
    min_score: Optional[float] = None,
) -> List[Dict[str, Any]]:
    ensure_volume_mismatch_signals_table(db)
    clauses = ["trade_date = :trade_date"]
    params: Dict[str, Any] = {"trade_date": trade_date}
    if direction:
        clauses.append("UPPER(direction) = UPPER(:direction)")
        params["direction"] = direction
    if entry_status:
        clauses.append("UPPER(entry_status) = UPPER(:entry_status)")
        params["entry_status"] = entry_status
    if min_score is not None:
        clauses.append("score >= :min_score")
        params["min_score"] = min_score

    sql = f"""
        SELECT *
        FROM volume_mismatch_signals
        WHERE {' AND '.join(clauses)}
        ORDER BY score DESC NULLS LAST, ABS(gap_percent) DESC NULLS LAST,
                 first_15m_volume DESC NULLS LAST
    """
    rows = db.execute(text(sql), params).fetchall()
    return [_row_to_dict(r) for r in rows]


def update_signal_monitor_fields(
    db: Session,
    signal_id: int,
    *,
    current_price: Optional[float] = None,
    vwap: Optional[float] = None,
    entry_status: Optional[str] = None,
) -> None:
    sets = ["updated_at = NOW()"]
    params: Dict[str, Any] = {"id": signal_id}
    if current_price is not None:
        sets.append("current_price = :current_price")
        params["current_price"] = current_price
    if vwap is not None:
        sets.append("vwap = :vwap")
        params["vwap"] = vwap
    if entry_status is not None:
        sets.append("entry_status = :entry_status")
        params["entry_status"] = entry_status
    db.execute(
        text(f"UPDATE volume_mismatch_signals SET {', '.join(sets)} WHERE id = :id"),
        params,
    )


def mark_triggered(db: Session, signal_id: int) -> Optional[Dict[str, Any]]:
    db.execute(
        text(
            """
            UPDATE volume_mismatch_signals
            SET entry_status = 'TRIGGERED', updated_at = NOW()
            WHERE id = :id AND entry_status = 'READY'
            RETURNING *
            """
        ),
        {"id": signal_id},
    )
    row = db.execute(
        text("SELECT * FROM volume_mismatch_signals WHERE id = :id"),
        {"id": signal_id},
    ).fetchone()
    return _row_to_dict(row) if row else None


def fetch_scan_meta(db: Session, trade_date: date) -> Dict[str, Any]:
    ensure_volume_mismatch_signals_table(db)
    row = db.execute(
        text(
            """
            SELECT COUNT(*) AS cnt,
                   MAX(updated_at) AS last_updated
            FROM volume_mismatch_signals
            WHERE trade_date = :trade_date
            """
        ),
        {"trade_date": trade_date},
    ).fetchone()
    d = _row_to_dict(row)
    return {
        "signal_count": int(d.get("cnt") or 0),
        "last_updated": d.get("last_updated"),
    }

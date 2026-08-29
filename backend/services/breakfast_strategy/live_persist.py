"""Persist Breakfast Strategy live lock signals to PostgreSQL."""
from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytz

from sqlalchemy import text
from sqlalchemy.orm import Session

from backend.database import SessionLocal, engine

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

_MIGRATION = Path(__file__).resolve().parents[2] / "migrations" / "add_breakfast_live_signals.sql"
_ENSURED = False

_MANUAL_FIELDS = frozenset(
    {
        "manual_entry_price",
        "manual_entry_time",
        "manual_exit_price",
        "manual_exit_time",
        "manual_note",
    }
)


def ensure_breakfast_live_signals_table() -> None:
    global _ENSURED
    if _ENSURED:
        return
    if not _MIGRATION.is_file():
        logger.warning("breakfast live signals migration missing: %s", _MIGRATION)
        return
    sql = _MIGRATION.read_text(encoding="utf-8")
    with engine.begin() as conn:
        conn.execute(text(sql))
    _ENSURED = True
    logger.info("breakfast_live_signals table ensured")


def _parse_ts(raw: Optional[str]) -> Optional[datetime]:
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            return IST.localize(dt)
        return dt.astimezone(IST)
    except (TypeError, ValueError):
        return None


def rows_from_live_state(
    state: Dict[str, Any],
    cross_check_status: str,
) -> List[Dict[str, Any]]:
    """Build insert payloads from a locked live-state dict (pure, no DB)."""
    session_date = str(state.get("session_date") or "")[:10]
    if not session_date:
        return []

    locked_at = _parse_ts(state.get("server_time")) or datetime.now(IST)
    nifty_pct = (state.get("nifty") or {}).get("bias_pct")
    rows: List[Dict[str, Any]] = []

    for sec in state.get("sectors") or []:
        sector_label = str(sec.get("sector_label") or sec.get("sector_key") or "").strip()
        sector_rank = int(sec.get("sector_rank") or 0)
        sector_move = sec.get("move_pct")
        direction = str(sec.get("direction") or "").strip().upper()
        if not sector_label or sector_rank < 1 or sector_move is None or direction not in ("LONG", "SHORT"):
            continue

        for stk in sec.get("stocks") or []:
            sym = str(stk.get("symbol") or "").strip().upper()
            if not sym:
                continue
            rank_at_lock = int(stk.get("rank_in_sector") or stk.get("stock_rank") or 0)
            move = stk.get("move_pct_at_entry")
            anchor = stk.get("anchor_price")
            tp = stk.get("tp_price")
            sl = stk.get("sl_price")
            lot = int(stk.get("lot_size") or 0)
            if rank_at_lock < 1 or rank_at_lock > 3 or move is None or anchor is None or tp is None or sl is None or lot <= 0:
                continue
            stk_dir = str(stk.get("direction") or direction).strip().upper()
            if stk_dir not in ("LONG", "SHORT"):
                stk_dir = direction
            rows.append(
                {
                    "session_date": session_date,
                    "symbol": sym,
                    "direction": stk_dir,
                    "sector": sector_label,
                    "sector_rank": sector_rank,
                    "rank_at_lock": rank_at_lock,
                    "nifty_bias_pct": nifty_pct,
                    "sector_move_pct": float(sector_move),
                    "stock_move_pct_at_lock": float(move),
                    "ltp_at_lock": stk.get("ltp"),
                    "anchor_price": float(anchor),
                    "tp_price": float(tp),
                    "sl_price": float(sl),
                    "lot_size": lot,
                    "locked_at_timestamp": locked_at.isoformat(),
                    "websocket_rest_cross_check_status": cross_check_status,
                    "instrument_key": stk.get("instrument_key"),
                }
            )
    return rows


def _insert_signal(db: Session, row: Dict[str, Any]) -> bool:
    n = db.execute(
        text(
            """
            SELECT COUNT(*) FROM breakfast_live_signals
            WHERE session_date = CAST(:session_date AS date)
              AND UPPER(symbol) = UPPER(:symbol)
              AND direction = :direction
            """
        ),
        row,
    ).scalar()
    if int(n or 0) > 0:
        return False
    db.execute(
        text(
            """
            INSERT INTO breakfast_live_signals (
                session_date, symbol, direction, sector, sector_rank, rank_at_lock,
                nifty_bias_pct, sector_move_pct, stock_move_pct_at_lock, ltp_at_lock,
                anchor_price, tp_price, sl_price, lot_size, locked_at_timestamp,
                websocket_rest_cross_check_status, instrument_key
            ) VALUES (
                CAST(:session_date AS date), :symbol, :direction, :sector, :sector_rank, :rank_at_lock,
                :nifty_bias_pct, :sector_move_pct, :stock_move_pct_at_lock, :ltp_at_lock,
                :anchor_price, :tp_price, :sl_price, :lot_size,
                CAST(:locked_at_timestamp AS timestamptz),
                :websocket_rest_cross_check_status, :instrument_key
            )
            """
        ),
        row,
    )
    return True


def persist_live_signals(state: Dict[str, Any], cross_check_status: str) -> Dict[str, int]:
    """Idempotent insert for all stocks in locked state (up to 6 rows)."""
    status = str(cross_check_status or "matched").strip().lower()
    if status not in ("matched", "mismatched"):
        status = "matched"
    rows = rows_from_live_state(state, status)
    if not rows:
        return {"inserted": 0, "skipped": 0}

    ensure_breakfast_live_signals_table()
    db = SessionLocal()
    inserted = skipped = 0
    try:
        for row in rows:
            if _insert_signal(db, row):
                inserted += 1
            else:
                skipped += 1
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
    return {"inserted": inserted, "skipped": skipped}


def fetch_live_signals(session_date: str) -> List[Dict[str, Any]]:
    ensure_breakfast_live_signals_table()
    sd = str(session_date or "")[:10]
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT *
                FROM breakfast_live_signals
                WHERE session_date = CAST(:sd AS date)
                ORDER BY sector_rank, rank_at_lock, symbol
                """
            ),
            {"sd": sd},
        ).mappings().all()
        return [dict(r) for r in rows]
    finally:
        db.close()


def update_manual_capture(
    session_date: str,
    symbol: str,
    direction: str,
    fields: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    ensure_breakfast_live_signals_table()
    sd = str(session_date or "")[:10]
    sym = str(symbol or "").strip().upper()
    dir_u = str(direction or "").strip().upper()
    if not sd or not sym or dir_u not in ("LONG", "SHORT"):
        return None

    sets: List[str] = []
    params: Dict[str, Any] = {"sd": sd, "sym": sym, "dir": dir_u}
    trade_taken = False

    for key in _MANUAL_FIELDS:
        if key not in fields:
            continue
        val = fields[key]
        if key in ("manual_entry_time", "manual_exit_time") and val is not None:
            sets.append(f"{key} = CAST(:{key} AS timestamptz)")
            params[key] = val
            trade_taken = True
        elif val is not None:
            sets.append(f"{key} = :{key}")
            params[key] = val
            trade_taken = True

    if not sets:
        rows = fetch_live_signals(sd)
        for r in rows:
            if str(r.get("symbol")).upper() == sym and r.get("direction") == dir_u:
                return r
        return None

    sets.append("trade_taken = :trade_taken")
    params["trade_taken"] = trade_taken
    sets.append("updated_at = NOW()")

    db = SessionLocal()
    try:
        row = db.execute(
            text(
                f"""
                UPDATE breakfast_live_signals
                SET {", ".join(sets)}
                WHERE session_date = CAST(:sd AS date)
                  AND UPPER(symbol) = UPPER(:sym)
                  AND direction = :dir
                RETURNING *
                """
            ),
            params,
        ).mappings().first()
        if not row:
            db.rollback()
            return None
        db.commit()
        return dict(row)
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()

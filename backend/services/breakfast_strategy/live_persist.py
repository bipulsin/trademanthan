"""Persist Breakfast Strategy live lock signals to PostgreSQL."""
from __future__ import annotations

import json
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
_SESSION_LOCK_MIGRATION = Path(__file__).resolve().parents[2] / "migrations" / "add_breakfast_session_lock.sql"
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
    for mig in (_MIGRATION, _SESSION_LOCK_MIGRATION):
        if not mig.is_file():
            logger.warning("breakfast live migration missing: %s", mig)
            continue
        sql = mig.read_text(encoding="utf-8")
        with engine.begin() as conn:
            conn.execute(text(sql))
    _ENSURED = True
    logger.info("breakfast_live_signals + breakfast_session_lock tables ensured")


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
    *,
    capture_source: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Build insert payloads from a locked live-state dict (pure, no DB)."""
    session_date = str(state.get("session_date") or "")[:10]
    if not session_date:
        return []

    locked_at = _parse_ts(state.get("server_time")) or datetime.now(IST)
    nifty_pct = (state.get("nifty") or {}).get("bias_pct")
    cap_src = capture_source or state.get("capture_source")
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
                    "capture_source": cap_src,
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
                websocket_rest_cross_check_status, instrument_key, capture_source
            ) VALUES (
                CAST(:session_date AS date), :symbol, :direction, :sector, :sector_rank, :rank_at_lock,
                :nifty_bias_pct, :sector_move_pct, :stock_move_pct_at_lock, :ltp_at_lock,
                :anchor_price, :tp_price, :sl_price, :lot_size,
                CAST(:locked_at_timestamp AS timestamptz),
                :websocket_rest_cross_check_status, :instrument_key, :capture_source
            )
            """
        ),
        row,
    )
    return True


def persist_live_signals(
    state: Dict[str, Any],
    cross_check_status: str,
    *,
    capture_source: Optional[str] = None,
) -> Dict[str, int]:
    """Idempotent insert for all stocks in locked state (up to 6 rows)."""
    status = str(cross_check_status or "matched").strip().lower()
    if status not in ("matched", "mismatched"):
        status = "matched"
    rows = rows_from_live_state(state, status, capture_source=capture_source)
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


def live_state_from_persisted_rows(session_date: str, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Reconstruct locked live-state dict from breakfast_live_signals rows."""
    sd = str(session_date or "")[:10]
    if not sd or not rows:
        return {}

    nifty_pct = rows[0].get("nifty_bias_pct")
    long_side = True
    if nifty_pct is not None:
        long_side = float(nifty_pct) >= 0

    nifty: Dict[str, Any] = {
        "bias": "positive" if long_side else "negative",
        "bias_pct": round(float(nifty_pct), 3) if nifty_pct is not None else None,
        "direction": "LONG" if long_side else "SHORT",
    }

    sectors_map: Dict[int, Dict[str, Any]] = {}
    for r in rows:
        sr = int(r.get("sector_rank") or 0)
        if sr < 1:
            continue
        if sr not in sectors_map:
            sectors_map[sr] = {
                "sector_key": "",
                "sector_label": str(r.get("sector") or "").strip(),
                "sector_rank": sr,
                "move_pct": round(float(r.get("sector_move_pct") or 0), 3),
                "direction": str(r.get("direction") or "LONG").strip().upper(),
                "stocks": [],
            }
        lot = int(r.get("lot_size") or 0)
        anchor = float(r.get("anchor_price") or 0)
        sl = float(r.get("sl_price") or 0)
        tp = float(r.get("tp_price") or 0)
        risk_inr = round(abs(anchor - sl) * lot, 2) if lot > 0 else None
        move = r.get("stock_move_pct_at_lock")
        rank = int(r.get("rank_at_lock") or 0)
        sym = str(r.get("symbol") or "").strip().upper()
        dir_u = str(r.get("direction") or "").strip().upper()
        sectors_map[sr]["stocks"].append(
            {
                "rank_label": str(rank),
                "stock_rank": rank,
                "rank_in_sector": rank,
                "symbol": sym,
                "display_symbol": sym,
                "direction": dir_u,
                "move_pct_at_entry": round(float(move), 3) if move is not None else None,
                "ltp": r.get("ltp_at_lock"),
                "lot_size": lot,
                "anchor_price": anchor,
                "sl_price": sl,
                "tp_price": tp,
                "risk_inr": risk_inr,
                "risk_inr_1lot": risk_inr,
                "instrument_key": r.get("instrument_key"),
            }
        )

    sectors = sorted(sectors_map.values(), key=lambda s: s["sector_rank"])
    locked_at = rows[0].get("locked_at_timestamp")
    server_time = locked_at.isoformat() if hasattr(locked_at, "isoformat") else str(locked_at or "")
    if not server_time:
        server_time = datetime.now(IST).isoformat()

    return {
        "ok": True,
        "state": "locked",
        "phase": "frozen",
        "session_date": sd,
        "banner": "LOCKED — 9:20 CONFIRMED",
        "server_time": server_time,
        "refresh_allowed": False,
        "poll_interval_sec": 0,
        "nifty": nifty,
        "sectors": sectors,
        "ranked_sector_count": len(sectors),
        "mismatch_instruments": [],
        "universe_instruments": 0,
        "from_persisted": True,
    }


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


def fetch_session_lock(session_date: str) -> Optional[Dict[str, Any]]:
    ensure_breakfast_live_signals_table()
    sd = str(session_date or "")[:10]
    db = SessionLocal()
    try:
        row = db.execute(
            text("SELECT * FROM breakfast_session_lock WHERE session_date = CAST(:sd AS date)"),
            {"sd": sd},
        ).mappings().first()
        return dict(row) if row else None
    finally:
        db.close()


def is_session_locked(session_date: str) -> bool:
    row = fetch_session_lock(session_date)
    return bool(row and str(row.get("lock_status") or "").lower() == "locked")


def persist_session_lock(
    state: Dict[str, Any],
    *,
    lock_status: str,
    failure_reason: Optional[str] = None,
    signal_count: int = 0,
    capture_source: str = "live_scheduler",
    locked_by: str = "auto",
) -> Dict[str, Any]:
    ensure_breakfast_live_signals_table()
    sd = str(state.get("session_date") or "")[:10]
    locked_at = _parse_ts(state.get("server_time")) or datetime.now(IST)
    status = str(lock_status or "locked").strip().lower()
    if status not in ("locked", "failed"):
        status = "locked"
    payload_json = json.dumps(state, default=str)
    db = SessionLocal()
    try:
        row = db.execute(
            text(
                """
                INSERT INTO breakfast_session_lock (
                    session_date, locked_at, locked_by, lock_status, failure_reason,
                    signal_count, capture_source, payload_json, updated_at
                ) VALUES (
                    CAST(:sd AS date), CAST(:locked_at AS timestamptz), :locked_by, :lock_status,
                    :failure_reason, :signal_count, :capture_source, CAST(:payload_json AS jsonb), NOW()
                )
                ON CONFLICT (session_date) DO UPDATE SET
                    locked_at = EXCLUDED.locked_at,
                    locked_by = EXCLUDED.locked_by,
                    lock_status = EXCLUDED.lock_status,
                    failure_reason = EXCLUDED.failure_reason,
                    signal_count = EXCLUDED.signal_count,
                    capture_source = EXCLUDED.capture_source,
                    payload_json = EXCLUDED.payload_json,
                    updated_at = NOW()
                RETURNING *
                """
            ),
            {
                "sd": sd,
                "locked_at": locked_at.isoformat(),
                "locked_by": locked_by,
                "lock_status": status,
                "failure_reason": failure_reason,
                "signal_count": int(signal_count),
                "capture_source": capture_source,
                "payload_json": payload_json,
            },
        ).mappings().first()
        db.commit()
        return dict(row) if row else {}
    except Exception:
        db.rollback()
        raise
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

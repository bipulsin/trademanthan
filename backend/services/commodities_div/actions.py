"""Take Trade / Exit submit + list APIs for Commodities Div."""
from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import pytz
from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.commodities_div.mapping import attach_instrument_fields
from backend.services.commodities_div.schema import ensure_commodities_div_tables
from backend.services.commodities_div.webhook import (
    STATUS_ACTIVATED,
    STATUS_EXIT_TRADE,
    STATUS_HISTORY,
    STATUS_IN_TRADE,
    now_ist_second,
)
from backend.services.ist_datetime import naive_ist
from backend.services.rule27_trade_log import ensure_trade_log_table, upsert_trade

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")


def _fmt_dt(v: Any) -> Optional[str]:
    if v is None:
        return None
    if isinstance(v, datetime):
        return naive_ist(v).strftime("%Y-%m-%d %H:%M:%S") if v.tzinfo else v.replace(microsecond=0).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    s = str(v)
    return s[:19] if len(s) >= 19 else s


def serialize_signal(row: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(row)
    for k in (
        "div_received_at",
        "div_tv_time_ist",
        "go_received_at",
        "go_tv_time_ist",
        "trade_taken_at",
        "ltp_updated_at",
        "exit_signal_received_at",
        "exit_tv_time_ist",
        "exit_submitted_at",
        "exit_at",
        "created_at",
        "updated_at",
    ):
        if k in out:
            out[k] = _fmt_dt(out.get(k))
    return out


def get_active_signal() -> Optional[Dict[str, Any]]:
    ensure_commodities_div_tables()
    db = SessionLocal()
    try:
        row = db.execute(
            text(
                """
                SELECT * FROM commodities_div_signals
                WHERE status <> 'History'
                ORDER BY id DESC LIMIT 1
                """
            )
        ).mappings().first()
        return serialize_signal(dict(row)) if row else None
    finally:
        db.close()


def list_history(limit: int = 100) -> List[Dict[str, Any]]:
    ensure_commodities_div_tables()
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT * FROM commodities_div_signals
                WHERE status = 'History'
                ORDER BY exit_submitted_at DESC NULLS LAST, id DESC
                LIMIT :lim
                """
            ),
            {"lim": int(limit)},
        ).mappings().all()
        return [serialize_signal(dict(r)) for r in rows]
    finally:
        db.close()


def take_trade(*, signal_id: int, entry_price: float) -> Dict[str, Any]:
    ensure_commodities_div_tables()
    if entry_price is None or float(entry_price) <= 0:
        raise ValueError("entry_price must be > 0")
    now = now_ist_second()
    db = SessionLocal()
    try:
        row = db.execute(
            text("SELECT * FROM commodities_div_signals WHERE id = :id FOR UPDATE"),
            {"id": int(signal_id)},
        ).mappings().first()
        if not row:
            raise ValueError("signal not found")
        if row["status"] != STATUS_ACTIVATED:
            raise ValueError(f"Take Trade only when Activated (got {row['status']})")

        # Try refresh instrument if missing
        ik = row.get("instrument_key")
        contract = row.get("contract")
        lot = row.get("lot_size")
        if not ik:
            inst = attach_instrument_fields(str(row["symbol_raw"]))
            ik = inst.get("instrument_key")
            contract = contract or inst.get("contract")
            lot = lot or inst.get("lot_size")

        db.execute(
            text(
                """
                UPDATE commodities_div_signals SET
                    status = 'In-Trade',
                    trade_taken_at = :tta,
                    entry_price = :ep,
                    instrument_key = COALESCE(:ik, instrument_key),
                    contract = COALESCE(:contract, contract),
                    lot_size = COALESCE(:lot, lot_size),
                    updated_at = NOW()
                WHERE id = :id
                """
            ),
            {
                "tta": now,
                "ep": float(entry_price),
                "ik": ik,
                "contract": contract,
                "lot": lot,
                "id": int(signal_id),
            },
        )
        db.commit()
        updated = db.execute(
            text("SELECT * FROM commodities_div_signals WHERE id = :id"),
            {"id": int(signal_id)},
        ).mappings().first()
        out = serialize_signal(dict(updated))
        out["play_trade_audio"] = True
        out["mapping_warning"] = None if ik else (
            "No mapping/instrument_key — LTP will not update until mapping + Upstox resolve succeed"
        )
        return out
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def _parse_exit_at(raw: Any) -> datetime:
    if isinstance(raw, datetime):
        return naive_ist(raw) if raw.tzinfo else raw.replace(microsecond=0)
    s = str(raw or "").strip().replace("T", " ")
    if len(s) == 16:  # YYYY-MM-DD HH:MM
        s = s + ":00"
    s = s[:19]
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    raise ValueError("exit_at must be YYYY-MM-DD HH:MM:SS")


def exit_submit(
    *,
    signal_id: int,
    exit_price: float,
    exit_at: Any,
) -> Dict[str, Any]:
    ensure_commodities_div_tables()
    if exit_price is None or float(exit_price) <= 0:
        raise ValueError("exit_price must be > 0")
    exit_dt = _parse_exit_at(exit_at)
    now = now_ist_second()
    db = SessionLocal()
    try:
        row = db.execute(
            text("SELECT * FROM commodities_div_signals WHERE id = :id FOR UPDATE"),
            {"id": int(signal_id)},
        ).mappings().first()
        if not row:
            raise ValueError("signal not found")
        if row["status"] != STATUS_EXIT_TRADE:
            raise ValueError(f"Exit submit only when Exit Trade (got {row['status']})")
        if row.get("entry_price") is None or row.get("trade_taken_at") is None:
            raise ValueError("missing entry data")

        direction_tv = str(row["direction"])
        tl_direction = "LONG" if direction_tv == "BULL" else "SHORT"
        entry_dt = row["trade_taken_at"]
        if isinstance(entry_dt, datetime):
            entry_dt = naive_ist(entry_dt) if entry_dt.tzinfo else entry_dt
        session_date = entry_dt.date() if isinstance(entry_dt, datetime) else now.date()
        qty = int(row["lot_size"]) if row.get("lot_size") else 1

        notes_obj = {
            "commodities_div_signal_id": int(row["id"]),
            "symbol_raw": row.get("symbol_raw"),
            "direction_tv": direction_tv,
            "div_received_at": _fmt_dt(row.get("div_received_at")),
            "div_tv_time_ist": _fmt_dt(row.get("div_tv_time_ist")),
            "go_received_at": _fmt_dt(row.get("go_received_at")),
            "go_tv_time_ist": _fmt_dt(row.get("go_tv_time_ist")),
            "exit_signal_received_at": _fmt_dt(row.get("exit_signal_received_at")),
            "exit_tv_time_ist": _fmt_dt(row.get("exit_tv_time_ist")),
            "ltp_at_exit_submit": row.get("ltp"),
        }
        ensure_trade_log_table()
        trade_log_id = upsert_trade(
            db,
            {
                "session_date": str(session_date),
                "symbol": str(row["symbol_mapped"]).strip().upper(),
                "contract": row.get("contract"),
                "direction": tl_direction,
                "qty": qty,
                "entry_time": entry_dt.strftime("%H:%M:%S") if isinstance(entry_dt, datetime) else str(entry_dt),
                "entry_price": float(row["entry_price"]),
                "exit_time": exit_dt.strftime("%H:%M:%S"),
                "exit_price": float(exit_price),
                "source": "commodities_div",
                "notes": json.dumps(notes_obj),
                "exit_trigger": "commodities_div_exit_modal",
                "exit_trigger_type": "discretionary",
                "garuda_confluence": "NOT_AVAILABLE",
            },
        )

        db.execute(
            text(
                """
                UPDATE commodities_div_signals SET
                    status = 'History',
                    exit_submitted_at = :sub,
                    exit_price = :xp,
                    exit_at = :xa,
                    trade_log_id = :tlid,
                    updated_at = NOW()
                WHERE id = :id
                """
            ),
            {
                "sub": now,
                "xp": float(exit_price),
                "xa": exit_dt,
                "tlid": int(trade_log_id),
                "id": int(signal_id),
            },
        )
        db.commit()
        updated = db.execute(
            text("SELECT * FROM commodities_div_signals WHERE id = :id"),
            {"id": int(signal_id)},
        ).mappings().first()
        out = serialize_signal(dict(updated))
        out["trade_log_id"] = int(trade_log_id)
        return out
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def workspace_payload() -> Dict[str, Any]:
    active = get_active_signal()
    return {
        "ok": True,
        "active": active,
        "history": list_history(100),
        "server_time_ist": now_ist_second().strftime("%Y-%m-%d %H:%M:%S"),
        "mappings": None,  # filled by router when needed
    }

"""CRUD + refresh for vajra_discretionary_trade."""
from __future__ import annotations

import json
import logging
from datetime import date, datetime
from typing import Any, Dict, List, Optional

import pytz
from sqlalchemy import text
from sqlalchemy.orm import Session

from backend.database import SessionLocal
from backend.services.smart_futures_session_date import effective_session_date_ist_for_trend
from backend.services.vajra.discretionary import build_validation_preview, refresh_trade_state
from backend.services.vajra.trade_tables import ensure_vajra_discretionary_tables, row_to_dict

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")


def _jdump(obj: Any) -> str:
    return json.dumps(obj, default=str)


def list_trades(
    user_id: int,
    *,
    status: str = "active",
    session_date: Optional[date] = None,
    platform: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Active trades: all open rows for the user (any session_date).
    Closed trades: default to today's IST session_date unless session_date is passed.
    """
    db = SessionLocal()
    try:
        ensure_vajra_discretionary_tables(db)
        q = """
            SELECT * FROM vajra_discretionary_trade
            WHERE user_id = :uid AND status = :st
        """
        params: Dict[str, Any] = {"uid": user_id, "st": status}
        if status == "closed":
            sd = session_date or effective_session_date_ist_for_trend()
            q += " AND session_date = :sd"
            params["sd"] = sd
        elif session_date is not None:
            q += " AND session_date = :sd"
            params["sd"] = session_date
        if platform:
            q += " AND platform = :pf"
            params["pf"] = platform
        q += " ORDER BY created_at DESC"
        rows = db.execute(text(q), params).fetchall()
        return [row_to_dict(r) for r in rows]
    finally:
        db.close()


def get_trade(user_id: int, trade_id: int) -> Optional[Dict[str, Any]]:
    db = SessionLocal()
    try:
        ensure_vajra_discretionary_tables(db)
        r = db.execute(
            text(
                "SELECT * FROM vajra_discretionary_trade WHERE id = :id AND user_id = :uid"
            ),
            {"id": trade_id, "uid": user_id},
        ).fetchone()
        return row_to_dict(r) if r else None
    finally:
        db.close()


def validation_preview(
    user_id: int,
    *,
    stock: str,
    direction: str,
    instrument_key: str,
    discovery_row: Dict[str, Any],
) -> Dict[str, Any]:
    del user_id
    return build_validation_preview(
        stock=stock,
        direction=direction,
        instrument_key=instrument_key,
        discovery_row=discovery_row,
    )


def activate_trade(
    user_id: int,
    *,
    platform: str,
    stock: str,
    future_symbol: str,
    instrument_key: str,
    direction: str,
    entry_price: float,
    lots: int,
    entry_time: datetime,
    discovery_row: Dict[str, Any],
    checklist: Dict[str, Any],
    metrics: Dict[str, Any],
    warnings: List[str],
) -> Dict[str, Any]:
    sd = effective_session_date_ist_for_trend()
    now = datetime.now(IST)
    db = SessionLocal()
    try:
        ensure_vajra_discretionary_tables(db)
        ins = db.execute(
            text(
                """
                INSERT INTO vajra_discretionary_trade (
                    user_id, platform, session_date, stock, future_symbol, instrument_key,
                    direction, lots, entry_price, entry_time, status, current_price,
                    discovery_snapshot, checklist, metrics_at_entry, warnings_at_entry,
                    lifecycle_state, trade_health, lifecycle_history, journal, created_at, updated_at
                ) VALUES (
                    :user_id, :platform, :sd, :stock, :fs, :ik,
                    :direction, :lots, :entry_price, :entry_time, 'active', :entry_price,
                    CAST(:disc AS jsonb), CAST(:chk AS jsonb), CAST(:met AS jsonb), CAST(:warn AS jsonb),
                    'Early Transition', 50, '[]'::jsonb, '{}'::jsonb, :now, :now
                )
                RETURNING id
                """
            ),
            {
                "user_id": user_id,
                "platform": platform,
                "sd": sd,
                "stock": stock,
                "fs": future_symbol,
                "ik": instrument_key,
                "direction": direction.upper(),
                "lots": max(1, int(lots)),
                "entry_price": float(entry_price),
                "entry_time": entry_time,
                "disc": _jdump(discovery_row),
                "chk": _jdump(checklist),
                "met": _jdump(metrics),
                "warn": _jdump(warnings),
                "now": now,
            },
        )
        tid = ins.fetchone()[0]
        db.commit()
        return get_trade(user_id, int(tid)) or {}
    except Exception as e:
        db.rollback()
        logger.exception("activate_trade: %s", e)
        raise
    finally:
        db.close()


def close_trade(
    user_id: int,
    trade_id: int,
    *,
    exit_price: float,
    exit_time: datetime,
    exit_reasons: List[str],
) -> Optional[Dict[str, Any]]:
    trade = get_trade(user_id, trade_id)
    if not trade or trade.get("status") != "active":
        return None
    entry = float(trade.get("entry_price") or 0)
    lots = int(trade.get("lots") or 1)
    bull = str(trade.get("direction") or "").upper().startswith("L")
    mult = 1 if bull else -1
    pnl = (float(exit_price) - entry) * lots * mult
    journal = dict(trade.get("journal") or {})
    journal["exit_reasons"] = exit_reasons
    journal["checklist_at_entry"] = trade.get("checklist")
    journal["warnings_during"] = trade.get("alerts")
    journal["lifecycle_transitions"] = trade.get("lifecycle_history")
    now = datetime.now(IST)
    db = SessionLocal()
    try:
        db.execute(
            text(
                """
                UPDATE vajra_discretionary_trade SET
                    status = 'closed',
                    exit_price = :xp,
                    exit_time = :xt,
                    exit_reasons = CAST(:er AS jsonb),
                    realized_pnl = :pnl,
                    journal = CAST(:jr AS jsonb),
                    closed_at = :now,
                    updated_at = :now
                WHERE id = :id AND user_id = :uid
                """
            ),
            {
                "xp": float(exit_price),
                "xt": exit_time,
                "er": _jdump(exit_reasons),
                "pnl": pnl,
                "jr": _jdump(journal),
                "now": now,
                "id": trade_id,
                "uid": user_id,
            },
        )
        db.commit()
        return get_trade(user_id, trade_id)
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def refresh_all_active_trades() -> int:
    """Scheduler: refresh every active trade (all users)."""
    db = SessionLocal()
    n = 0
    try:
        ensure_vajra_discretionary_tables(db)
        rows = db.execute(
            text("SELECT * FROM vajra_discretionary_trade WHERE status = 'active'")
        ).fetchall()
        for row in rows:
            trade = row_to_dict(row)
            upd = refresh_trade_state(trade)
            db.execute(
                text(
                    """
                    UPDATE vajra_discretionary_trade SET
                        current_price = COALESCE(:cp, current_price),
                        trade_health = :th,
                        lifecycle_state = :lc,
                        structure_status = :ss,
                        momentum_status = :ms,
                        ema_status = :es,
                        vwap_status = :vs,
                        alerts = CAST(:al AS jsonb),
                        lifecycle_history = CAST(:lh AS jsonb),
                        updated_at = NOW()
                    WHERE id = :id
                    """
                ),
                {
                    "cp": upd.get("current_price"),
                    "th": upd.get("trade_health"),
                    "lc": upd.get("lifecycle_state"),
                    "ss": upd.get("structure_status"),
                    "ms": upd.get("momentum_status"),
                    "es": upd.get("ema_status"),
                    "vs": upd.get("vwap_status"),
                    "al": _jdump(upd.get("alerts") or []),
                    "lh": _jdump(upd.get("lifecycle_history") or []),
                    "id": trade["id"],
                },
            )
            n += 1
        db.commit()
    except Exception as e:
        db.rollback()
        logger.error("refresh_all_active_trades: %s", e)
    finally:
        db.close()
    return n


def persist_refresh(user_id: int, trade_id: int) -> Optional[Dict[str, Any]]:
    trade = get_trade(user_id, trade_id)
    if not trade or trade.get("status") != "active":
        return trade
    upd = refresh_trade_state(trade)
    db = SessionLocal()
    try:
        db.execute(
            text(
                """
                UPDATE vajra_discretionary_trade SET
                    current_price = COALESCE(:cp, current_price),
                    trade_health = :th,
                    lifecycle_state = :lc,
                    structure_status = :ss,
                    momentum_status = :ms,
                    ema_status = :es,
                    vwap_status = :vs,
                    alerts = CAST(:al AS jsonb),
                    lifecycle_history = CAST(:lh AS jsonb),
                    updated_at = NOW()
                WHERE id = :id AND user_id = :uid
                """
            ),
            {
                "cp": upd.get("current_price"),
                "th": upd.get("trade_health"),
                "lc": upd.get("lifecycle_state"),
                "ss": upd.get("structure_status"),
                "ms": upd.get("momentum_status"),
                "es": upd.get("ema_status"),
                "vs": upd.get("vwap_status"),
                "al": _jdump(upd.get("alerts") or []),
                "lh": _jdump(upd.get("lifecycle_history") or []),
                "id": trade_id,
                "uid": user_id,
            },
        )
        db.commit()
    finally:
        db.close()
    out = get_trade(user_id, trade_id) or {}
    out["unrealized_pnl"] = upd.get("unrealized_pnl")
    out["trade_health_label"] = upd.get("trade_health_label")
    return out

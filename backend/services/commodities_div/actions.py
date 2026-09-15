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

EDITABLE_STATUSES = {STATUS_IN_TRADE, STATUS_EXIT_TRADE}
EXITABLE_STATUSES = {STATUS_IN_TRADE, STATUS_EXIT_TRADE}


def _sync_ws_ltp_best_effort() -> None:
    """Subscribe/unsubscribe In-Trade keys on the shared Upstox feed (non-fatal)."""
    try:
        from backend.services.commodities_div.ws_ltp import sync_in_trade_subscriptions

        sync_in_trade_subscriptions(force=True)
    except Exception as e:
        logger.debug("commodities_div ws_ltp sync after mutation failed: %s", e)


def _fmt_dt(v: Any) -> Optional[str]:
    if v is None:
        return None
    if isinstance(v, datetime):
        return naive_ist(v).strftime("%Y-%m-%d %H:%M:%S") if v.tzinfo else v.replace(microsecond=0).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    s = str(v)
    return s[:19] if len(s) >= 19 else s


def _lot_qty(row: Dict[str, Any]) -> int:
    """One lot quantity for the symbol (Upstox lot_size); fallback 1."""
    try:
        lot = int(row.get("lot_size") or 0)
    except (TypeError, ValueError):
        lot = 0
    return lot if lot > 0 else 1


def compute_pnl(
    *,
    direction: Any,
    entry_price: Any,
    mark_price: Any,
    lot_size: Any = None,
) -> Optional[float]:
    """
    BULL: (mark − entry) × lot
    BEAR: (entry − mark) × lot
    mark = LTP (In-Trade) or exit price (History / when set).
    """
    try:
        entry = float(entry_price) if entry_price is not None else None
        mark = float(mark_price) if mark_price is not None else None
    except (TypeError, ValueError):
        return None
    if entry is None or mark is None:
        return None
    lot = 1
    try:
        lot = int(lot_size or 0) or 1
    except (TypeError, ValueError):
        lot = 1
    dir_u = str(direction or "").upper()
    if dir_u == "BEAR":
        return round((entry - mark) * lot, 2)
    return round((mark - entry) * lot, 2)


def _mark_for_pnl(row: Dict[str, Any], *, prefer_exit: bool = False) -> Any:
    """In-Trade: exit_price if set else LTP. History: exit_price."""
    if prefer_exit or str(row.get("status") or "") == STATUS_HISTORY:
        return row.get("exit_price")
    if row.get("exit_price") is not None:
        return row.get("exit_price")
    return row.get("ltp")


def serialize_signal(row: Dict[str, Any], *, prefer_exit_pnl: bool = False) -> Dict[str, Any]:
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
    if not out.get("trade_mode"):
        out["trade_mode"] = "PAPER"
    status = str(out.get("status") or "")
    prefer_exit = prefer_exit_pnl or status == STATUS_HISTORY
    mark = _mark_for_pnl(out, prefer_exit=prefer_exit)
    out["pnl"] = compute_pnl(
        direction=out.get("direction"),
        entry_price=out.get("entry_price"),
        mark_price=mark,
        lot_size=out.get("lot_size"),
    )
    out["lot_qty"] = _lot_qty(out)
    return out


def get_active_signal() -> Optional[Dict[str, Any]]:
    """Most recent Active-tab row (compat). Prefer list_active_tab_signals()."""
    rows = list_active_tab_signals()
    return rows[0] if rows else None


def list_active_signals() -> List[Dict[str, Any]]:
    """All non-History cycles (compat — Active + In-Trade + Exit Trade)."""
    ensure_commodities_div_tables()
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT * FROM commodities_div_signals
                WHERE status <> 'History'
                ORDER BY id DESC
                """
            )
        ).mappings().all()
        return [serialize_signal(dict(r)) for r in rows]
    finally:
        db.close()


def list_active_tab_signals() -> List[Dict[str, Any]]:
    """Active tab: Divergence / Activated / Exit Trade (not In-Trade, not History)."""
    ensure_commodities_div_tables()
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT * FROM commodities_div_signals
                WHERE status IN ('Divergence', 'Activated', 'Exit Trade')
                ORDER BY id DESC
                """
            )
        ).mappings().all()
        return [serialize_signal(dict(r)) for r in rows]
    finally:
        db.close()


def list_in_trade_signals() -> List[Dict[str, Any]]:
    """In Trade tab: status = In-Trade only."""
    ensure_commodities_div_tables()
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT * FROM commodities_div_signals
                WHERE status = 'In-Trade'
                ORDER BY trade_taken_at DESC NULLS LAST, id DESC
                """
            )
        ).mappings().all()
        return [serialize_signal(dict(r)) for r in rows]
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
        return [serialize_signal(dict(r), prefer_exit_pnl=True) for r in rows]
    finally:
        db.close()


def take_trade(*, signal_id: int, entry_price: float, trade_mode: Optional[str] = None) -> Dict[str, Any]:
    ensure_commodities_div_tables()
    if entry_price is None or float(entry_price) <= 0:
        raise ValueError("entry_price must be > 0")
    mode = _normalize_trade_mode(trade_mode) if trade_mode else "PAPER"
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
                    trade_mode = :mode,
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
                "mode": mode,
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
            "Upstox front-month FUT not resolved yet — LTP will retry on the 10-min sidecar"
        )
        _sync_ws_ltp_best_effort()
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


def _parse_entry_at(raw: Any) -> datetime:
    return _parse_exit_at(raw)


def _normalize_trade_mode(raw: Any) -> str:
    mode = str(raw or "PAPER").strip().upper()
    if mode not in ("PAPER", "LIVE"):
        raise ValueError("trade_mode must be PAPER or LIVE")
    return mode


def _normalize_direction(raw: Any) -> str:
    d = str(raw or "").strip().upper()
    if d not in ("BULL", "BEAR"):
        raise ValueError("direction must be BULL or BEAR")
    return d


def update_signal(
    *,
    signal_id: int,
    entry_price: Optional[float] = None,
    trade_taken_at: Any = None,
    direction: Optional[str] = None,
    exit_price: Optional[float] = None,
    exit_at: Any = None,
    trade_mode: Optional[str] = None,
) -> Dict[str, Any]:
    """Discretionary edit for In-Trade (or Exit Trade) rows — does not move to History."""
    ensure_commodities_div_tables()
    db = SessionLocal()
    try:
        row = db.execute(
            text("SELECT * FROM commodities_div_signals WHERE id = :id FOR UPDATE"),
            {"id": int(signal_id)},
        ).mappings().first()
        if not row:
            raise ValueError("signal not found")
        if row["status"] not in EDITABLE_STATUSES:
            raise ValueError(f"Edit only when In-Trade or Exit Trade (got {row['status']})")

        sets: List[str] = ["updated_at = NOW()"]
        params: Dict[str, Any] = {"id": int(signal_id)}

        if entry_price is not None:
            if float(entry_price) <= 0:
                raise ValueError("entry_price must be > 0")
            sets.append("entry_price = :ep")
            params["ep"] = float(entry_price)

        if trade_taken_at is not None and str(trade_taken_at).strip():
            entry_dt = _parse_entry_at(trade_taken_at)
            sets.append("trade_taken_at = :tta")
            params["tta"] = entry_dt

        if direction is not None:
            sets.append("direction = :dir")
            params["dir"] = _normalize_direction(direction)

        if exit_price is not None:
            if float(exit_price) <= 0:
                raise ValueError("exit_price must be > 0")
            sets.append("exit_price = :xp")
            params["xp"] = float(exit_price)

        if exit_at is not None and str(exit_at).strip():
            exit_dt = _parse_exit_at(exit_at)
            sets.append("exit_at = :xa")
            params["xa"] = exit_dt

        if trade_mode is not None:
            sets.append("trade_mode = :mode")
            params["mode"] = _normalize_trade_mode(trade_mode)

        if len(sets) == 1:
            raise ValueError("no fields to update")

        db.execute(
            text(f"UPDATE commodities_div_signals SET {', '.join(sets)} WHERE id = :id"),
            params,
        )
        db.commit()
        updated = db.execute(
            text("SELECT * FROM commodities_div_signals WHERE id = :id"),
            {"id": int(signal_id)},
        ).mappings().first()
        return serialize_signal(dict(updated))
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def delete_in_trade(*, signal_id: int) -> Dict[str, Any]:
    """
    Remove an In-Trade row. Webhook raw log is retained; clear active_signal_id refs.
    """
    ensure_commodities_div_tables()
    db = SessionLocal()
    try:
        row = db.execute(
            text("SELECT * FROM commodities_div_signals WHERE id = :id FOR UPDATE"),
            {"id": int(signal_id)},
        ).mappings().first()
        if not row:
            raise ValueError("signal not found")
        if row["status"] != STATUS_IN_TRADE:
            raise ValueError(f"Delete only when In-Trade (got {row['status']})")

        snap = serialize_signal(dict(row))
        db.execute(
            text(
                """
                UPDATE commodities_div_webhook_log
                SET active_signal_id = NULL
                WHERE active_signal_id = :id
                """
            ),
            {"id": int(signal_id)},
        )
        db.execute(
            text("DELETE FROM commodities_div_signals WHERE id = :id"),
            {"id": int(signal_id)},
        )
        db.commit()
        _sync_ws_ltp_best_effort()
        return {"ok": True, "deleted_id": int(signal_id), "signal": snap}
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def exit_submit(
    *,
    signal_id: int,
    exit_price: float,
    exit_at: Any,
    entry_price: Optional[float] = None,
    trade_taken_at: Any = None,
    direction: Optional[str] = None,
    trade_mode: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Move In-Trade or Exit Trade → History and write trade_log.
    Optional discretionary overrides for entry/side/mode applied before close.
    """
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
        if row["status"] not in EXITABLE_STATUSES:
            raise ValueError(f"Exit submit only when In-Trade or Exit Trade (got {row['status']})")

        # Apply discretionary overrides before logging
        entry_px = float(entry_price) if entry_price is not None else row.get("entry_price")
        if entry_px is None or float(entry_px) <= 0:
            raise ValueError("missing entry data")
        direction_tv = _normalize_direction(direction) if direction else str(row["direction"])
        mode = _normalize_trade_mode(trade_mode) if trade_mode else (row.get("trade_mode") or "PAPER")

        if trade_taken_at is not None and str(trade_taken_at).strip():
            entry_dt = _parse_entry_at(trade_taken_at)
        else:
            entry_dt = row.get("trade_taken_at")
            if isinstance(entry_dt, datetime):
                entry_dt = naive_ist(entry_dt) if entry_dt.tzinfo else entry_dt
        if entry_dt is None:
            raise ValueError("missing entry data")

        tl_direction = "LONG" if direction_tv == "BULL" else "SHORT"
        session_date = entry_dt.date() if isinstance(entry_dt, datetime) else now.date()
        qty = _lot_qty(dict(row))

        notes_obj = {
            "commodities_div_signal_id": int(row["id"]),
            "symbol_raw": row.get("symbol_raw"),
            "direction_tv": direction_tv,
            "trade_mode": mode,
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
                "entry_price": float(entry_px),
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
                    direction = :dir,
                    entry_price = :ep,
                    trade_taken_at = :tta,
                    trade_mode = :mode,
                    exit_submitted_at = :sub,
                    exit_price = :xp,
                    exit_at = :xa,
                    trade_log_id = :tlid,
                    updated_at = NOW()
                WHERE id = :id
                """
            ),
            {
                "dir": direction_tv,
                "ep": float(entry_px),
                "tta": entry_dt,
                "mode": mode,
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
        out = serialize_signal(dict(updated), prefer_exit_pnl=True)
        out["trade_log_id"] = int(trade_log_id)
        _sync_ws_ltp_best_effort()
        return out
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def workspace_payload() -> Dict[str, Any]:
    actives = list_active_tab_signals()
    in_trade = list_in_trade_signals()
    # Compat: "actives" was all non-History; keep both split + combined
    combined = actives + in_trade
    return {
        "ok": True,
        "active": actives,
        "actives": actives,
        "in_trade": in_trade,
        "all_open": combined,
        "history": list_history(100),
        "server_time_ist": now_ist_second().strftime("%Y-%m-%d %H:%M:%S"),
        "mappings": None,
    }

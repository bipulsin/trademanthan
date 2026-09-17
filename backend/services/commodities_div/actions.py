"""Take Trade / Exit submit + list APIs for Commodities Div."""
from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import pytz
from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.commodities_div.mapping import (
    attach_instrument_fields,
    display_symbol_for,
    normalize_tv_ticker,
)
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

EDITABLE_STATUSES = {STATUS_IN_TRADE, STATUS_EXIT_TRADE, STATUS_HISTORY}
EXITABLE_STATUSES = {STATUS_IN_TRADE, STATUS_EXIT_TRADE}
DELETABLE_STATUSES = {STATUS_IN_TRADE, STATUS_HISTORY}


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


def _ensure_resolved_instrument(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Fill missing contract / lot_size / instrument_key before trade_log write.

    Manual free-text entries saved before MCX parsing, or webhook rows that
    never got sidecar resolve, must not write symbol=COPPERSEPFUT qty=1.
    """
    out = dict(row)
    has_contract = bool(str(out.get("contract") or "").strip())
    has_ik = bool(str(out.get("instrument_key") or "").strip())
    try:
        lot_ok = int(out.get("lot_size") or 0) > 0
    except (TypeError, ValueError):
        lot_ok = False
    if has_contract and has_ik and lot_ok:
        return out

    raw = str(out.get("symbol_raw") or "").strip()
    mapped = str(out.get("symbol_mapped") or "").strip()
    if not raw and not mapped:
        return out
    try:
        inst = attach_instrument_fields(raw or mapped, resolve_contract=True)
    except Exception as e:
        logger.debug(
            "commodities_div exit resolve failed for %s: %s", raw or mapped, e
        )
        return out
    if not inst or not inst.get("instrument_key"):
        return out

    tsym = str(inst.get("trading_symbol") or inst.get("contract") or "").strip()
    if tsym:
        out["contract"] = tsym
    ik = str(inst.get("instrument_key") or "").strip()
    if ik:
        out["instrument_key"] = ik
    if inst.get("lot_size") is not None:
        try:
            lot_i = int(inst["lot_size"])
        except (TypeError, ValueError):
            lot_i = 0
        if lot_i > 0:
            out["lot_size"] = lot_i
    underlying = str(inst.get("symbol_mapped") or "").strip().upper()
    if underlying:
        out["symbol_mapped"] = underlying
    return out


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


def _backfill_contract_fields(out: Dict[str, Any]) -> None:
    """
    When DB row lacks contract / instrument_key, resolve MCX FUT from symbol_raw
    (honors TV month letter+year) for display and chart. Uses mapping resolve cache.
    """
    contract = str(out.get("contract") or "").strip()
    if contract:
        out["trading_symbol"] = contract
        return
    raw = str(out.get("symbol_raw") or "").strip()
    mapped = str(out.get("symbol_mapped") or "").strip()
    if not raw and not mapped:
        return
    try:
        # Prefer symbol_raw so NATURALGASV2026 → Oct FUT, not blind front-month.
        inst = attach_instrument_fields(raw or mapped, resolve_contract=True)
    except Exception as e:
        logger.debug(
            "commodities_div display resolve failed for %s: %s", raw or mapped, e
        )
        return
    if not inst or not inst.get("instrument_key"):
        return
    tsym = str(inst.get("trading_symbol") or inst.get("contract") or "").strip()
    if tsym:
        out["contract"] = tsym
        out["trading_symbol"] = tsym
    ik = str(inst.get("instrument_key") or "").strip()
    if ik and not str(out.get("instrument_key") or "").strip():
        out["instrument_key"] = ik
    if out.get("lot_size") is None and inst.get("lot_size") is not None:
        out["lot_size"] = inst.get("lot_size")


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
    _backfill_contract_fields(out)
    tsym = str(out.get("trading_symbol") or out.get("contract") or "").strip()
    if tsym:
        out["trading_symbol"] = tsym
        out["contract"] = out.get("contract") or tsym
    out["display_symbol"] = display_symbol_for(
        contract=out.get("contract"),
        trading_symbol=out.get("trading_symbol"),
        symbol_mapped=out.get("symbol_mapped"),
        symbol_raw=out.get("symbol_raw"),
    )
    out["mcx_symbol"] = out["display_symbol"]
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
    """Discretionary edit for In-Trade, Exit Trade, or History — does not change status."""
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
            raise ValueError(
                f"Edit only when In-Trade, Exit Trade, or History (got {row['status']})"
            )

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
        prefer_exit = str(updated["status"]) == STATUS_HISTORY
        return serialize_signal(dict(updated), prefer_exit_pnl=prefer_exit)
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def delete_signal(*, signal_id: int) -> Dict[str, Any]:
    """
    Remove an In-Trade or History row.

    Webhook raw log is retained (active_signal_id cleared). trade_log rows are left
    intact — no cascade delete — so integrity of the trade log is preserved.
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
        status = str(row["status"])
        if status not in DELETABLE_STATUSES:
            raise ValueError(f"Delete only when In-Trade or History (got {status})")

        prefer_exit = status == STATUS_HISTORY
        snap = serialize_signal(dict(row), prefer_exit_pnl=prefer_exit)
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
        if status == STATUS_IN_TRADE:
            _sync_ws_ltp_best_effort()
        return {"ok": True, "deleted_id": int(signal_id), "signal": snap}
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def delete_in_trade(*, signal_id: int) -> Dict[str, Any]:
    """Compat alias — prefer delete_signal()."""
    return delete_signal(signal_id=signal_id)


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
        # Re-resolve free-text / incomplete instrument before trade_log write so
        # LIVE exits never persist COPPERSEPFUT / qty=1 when MCX lot is known.
        resolved = _ensure_resolved_instrument(dict(row))
        qty = _lot_qty(resolved)
        tl_symbol = str(resolved.get("symbol_mapped") or row["symbol_mapped"]).strip().upper()
        tl_contract = resolved.get("contract") or row.get("contract")

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
        # PAPER stays on CommDiv History only — do not pollute trade_log /
        # reports / dashboard (mirror Premium Futures + Stock Options Selling).
        trade_log_id: Optional[int] = None
        if mode == "LIVE":
            ensure_trade_log_table()
            trade_log_id = upsert_trade(
                db,
                {
                    "session_date": str(session_date),
                    "symbol": tl_symbol,
                    "contract": tl_contract,
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
                    symbol_mapped = COALESCE(:mapped, symbol_mapped),
                    contract = COALESCE(:contract, contract),
                    lot_size = COALESCE(:lot, lot_size),
                    instrument_key = COALESCE(:ik, instrument_key),
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
                "tlid": int(trade_log_id) if trade_log_id is not None else None,
                "mapped": tl_symbol or None,
                "contract": tl_contract,
                "lot": resolved.get("lot_size"),
                "ik": resolved.get("instrument_key"),
                "id": int(signal_id),
            },
        )
        db.commit()
        updated = db.execute(
            text("SELECT * FROM commodities_div_signals WHERE id = :id"),
            {"id": int(signal_id)},
        ).mappings().first()
        out = serialize_signal(dict(updated), prefer_exit_pnl=True)
        if trade_log_id is not None:
            out["trade_log_id"] = int(trade_log_id)
        else:
            out["trade_log_id"] = None
        _sync_ws_ltp_best_effort()
        return out
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def create_manual_history(
    *,
    commodity: str,
    direction: str,
    entry_price: float,
    trade_taken_at: Any,
    exit_price: float,
    exit_at: Any,
    trade_mode: Optional[str] = None,
    qty: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Insert a completed History row without webhook / DIV / GO flow.

    Resolves MCX contract via mapping when the typed name matches an allowed
    underlying (optionally with month code). Unresolvable names still save with
    the typed symbol text so CommDiv History works.

    LIVE: also upsert trade_log (same conventions as exit_submit).
    PAPER: History only — never write trade_log.
    """
    symbol_raw = str(commodity or "").strip()
    if not symbol_raw:
        raise ValueError("commodity name is required")
    if entry_price is None or float(entry_price) <= 0:
        raise ValueError("entry_price must be > 0")
    if exit_price is None or float(exit_price) <= 0:
        raise ValueError("exit_price must be > 0")

    direction_tv = _normalize_direction(direction)
    mode = _normalize_trade_mode(trade_mode) if trade_mode else "PAPER"
    entry_dt = _parse_entry_at(trade_taken_at)
    exit_dt = _parse_exit_at(exit_at)

    ensure_commodities_div_tables()
    now = now_ist_second()

    try:
        inst = attach_instrument_fields(symbol_raw, resolve_contract=True)
    except Exception as e:
        logger.debug("commodities_div manual resolve failed for %s: %s", symbol_raw, e)
        inst = {}

    symbol_mapped = (
        str(inst.get("symbol_mapped") or "").strip()
        or normalize_tv_ticker(symbol_raw)
        or symbol_raw.upper()
    )
    instrument_key = inst.get("instrument_key")
    contract = inst.get("contract") or inst.get("trading_symbol")
    lot_size: Optional[int] = None
    if qty is not None:
        try:
            q = int(qty)
        except (TypeError, ValueError) as e:
            raise ValueError("qty must be a positive integer") from e
        if q <= 0:
            raise ValueError("qty must be a positive integer")
        lot_size = q
    elif inst.get("lot_size") is not None:
        try:
            lot_size = int(inst["lot_size"]) or None
        except (TypeError, ValueError):
            lot_size = None

    row_for_qty = {
        "lot_size": lot_size,
        "symbol_mapped": symbol_mapped,
        "symbol_raw": symbol_raw,
        "contract": contract,
    }
    tl_qty = _lot_qty(row_for_qty)
    tl_direction = "LONG" if direction_tv == "BULL" else "SHORT"
    session_date = entry_dt.date() if isinstance(entry_dt, datetime) else now.date()

    notes_obj = {
        "commodities_div_manual": True,
        "symbol_raw": symbol_raw,
        "direction_tv": direction_tv,
        "trade_mode": mode,
        "underlying_matched": bool(inst.get("underlying_matched")),
        "mapping_found": bool(inst.get("mapping_found")),
        "parse_mode": inst.get("parse_mode"),
        "match_mode": inst.get("match_mode"),
        "contract_month": inst.get("contract_month"),
        "contract_year": inst.get("contract_year"),
    }

    db = SessionLocal()
    try:
        rid = db.execute(
            text(
                """
                INSERT INTO commodities_div_signals (
                    symbol_raw, symbol_mapped, direction, status,
                    div_received_at, trade_taken_at, entry_price,
                    instrument_key, contract, lot_size,
                    exit_submitted_at, exit_price, exit_at,
                    trade_log_id, trade_mode, updated_at
                ) VALUES (
                    :symbol_raw, :symbol_mapped, :direction, 'History',
                    :div_received_at, :trade_taken_at, :entry_price,
                    :instrument_key, :contract, :lot_size,
                    :exit_submitted_at, :exit_price, :exit_at,
                    NULL, :trade_mode, NOW()
                )
                RETURNING id
                """
            ),
            {
                "symbol_raw": symbol_raw,
                "symbol_mapped": symbol_mapped,
                "direction": direction_tv,
                "div_received_at": entry_dt,
                "trade_taken_at": entry_dt,
                "entry_price": float(entry_price),
                "instrument_key": instrument_key,
                "contract": contract,
                "lot_size": lot_size,
                "exit_submitted_at": now,
                "exit_price": float(exit_price),
                "exit_at": exit_dt,
                "trade_mode": mode,
            },
        ).scalar()
        signal_id = int(rid)

        # PAPER stays on CommDiv History only — do not pollute trade_log /
        # reports / dashboard (same rule as exit_submit).
        trade_log_id: Optional[int] = None
        if mode == "LIVE":
            ensure_trade_log_table()
            notes_obj["commodities_div_signal_id"] = signal_id
            trade_log_id = upsert_trade(
                db,
                {
                    "session_date": str(session_date),
                    "symbol": str(symbol_mapped).strip().upper(),
                    "contract": contract,
                    "direction": tl_direction,
                    "qty": tl_qty,
                    "entry_time": entry_dt.strftime("%H:%M:%S"),
                    "entry_price": float(entry_price),
                    "exit_time": exit_dt.strftime("%H:%M:%S"),
                    "exit_price": float(exit_price),
                    "source": "commodities_div",
                    "notes": json.dumps(notes_obj),
                    "exit_trigger": "commodities_div_manual_entry",
                    "exit_trigger_type": "discretionary",
                    "garuda_confluence": "NOT_AVAILABLE",
                },
            )
            db.execute(
                text(
                    """
                    UPDATE commodities_div_signals
                    SET trade_log_id = :tlid, updated_at = NOW()
                    WHERE id = :id
                    """
                ),
                {"tlid": int(trade_log_id), "id": signal_id},
            )

        db.commit()
        updated = db.execute(
            text("SELECT * FROM commodities_div_signals WHERE id = :id"),
            {"id": signal_id},
        ).mappings().first()
        out = serialize_signal(dict(updated), prefer_exit_pnl=True)
        out["trade_log_id"] = int(trade_log_id) if trade_log_id is not None else None
        out["mapping_resolved"] = bool(instrument_key)
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

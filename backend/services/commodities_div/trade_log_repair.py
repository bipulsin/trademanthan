"""One-shot / defensive repair for CommDiv → trade_log free-text symbol bugs.

Before free-text MCX parsing (commit 3a1eb1c), manual LIVE entries like
``COPPER SEP FUT`` wrote trade_log with symbol=COPPERSEPFUT, qty=1, contract=NULL.
CommDiv History UI backfills the contract for display, but trade_log / desktop /
reports keep the bad qty×points PnL.

This module re-resolves linked History rows via mapping.attach_instrument_fields
and updates both commodities_div_signals and trade_log. It only touches
source=commodities_div rows (never Kavach / tradelog_ui journals).
"""
from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.commodities_div.mapping import attach_instrument_fields
from backend.services.rule27_trade_log import SOURCE_COMMODITIES_DIV, ensure_trade_log_table

logger = logging.getLogger(__name__)


def _looks_unresolved_symbol(symbol: Any) -> bool:
    """True for concatenated free-text leftovers like COPPERSEPFUT / GOLDOCTFUT."""
    s = str(symbol or "").strip().upper()
    if not s:
        return False
    if " " in s:
        return False
    # Underlying-only names (COPPER, SILVERM) are fine.
    if s.endswith("FUT") or s.endswith("FUTURES"):
        return True
    return False


def _notes_mapping_failed(notes: Any) -> bool:
    if notes is None:
        return False
    obj: Any = notes
    if isinstance(notes, str):
        s = notes.strip()
        if not s or s[0] not in "{[":
            return False
        try:
            obj = json.loads(s)
        except (TypeError, ValueError, json.JSONDecodeError):
            return False
    if not isinstance(obj, dict):
        return False
    if obj.get("mapping_found") is False:
        return True
    if obj.get("underlying_matched") is False and obj.get("commodities_div_manual"):
        return True
    return False


def _merge_notes(notes: Any, extra: Dict[str, Any]) -> str:
    obj: Dict[str, Any] = {}
    if isinstance(notes, dict):
        obj = dict(notes)
    elif isinstance(notes, str) and notes.strip():
        try:
            parsed = json.loads(notes)
            if isinstance(parsed, dict):
                obj = parsed
        except (TypeError, ValueError, json.JSONDecodeError):
            obj = {"prior_notes": notes}
    obj.update(extra)
    return json.dumps(obj)


def resolve_instrument_for_repair(
    *,
    symbol_raw: Optional[str] = None,
    symbol_mapped: Optional[str] = None,
    symbol: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Resolve MCX contract+lot from the best available free-text / TV symbol."""
    candidates = [
        str(symbol_raw or "").strip(),
        str(symbol_mapped or "").strip(),
        str(symbol or "").strip(),
    ]
    seen = set()
    for cand in candidates:
        if not cand or cand in seen:
            continue
        seen.add(cand)
        try:
            inst = attach_instrument_fields(cand, resolve_contract=True)
        except Exception as e:
            logger.debug("commdiv trade_log repair resolve failed for %s: %s", cand, e)
            continue
        if not inst or not inst.get("instrument_key"):
            continue
        if not inst.get("mapping_found") and not inst.get("underlying_matched"):
            continue
        lot = inst.get("lot_size")
        try:
            lot_i = int(lot) if lot is not None else 0
        except (TypeError, ValueError):
            lot_i = 0
        if lot_i <= 0:
            continue
        contract = str(inst.get("contract") or inst.get("trading_symbol") or "").strip()
        mapped = str(inst.get("symbol_mapped") or "").strip().upper()
        if not contract or not mapped:
            continue
        return {
            "symbol_mapped": mapped,
            "contract": contract,
            "instrument_key": str(inst.get("instrument_key") or "").strip() or None,
            "lot_size": lot_i,
            "parse_mode": inst.get("parse_mode"),
            "match_mode": inst.get("match_mode"),
        }
    return None


def needs_commdiv_trade_log_repair(tl: Dict[str, Any], signal: Optional[Dict[str, Any]] = None) -> bool:
    """Whether a commodities_div trade_log row still has free-text / qty=1 damage."""
    if str(tl.get("source") or "").strip().lower() != SOURCE_COMMODITIES_DIV:
        return False
    if signal is not None:
        mode = str(signal.get("trade_mode") or "PAPER").strip().upper()
        if mode != "LIVE":
            return False
        if not signal.get("contract") or not signal.get("lot_size") or not signal.get("instrument_key"):
            return True
    try:
        qty = int(tl.get("qty") or 0)
    except (TypeError, ValueError):
        qty = 0
    contract = str(tl.get("contract") or "").strip()
    if _looks_unresolved_symbol(tl.get("symbol")):
        return True
    if _notes_mapping_failed(tl.get("notes")):
        return True
    if not contract and qty <= 1:
        return True
    return False


def repair_unresolved_commdiv_live_trade_logs(*, dry_run: bool = False) -> List[Dict[str, Any]]:
    """
    Find LIVE CommDiv trade_log rows with unresolved free-text symbols / missing
    lot size and rewrite them from the linked History signal (or notes.symbol_raw).

    Returns a list of change summaries (empty when nothing to fix).
    """
    ensure_trade_log_table()
    db = SessionLocal()
    changes: List[Dict[str, Any]] = []
    try:
        rows = db.execute(
            text(
                """
                SELECT
                    tl.id AS tl_id,
                    tl.session_date,
                    tl.symbol AS tl_symbol,
                    tl.contract AS tl_contract,
                    tl.qty AS tl_qty,
                    tl.direction,
                    tl.entry_time,
                    tl.entry_price,
                    tl.exit_price,
                    tl.notes AS tl_notes,
                    tl.source,
                    s.id AS signal_id,
                    s.symbol_raw,
                    s.symbol_mapped,
                    s.contract AS signal_contract,
                    s.lot_size AS signal_lot_size,
                    s.instrument_key AS signal_instrument_key,
                    s.trade_mode
                FROM trade_log tl
                LEFT JOIN commodities_div_signals s ON s.trade_log_id = tl.id
                WHERE tl.source = :src
                ORDER BY tl.id
                """
            ),
            {"src": SOURCE_COMMODITIES_DIV},
        ).mappings().all()

        for r in rows:
            tl = {
                "id": r["tl_id"],
                "symbol": r["tl_symbol"],
                "contract": r["tl_contract"],
                "qty": r["tl_qty"],
                "notes": r["tl_notes"],
                "source": r["source"],
            }
            signal = None
            if r.get("signal_id") is not None:
                signal = {
                    "id": r["signal_id"],
                    "symbol_raw": r.get("symbol_raw"),
                    "symbol_mapped": r.get("symbol_mapped"),
                    "contract": r.get("signal_contract"),
                    "lot_size": r.get("signal_lot_size"),
                    "instrument_key": r.get("signal_instrument_key"),
                    "trade_mode": r.get("trade_mode") or "PAPER",
                }
                if str(signal["trade_mode"]).upper() != "LIVE":
                    continue
            elif not needs_commdiv_trade_log_repair(tl, None):
                continue

            if not needs_commdiv_trade_log_repair(tl, signal):
                continue

            notes_raw = None
            if isinstance(r.get("tl_notes"), str):
                try:
                    nobj = json.loads(r["tl_notes"])
                    if isinstance(nobj, dict):
                        notes_raw = nobj.get("symbol_raw")
                except (TypeError, ValueError, json.JSONDecodeError):
                    notes_raw = None

            inst = resolve_instrument_for_repair(
                symbol_raw=(signal or {}).get("symbol_raw") or notes_raw,
                symbol_mapped=(signal or {}).get("symbol_mapped") or r.get("tl_symbol"),
                symbol=r.get("tl_symbol"),
            )
            if not inst:
                changes.append(
                    {
                        "tl_id": int(r["tl_id"]),
                        "signal_id": int(r["signal_id"]) if r.get("signal_id") is not None else None,
                        "status": "skipped_unresolvable",
                        "old_symbol": r["tl_symbol"],
                    }
                )
                continue

            new_symbol = inst["symbol_mapped"]
            new_contract = inst["contract"]
            new_qty = int(inst["lot_size"])
            summary = {
                "tl_id": int(r["tl_id"]),
                "signal_id": int(r["signal_id"]) if r.get("signal_id") is not None else None,
                "status": "dry_run" if dry_run else "updated",
                "old_symbol": r["tl_symbol"],
                "new_symbol": new_symbol,
                "old_contract": r["tl_contract"],
                "new_contract": new_contract,
                "old_qty": r["tl_qty"],
                "new_qty": new_qty,
            }
            if dry_run:
                changes.append(summary)
                continue

            new_notes = _merge_notes(
                r.get("tl_notes"),
                {
                    "mapping_found": True,
                    "underlying_matched": True,
                    "repair_commdiv_trade_log": True,
                    "parse_mode": inst.get("parse_mode"),
                    "match_mode": inst.get("match_mode"),
                    "repaired_from_symbol": r["tl_symbol"],
                },
            )

            # Direct UPDATE (not upsert): changing symbol changes the unique key.
            db.execute(
                text(
                    """
                    UPDATE trade_log SET
                        symbol = :symbol,
                        contract = :contract,
                        qty = :qty,
                        notes = :notes,
                        updated_at = NOW()
                    WHERE id = :id
                      AND source = :src
                    """
                ),
                {
                    "symbol": new_symbol,
                    "contract": new_contract,
                    "qty": new_qty,
                    "notes": new_notes,
                    "id": int(r["tl_id"]),
                    "src": SOURCE_COMMODITIES_DIV,
                },
            )

            if r.get("signal_id") is not None:
                db.execute(
                    text(
                        """
                        UPDATE commodities_div_signals SET
                            symbol_mapped = :mapped,
                            contract = :contract,
                            lot_size = :lot,
                            instrument_key = COALESCE(:ik, instrument_key),
                            updated_at = NOW()
                        WHERE id = :id
                        """
                    ),
                    {
                        "mapped": new_symbol,
                        "contract": new_contract,
                        "lot": new_qty,
                        "ik": inst.get("instrument_key"),
                        "id": int(r["signal_id"]),
                    },
                )

            changes.append(summary)

        if not dry_run and changes:
            db.commit()
        else:
            db.rollback()
        return changes
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()

"""Read-only broker verification for user-entered Live fills. Never places orders."""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.tarang.labels import FILL_BROKER, FILL_USER
from backend.services.tarang.schema import ensure_tarang_tables

logger = logging.getLogger(__name__)


def _read_positions() -> Dict[str, Any]:
    out: Dict[str, Any] = {"delta_india": None, "upstox_mcx": None, "errors": []}
    try:
        from backend.services.tarang.adapters.delta_india import DeltaIndiaAdapter

        ad = DeltaIndiaAdapter()
        if hasattr(ad, "positions"):
            out["delta_india"] = ad.positions()  # type: ignore[attr-defined]
        else:
            out["delta_india"] = {"ok": False, "error": "positions_not_implemented"}
    except Exception as e:
        out["errors"].append(f"delta:{e}")
    try:
        from backend.services.tarang.adapters.upstox_mcx import UpstoxMcxAdapter

        ad = UpstoxMcxAdapter()
        if hasattr(ad, "positions"):
            out["upstox_mcx"] = ad.positions()  # type: ignore[attr-defined]
        else:
            out["upstox_mcx"] = {"ok": False, "error": "positions_not_implemented"}
    except Exception as e:
        out["errors"].append(f"upstox:{e}")
    return out


def verify_trade_against_broker(trade_id: int) -> Dict[str, Any]:
    ensure_tarang_tables()
    from backend.services.tarang.lifecycle import get_trade

    trade = get_trade(trade_id)
    if not trade:
        return {"ok": False, "error": "trade_not_found"}
    if str(trade.get("record_type") or "").upper() != "LIVE":
        return {"ok": False, "error": "not_a_live_record"}
    pos = _read_positions()
    legs = trade.get("legs") or []
    symbols = {str(lg.get("symbol") or lg.get("instrument_key") or "") for lg in legs}
    broker_syms: List[str] = []
    for block in (pos.get("delta_india"), pos.get("upstox_mcx")):
        if not isinstance(block, dict):
            continue
        for p in block.get("positions") or block.get("result") or []:
            if isinstance(p, dict):
                broker_syms.append(str(p.get("symbol") or p.get("product_id") or p.get("instrument_key") or ""))
    matched = bool(symbols and any(s and s in broker_syms for s in symbols))
    diff = {"trade_symbols": sorted(symbols), "broker_symbols": broker_syms[:50], "matched": matched}
    db = SessionLocal()
    try:
        if matched:
            db.execute(
                text(
                    """
                    UPDATE tarang_trades
                    SET fill_source = :fs, broker_verified = TRUE, verify_diff = CAST(:d AS jsonb), updated_at = NOW()
                    WHERE id = :id
                    """
                ),
                {"fs": FILL_BROKER, "d": __import__("json").dumps(diff), "id": trade_id},
            )
            db.execute(
                text("UPDATE tarang_fills SET fill_source = :fs WHERE trade_id = :id"),
                {"fs": FILL_BROKER, "id": trade_id},
            )
        else:
            db.execute(
                text(
                    """
                    UPDATE tarang_trades
                    SET broker_verified = FALSE, verify_diff = CAST(:d AS jsonb), updated_at = NOW()
                    WHERE id = :id
                    """
                ),
                {"d": __import__("json").dumps(diff), "id": trade_id},
            )
        db.commit()
    except Exception as e:
        db.rollback()
        return {"ok": False, "error": str(e)[:200], "diff": diff, "verified": False}
    finally:
        db.close()
    return {
        "ok": True,
        "verified": matched,
        "badge": "Verified" if matched else None,
        "diff": None if matched else diff,
        "fill_source": FILL_BROKER if matched else FILL_USER,
        "read_only": True,
        "placed": False,
    }

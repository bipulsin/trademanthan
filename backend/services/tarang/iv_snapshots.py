"""IV snapshot collection + persistence (start immediately via scheduler)."""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.tarang.schema import ensure_tarang_tables

logger = logging.getLogger(__name__)


def _atm_iv_from_chain(chain) -> Dict[str, Any]:
    F = chain.futures_or_spot
    if not F or not chain.quotes:
        return {}
    # nearest CE+PE by strike to F
    ces = [q for q in chain.quotes if q.right == "CE" and q.iv]
    pes = [q for q in chain.quotes if q.right == "PE" and q.iv]
    if not ces or not pes:
        # fallback any
        qs = [q for q in chain.quotes if q.iv]
        if not qs:
            return {}
        q = min(qs, key=lambda x: abs(x.strike - F))
        return {
            "atm_strike": q.strike,
            "atm_iv": q.iv,
            "atm_iv_raw": q.iv_raw,
            "atm_iv_unit": q.iv_unit,
            "source": q.greeks_source,
        }
    ce = min(ces, key=lambda x: abs(x.strike - F))
    pe = min(pes, key=lambda x: abs(x.strike - ce.strike))
    atm_iv = None
    if ce.iv and pe.iv:
        atm_iv = 0.5 * (ce.iv + pe.iv)
    elif ce.iv:
        atm_iv = ce.iv
    elif pe.iv:
        atm_iv = pe.iv
    return {
        "atm_strike": ce.strike,
        "atm_iv": atm_iv,
        "atm_iv_raw": ce.iv_raw,
        "atm_iv_unit": ce.iv_unit,
        "source": ce.greeks_source or pe.greeks_source,
    }


def capture_iv_snapshots(profile_ids: Optional[List[str]] = None) -> Dict[str, Any]:
    """Back-compat wrapper: full-chain snapshot also writes ATM IV rows."""
    from backend.services.tarang.chain_snapshots import capture_chain_snapshots

    return capture_chain_snapshots(profile_ids=profile_ids)


def recent_iv_snapshot_counts() -> Dict[str, int]:
    ensure_tarang_tables()
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT profile_id, COUNT(*)::int AS n
                FROM tarang_iv_snapshots
                GROUP BY profile_id
                """
            )
        ).mappings().all()
        return {r["profile_id"]: r["n"] for r in rows}
    finally:
        db.close()

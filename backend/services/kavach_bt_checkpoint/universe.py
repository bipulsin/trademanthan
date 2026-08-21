"""FO universe helpers for Kavach BT checkpoint."""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from backend.services.kavach_bt_checkpoint.config import EXCLUDED_SYMBOLS


def resolve_instrument_key(db: Session, symbol: str) -> Optional[str]:
    row = (
        db.execute(
            text(
                """
                SELECT currmth_future_instrument_key,
                       nextmth_future_instrement_key AS next_key
                FROM arbitrage_master WHERE UPPER(stock)=UPPER(:s) LIMIT 1
                """
            ),
            {"s": symbol},
        )
        .mappings()
        .first()
    )
    if not row:
        return None
    return (row["currmth_future_instrument_key"] or row["next_key"] or "").strip() or None


def load_fo_universe(db: Session) -> List[Dict[str, Any]]:
    rows = (
        db.execute(
            text(
                """
                SELECT UPPER(stock) AS symbol, currmth_future_instrument_key AS ikey
                FROM arbitrage_master
                WHERE currmth_future_instrument_key IS NOT NULL
                  AND TRIM(currmth_future_instrument_key) <> ''
                ORDER BY stock
                """
            )
        )
        .mappings()
        .all()
    )
    out = []
    for r in rows:
        sym = (r["symbol"] or "").strip().upper()
        if not sym or sym in EXCLUDED_SYMBOLS:
            continue
        ikey = (r["ikey"] or "").strip()
        if not ikey:
            continue
        out.append({"symbol": sym, "instrument_key": ikey})
    return out

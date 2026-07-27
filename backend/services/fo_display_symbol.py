"""Map underlying stock → arbitrage_master.currmth_future_symbol for UI labels.

Internal keys (``symbol`` / stock ticker) stay unchanged for lookups and gating.
UI-visible fields: ``future_symbol`` (FO contract or \"\") and ``display_symbol``
(``future_symbol`` when present, else underlying — null-FO fallback).
"""
from __future__ import annotations

import logging
from typing import Any, Dict, Iterable, List, Optional, Sequence

from sqlalchemy import text

logger = logging.getLogger(__name__)

_MAP_ALL_SQL = text(
    """
    SELECT UPPER(TRIM(stock)) AS stock,
           NULLIF(TRIM(currmth_future_symbol), '') AS future_symbol
    FROM arbitrage_master
    WHERE stock IS NOT NULL AND TRIM(stock) <> ''
    """
)


def ui_display_symbol(underlying: str, future_symbol: Optional[str]) -> str:
    """Prefer currmth future contract; fall back to underlying when FO is null/blank."""
    fut = (future_symbol or "").strip()
    if fut:
        return fut
    return (underlying or "").strip()


def load_currmth_future_symbol_map(
    db, symbols: Optional[Iterable[str]] = None
) -> Dict[str, str]:
    """Return ``{STOCK: currmth_future_symbol}`` for non-empty FO symbols only.

    ``symbols`` optionally filters the result set in Python (table is small ~200).
    """
    want = None
    if symbols is not None:
        want = {str(s).strip().upper() for s in symbols if s and str(s).strip()}
    out: Dict[str, str] = {}
    try:
        rows = db.execute(_MAP_ALL_SQL).fetchall()
        for r in rows:
            stock = (r.stock or "").strip().upper()
            fut = (r.future_symbol or "").strip()
            if not stock or not fut:
                continue
            if want is not None and stock not in want:
                continue
            out[stock] = fut
    except Exception as exc:
        logger.debug("currmth future symbol map failed: %s", exc)
    return out


def attach_future_symbols(
    items: Sequence[Dict[str, Any]],
    *,
    db=None,
    symbol_key: str = "symbol",
    fmap: Optional[Dict[str, str]] = None,
) -> None:
    """Mutate dict items: set ``future_symbol`` + ``display_symbol``; keep ``symbol``."""
    rows: List[Dict[str, Any]] = [x for x in items if isinstance(x, dict) and x.get(symbol_key)]
    if not rows:
        return

    own_db = False
    mapping = fmap
    if mapping is None:
        from backend.database import SessionLocal

        if db is None:
            db = SessionLocal()
            own_db = True
        try:
            mapping = load_currmth_future_symbol_map(
                db, [x.get(symbol_key) for x in rows]
            )
        finally:
            if own_db:
                db.close()

    for item in rows:
        underlying = str(item.get(symbol_key) or "").strip()
        fut = mapping.get(underlying.upper(), "") if mapping else ""
        # Prefer already-populated FO from join; else map; else blank.
        existing = str(item.get("future_symbol") or "").strip()
        if existing:
            fut = existing
        item["future_symbol"] = fut
        item["display_symbol"] = ui_display_symbol(underlying, fut)

"""Ignition evaluation universe — Core board or lock ∪ RS top-5."""
from __future__ import annotations

from typing import List, Set, Tuple

from sqlalchemy import text

from backend.services.daily_checklist_snapshot import get_locked_symbols
from backend.services.rs_conviction_board import SIDE_BEAR, SIDE_BULL
from backend.services.rs_conviction_config import get_config
from backend.services.rs_fast_watch import universe_symbols

SCOPE_CORE_BOARD = "core_board"
SCOPE_LOCKED_OR_TOP5 = "locked_or_top5"


def ignition_scope() -> str:
    scope = (get_config().get("ignition_scope") or SCOPE_LOCKED_OR_TOP5).strip().lower()
    if scope in (SCOPE_CORE_BOARD, SCOPE_LOCKED_OR_TOP5):
        return scope
    return SCOPE_LOCKED_OR_TOP5


def _top5_symbols(db, session_date: str) -> Set[str]:
    row = db.execute(
        text(
            """
            SELECT MAX(scan_time) AS t
            FROM relative_strength_snapshot
            WHERE scan_time::date = CAST(:d AS date)
            """
        ),
        {"d": session_date},
    ).fetchone()
    if not row or not row.t:
        return set()
    rows = db.execute(
        text(
            """
            SELECT symbol
            FROM relative_strength_snapshot
            WHERE scan_time = :t AND rank_position <= 5
            """
        ),
        {"t": row.t},
    ).fetchall()
    return {r.symbol for r in rows if r.symbol}


def _side_for_symbol(db, session_date: str, symbol: str) -> str:
    row = db.execute(
        text(
            """
            SELECT ranking_type
            FROM relative_strength_snapshot
            WHERE scan_time::date = CAST(:d AS date) AND symbol = :sym
            ORDER BY scan_time DESC
            LIMIT 1
            """
        ),
        {"d": session_date, "sym": symbol},
    ).fetchone()
    if row and (row.ranking_type or "").upper() == "BEARISH":
        return SIDE_BEAR
    return SIDE_BULL


def load_ignition_pairs(db, session_date: str) -> List[Tuple[str, str]]:
    """
    Symbols + Kavach side for ignition / silent-accumulation cycles.

    Default scope: morning lock ∪ current RS top-5 (same as Fast Watch recording).
    """
    scope = ignition_scope()
    if scope == SCOPE_CORE_BOARD:
        from backend.services.rs_conviction_board import _load_core_board

        bull = _load_core_board(db, session_date, SIDE_BULL)
        bear = _load_core_board(db, session_date, SIDE_BEAR)
        return [(c["symbol"], SIDE_BULL) for c in bull] + [(c["symbol"], SIDE_BEAR) for c in bear]

    locked = set(get_locked_symbols(db, session_date))
    top5 = _top5_symbols(db, session_date)
    eligible = universe_symbols(session_date, locked=locked, top5_symbols=top5, db=db)
    pairs: List[Tuple[str, str]] = []
    for sym in sorted(eligible):
        pairs.append((sym, _side_for_symbol(db, session_date, sym)))
    return pairs

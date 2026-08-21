"""BT-4 — Garuda confluence wrapper (shadow only)."""
from __future__ import annotations

from typing import Any, Dict, Optional


def classify_garuda(
    *,
    symbol: str,
    direction: str,
    entry_time: Any,
    session_date: Any = None,
) -> Dict[str, Any]:
    """MATCH / NO_MATCH / NOT_AVAILABLE via existing lookup_garuda_confluence."""
    from backend.services.garuda_screener.job import lookup_garuda_confluence

    try:
        out = lookup_garuda_confluence(
            symbol=symbol,
            direction=direction,
            entry_at=entry_time,
            session_date=session_date,
        )
    except TypeError:
        # older signature variants
        out = lookup_garuda_confluence(symbol, direction, entry_time)
    except Exception as e:
        return {
            "garuda_confluence": "NOT_AVAILABLE",
            "garuda_rank": None,
            "garuda_direction": None,
            "error": str(e),
        }

    if not isinstance(out, dict):
        return {
            "garuda_confluence": "NOT_AVAILABLE",
            "garuda_rank": None,
            "garuda_direction": None,
        }

    return {
        "garuda_confluence": out.get("garuda_confluence")
        or out.get("confluence")
        or "NOT_AVAILABLE",
        "garuda_rank": out.get("garuda_rank") or out.get("rank") or out.get("top6_rank"),
        "garuda_direction": out.get("garuda_direction") or out.get("side") or out.get("direction"),
    }

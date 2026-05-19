"""Qualification layer — unified trade state pipeline."""
from __future__ import annotations

from typing import Any, Dict, Optional, Sequence

from backend.services.vajra.trade_state import apply_trade_state


def apply_trade_qualification(
    row: Dict[str, Any],
    *,
    candles_30m: Sequence[Dict[str, Any]],
    candles_5m: Optional[Sequence[Dict[str, Any]]] = None,
    candles_1hr: Optional[Sequence[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    return apply_trade_state(
        row,
        candles_30m=candles_30m,
        candles_5m=candles_5m,
        candles_1hr=candles_1hr,
    )

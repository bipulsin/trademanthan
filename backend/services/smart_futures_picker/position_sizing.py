"""
Position sizing for Smart Futures (rupee risk ÷ stop distance ÷ lot size).
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from backend.config import get_instruments_file_path
from backend.services.smart_futures_config import (
    CAPITAL,
    RISK_PCT,
    TIER1_SIZING_MULT,
    TIER2_SIZING_MULT,
)

logger = logging.getLogger(__name__)


@dataclass
class PositionSizeResult:
    final_lots: int
    rupee_risk: float
    stop_loss_distance: float
    tier_sizing_mult: float
    skipped_reason: Optional[str] = None


def get_futures_lot_size_by_instrument_key(instrument_key: str) -> int:
    """Lot size from nse_instruments.json (same file as Upstox tick lookup)."""
    if not instrument_key:
        return 0
    try:
        instruments_file = get_instruments_file_path()
        if not instruments_file.exists():
            return 0
        instruments_data = json.loads(instruments_file.read_text(encoding="utf-8"))
        for inst in instruments_data:
            if isinstance(inst, dict) and inst.get("instrument_key") == instrument_key:
                lot = inst.get("lot_size") or inst.get("lotSize")
                if lot:
                    return int(lot)
        return 0
    except Exception as e:
        logger.debug("get_futures_lot_size: %s", e)
        return 0


def calculate_position_size(
    *,
    capital: float = CAPITAL,
    risk_pct: float = RISK_PCT,
    stop_loss_distance: float,
    lot_size: int,
    signal_tier: str,
) -> PositionSizeResult:
    """
    rupee_risk = capital * (risk_pct / 100)
    raw_lots = rupee_risk / (stop_loss_distance * lot_size)
    final_lots = floor(raw_lots * tier_sizing_mult)
    """
    if stop_loss_distance <= 0 or lot_size <= 0:
        return PositionSizeResult(
            0, 0.0, stop_loss_distance, 1.0, skipped_reason="BLOCKED: Position size zero"
        )
    tier_u = (signal_tier or "").strip().upper()
    tier_sizing_mult = TIER1_SIZING_MULT if tier_u == "TIER1" else TIER2_SIZING_MULT
    rupee_risk = float(capital) * (float(risk_pct) / 100.0)
    raw_lots = rupee_risk / (float(stop_loss_distance) * float(lot_size))
    final = int(raw_lots * tier_sizing_mult)
    if final < 1:
        return PositionSizeResult(
            0,
            rupee_risk,
            stop_loss_distance,
            tier_sizing_mult,
            skipped_reason="BLOCKED: Position size zero",
        )
    return PositionSizeResult(final, rupee_risk, stop_loss_distance, tier_sizing_mult, None)

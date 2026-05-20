"""Breakout phase taxonomy — initiation through exhaustion."""
from __future__ import annotations

from typing import Any, Dict

PHASE_COMPRESSION = "compression"
PHASE_BREAKOUT_INITIATED = "breakout_initiated"
PHASE_BREAKOUT_VALIDATED = "breakout_validated"
PHASE_EXPANSION = "expansion"
PHASE_MATURE_TREND = "mature_trend"
PHASE_EXTENDED = "extended"
PHASE_EXHAUSTED = "exhausted"

ALL_BREAKOUT_PHASES = frozenset(
    {
        PHASE_COMPRESSION,
        PHASE_BREAKOUT_INITIATED,
        PHASE_BREAKOUT_VALIDATED,
        PHASE_EXPANSION,
        PHASE_MATURE_TREND,
        PHASE_EXTENDED,
        PHASE_EXHAUSTED,
    }
)


def classify_breakout_phase(
    *,
    evs_score: float,
    breakout_score: float,
    extension_risk: float,
    extension_quality: float,
    compression_broken: bool,
    vwap_accepted: bool,
    execution_validated: bool,
    adx_accelerating: bool,
    momentum_score: float = 0.0,
) -> str:
    """Map EVS + structure into breakout lifecycle phase."""
    ext_q = extension_quality if extension_quality > 0 else max(0.0, 100.0 - extension_risk)
    evs = float(evs_score or 0)
    brk = float(breakout_score or 0)

    if ext_q < 22 or extension_risk >= 92:
        if momentum_score < 45:
            return PHASE_EXHAUSTED
        return PHASE_EXTENDED

    if execution_validated and brk >= 58:
        if ext_q >= 55 and adx_accelerating:
            return PHASE_MATURE_TREND
        return PHASE_BREAKOUT_VALIDATED

    if brk >= 65 and evs >= 50 and vwap_accepted:
        return PHASE_EXPANSION

    if evs >= 52 and compression_broken and vwap_accepted and brk >= 45:
        if brk >= 52 or execution_validated:
            return PHASE_BREAKOUT_VALIDATED
        return PHASE_BREAKOUT_INITIATED

    if evs >= 48 and compression_broken and vwap_accepted:
        return PHASE_BREAKOUT_INITIATED

    if not compression_broken and evs < 42:
        return PHASE_COMPRESSION

    if evs >= 40 and vwap_accepted:
        return PHASE_BREAKOUT_INITIATED

    return PHASE_COMPRESSION


def breakout_phase_to_row(phase: str) -> Dict[str, Any]:
    return {"breakout_phase": phase, "breakout_lifecycle": phase}

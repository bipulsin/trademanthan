"""Volume mismatch detection + trade level calculation."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from backend.services.volume_mismatch.constants import DEFAULT_GAP_THRESHOLD_PCT, PREFERRED_ENTRY_BUFFER_PCT
from backend.services.volume_mismatch.signal_rules import (
    compute_gap_percent,
    compute_net_volume,
    evaluate_vm_signal,
)

# Re-export for callers that imported from signal_engine.
__all__ = [
    "MismatchSignal",
    "compute_gap_percent",
    "compute_net_volume",
    "evaluate_mismatch",
]


@dataclass
class MismatchSignal:
    symbol: str
    future_symbol: str
    instrument_key: str
    direction: str  # LONG | SHORT
    gap_percent: float
    first_15m_volume: float
    relative_volume: Optional[float]
    net_volume: float
    score: float
    entry_price: float
    preferred_entry: float
    stop_loss: float
    target1: float
    target2: float
    first_15m_high: float
    first_15m_low: float
    first_15m_open: float
    first_15m_close: float
    bb_upper: Optional[float] = None
    bb_middle: Optional[float] = None
    bb_lower: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "future_symbol": self.future_symbol,
            "instrument_key": self.instrument_key,
            "direction": self.direction,
            "gap_percent": self.gap_percent,
            "first_15m_volume": self.first_15m_volume,
            "relative_volume": self.relative_volume,
            "net_volume": self.net_volume,
            "score": self.score,
            "entry_price": self.entry_price,
            "preferred_entry": self.preferred_entry,
            "stop_loss": self.stop_loss,
            "target1": self.target1,
            "target2": self.target2,
            "first_15m_high": self.first_15m_high,
            "first_15m_low": self.first_15m_low,
            "first_15m_open": self.first_15m_open,
            "first_15m_close": self.first_15m_close,
            "bb_upper": self.bb_upper,
            "bb_middle": self.bb_middle,
            "bb_lower": self.bb_lower,
            "entry_status": "WAITING",
        }


def _trade_levels_long(high: float, low: float) -> Dict[str, float]:
    entry = high
    preferred = high * (1.0 + PREFERRED_ENTRY_BUFFER_PCT / 100.0)
    sl = low
    risk = preferred - sl
    if risk <= 0:
        risk = max(preferred * 0.002, 0.01)
    return {
        "entry_price": round(entry, 4),
        "preferred_entry": round(preferred, 4),
        "stop_loss": round(sl, 4),
        "target1": round(preferred + risk, 4),
        "target2": round(preferred + 2 * risk, 4),
    }


def _trade_levels_short(high: float, low: float) -> Dict[str, float]:
    entry = low
    preferred = low * (1.0 - PREFERRED_ENTRY_BUFFER_PCT / 100.0)
    sl = high
    risk = sl - preferred
    if risk <= 0:
        risk = max(preferred * 0.002, 0.01)
    return {
        "entry_price": round(entry, 4),
        "preferred_entry": round(preferred, 4),
        "stop_loss": round(sl, 4),
        "target1": round(preferred - risk, 4),
        "target2": round(preferred - 2 * risk, 4),
    }


def evaluate_mismatch(
    *,
    symbol: str,
    future_symbol: str,
    instrument_key: str,
    first_bar: Dict[str, Any],
    previous_close: float,
    relative_volume: Optional[float],
    bb: Dict[str, float],
    gap_threshold: float = DEFAULT_GAP_THRESHOLD_PCT,
) -> Optional[MismatchSignal]:
    del gap_threshold  # unified rules use MIN_GAP_PCT_* from constants
    core = evaluate_vm_signal(
        symbol=symbol,
        future_symbol=future_symbol,
        instrument_key=instrument_key,
        first_bar=first_bar,
        previous_close=previous_close,
        bb=bb,
        relative_volume=relative_volume,
    )
    if not core:
        return None

    direction = core["direction"]
    h = core["first_15m_high"]
    l = core["first_15m_low"]
    levels = _trade_levels_long(h, l) if direction == "LONG" else _trade_levels_short(h, l)

    return MismatchSignal(
        symbol=symbol,
        future_symbol=future_symbol,
        instrument_key=instrument_key,
        direction=direction,
        gap_percent=core["gap_percent"],
        first_15m_volume=core["first_15m_volume"],
        relative_volume=core.get("relative_volume"),
        net_volume=core["net_volume"],
        score=core["score"],
        first_15m_high=h,
        first_15m_low=l,
        first_15m_open=core["first_15m_open"],
        first_15m_close=core["first_15m_close"],
        bb_upper=core.get("bb_upper"),
        bb_middle=core.get("bb_middle"),
        bb_lower=core.get("bb_lower"),
        **levels,
    )

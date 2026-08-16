"""Abstract strategy interface for Rocket."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional


class Bias(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"
    NEUTRAL = "NEUTRAL"


@dataclass
class Signal:
    symbol: str
    instrument_key: str
    bias: Bias
    confidence: float
    target: Optional[float] = None
    stop_loss: Optional[float] = None
    lots: int = 1
    reason: str = ""
    features: Optional[Dict[str, float]] = None
    atr: Optional[float] = None


class BaseStrategy(ABC):
    """Strategies emit signals from a synchronized market snapshot."""

    name: str = "base"

    @abstractmethod
    def generate_signals(
        self,
        timestamp: datetime,
        market_snapshot: Dict[str, Dict[str, Any]],
    ) -> List[Signal]:
        """
        ``market_snapshot[symbol]`` typically includes:
        open, high, low, close, volume, oi, vwap, atr, features..., position (optional).
        """

    def on_start(self, context: Dict[str, Any]) -> None:
        """Optional hook before the event loop."""

    def on_finish(self, context: Dict[str, Any]) -> None:
        """Optional hook after the event loop."""

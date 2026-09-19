"""Kosmic Tarang domain types (Phase 1)."""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


class Venue(str, Enum):
    UPSTOX_MCX = "upstox_mcx"
    DELTA_INDIA = "delta_india"


class OptionRight(str, Enum):
    CALL = "CE"
    PUT = "PE"


class TradeMode(str, Enum):
    PAPER = "PAPER"
    LIVE = "LIVE"


class HoldingMode(str, Enum):
    INTRADAY = "INTRADAY"
    POSITIONAL = "POSITIONAL"


@dataclass
class OptionQuote:
    instrument_key: str
    symbol: str
    underlying: str
    expiry: str
    strike: float
    right: str  # CE / PE
    bid: Optional[float] = None
    ask: Optional[float] = None
    mid: Optional[float] = None
    last: Optional[float] = None
    oi: Optional[float] = None
    volume: Optional[float] = None
    iv: Optional[float] = None
    iv_raw: Optional[float] = None
    iv_unit: Optional[str] = None  # "decimal" | "percent"
    delta: Optional[float] = None
    gamma: Optional[float] = None
    theta: Optional[float] = None
    vega: Optional[float] = None
    greeks_source: Optional[str] = None  # venue | black76 | bs
    lot_size: Optional[int] = None
    contract_value: Optional[float] = None
    meta: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class OptionChain:
    profile_id: str
    venue: str
    underlying: str
    expiry: str
    futures_or_spot: Optional[float]
    lot_size: Optional[int]
    strike_step: Optional[float]
    quotes: List[OptionQuote] = field(default_factory=list)
    built_at: Optional[str] = None
    meta: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["quotes"] = [q if isinstance(q, dict) else q for q in d["quotes"]]
        return d

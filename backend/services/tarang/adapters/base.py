"""Adapter interfaces for Kosmic Tarang."""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Protocol

from backend.services.tarang.domain.types import OptionChain


class IMarketAdapter(Protocol):
    venue: str

    def health(self) -> Dict[str, Any]:
        ...

    def build_chain(self, profile_id: str, underlying: str, expiry: Optional[str] = None) -> OptionChain:
        ...


class IBrokerAdapter(Protocol):
    """Read-only in Phase 1; trade methods reserved for Phase 4."""

    venue: str

    def balances(self) -> Dict[str, Any]:
        ...

    def positions(self) -> Dict[str, Any]:
        ...

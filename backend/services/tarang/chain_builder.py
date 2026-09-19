"""ChainBuilder — normalize venue chains into OptionChain with decimal IV."""
from __future__ import annotations

from typing import Optional

from backend.services.tarang.adapters.delta_india import DeltaIndiaAdapter
from backend.services.tarang.adapters.upstox_mcx import UpstoxMcxAdapter
from backend.services.tarang.config import get_profiles
from backend.services.tarang.domain.types import OptionChain


class ChainBuilder:
    def __init__(self) -> None:
        self._upstox = UpstoxMcxAdapter()
        self._delta = DeltaIndiaAdapter()

    def build(self, profile_id: str, expiry: Optional[str] = None, atm_window: int = 8) -> OptionChain:
        profiles = (get_profiles().get("profiles") or {})
        prof = profiles.get(profile_id) or profiles.get(profile_id.upper())
        if not prof:
            return OptionChain(
                profile_id=profile_id,
                venue="unknown",
                underlying=profile_id,
                expiry=expiry or "",
                futures_or_spot=None,
                lot_size=None,
                strike_step=None,
                meta={"error": "unknown_profile"},
            )
        venue = prof.get("venue")
        underlying = prof.get("underlying_symbol") or profile_id
        if venue == "upstox_mcx":
            return self._upstox.build_chain(profile_id, underlying, expiry=expiry, atm_window=atm_window)
        if venue == "delta_india":
            return self._delta.build_chain(profile_id, underlying, expiry=expiry)
        return OptionChain(
            profile_id=profile_id,
            venue=str(venue or "unknown"),
            underlying=underlying,
            expiry=expiry or "",
            futures_or_spot=None,
            lot_size=None,
            strike_step=None,
            meta={"error": f"unsupported_venue:{venue}"},
        )

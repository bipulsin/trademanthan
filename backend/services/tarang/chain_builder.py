"""ChainBuilder — normalize venue chains into OptionChain with decimal IV."""
from __future__ import annotations

from typing import List, Optional

from backend.services.tarang.adapters.delta_india import DeltaIndiaAdapter
from backend.services.tarang.adapters.upstox_mcx import UpstoxMcxAdapter
from backend.services.tarang.config import get_profiles, resolve_energy_underlying
from backend.services.tarang.domain.types import OptionChain


class ChainBuilder:
    def __init__(self) -> None:
        self._upstox = UpstoxMcxAdapter()
        self._delta = DeltaIndiaAdapter()

    def _profile(self, profile_id: str) -> Optional[dict]:
        profiles = get_profiles().get("profiles") or {}
        return profiles.get(profile_id) or profiles.get(profile_id.upper())

    def near_expiries(self, profile_id: str, n: int = 2) -> List[str]:
        prof = self._profile(profile_id)
        if not prof:
            return []
        venue = prof.get("venue")
        if venue == "upstox_mcx":
            us = resolve_energy_underlying(profile_id, prof)
            return self._upstox.near_expiries(us, n=n)
        if venue == "delta_india":
            return self._delta.near_expiries(prof.get("underlying_symbol") or profile_id, n=n)
        return []

    def build(self, profile_id: str, expiry: Optional[str] = None, atm_window: int = 10) -> OptionChain:
        prof = self._profile(profile_id)
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
        if venue == "upstox_mcx":
            underlying = resolve_energy_underlying(profile_id, prof)
            return self._upstox.build_chain(profile_id, underlying, expiry=expiry, atm_window=atm_window)
        if venue == "delta_india":
            underlying = prof.get("underlying_symbol") or profile_id
            return self._delta.build_chain(profile_id, underlying, expiry=expiry, atm_window=atm_window)
        return OptionChain(
            profile_id=profile_id,
            venue=str(venue or "unknown"),
            underlying=str(prof.get("underlying_symbol") or profile_id),
            expiry=expiry or "",
            futures_or_spot=None,
            lot_size=None,
            strike_step=None,
            meta={"error": f"unsupported_venue:{venue}"},
        )

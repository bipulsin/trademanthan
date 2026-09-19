"""ChainBuilder — normalize venue chains into OptionChain with decimal IV."""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from backend.services.tarang.adapters.delta_india import DeltaIndiaAdapter
from backend.services.tarang.adapters.upstox_mcx import UpstoxMcxAdapter
from backend.services.tarang.config import get_profiles, get_risk, resolve_energy_underlying
from backend.services.tarang.domain.types import OptionChain
from backend.services.tarang.strike_window import MIN_ATM_WINDOW_DEFAULT


def _window_cfg() -> Dict[str, Any]:
    snap = get_risk().get("chain_snapshots") or {}
    return {
        "delta_abs_min": float(snap.get("delta_abs_min") or 0.03),
        "delta_abs_max": float(snap.get("delta_abs_max") or 0.97),
        "sigma_mult": float(snap.get("sigma_mult") or 2.5),
        "min_atm_window": int(snap.get("min_atm_window") or snap.get("atm_window") or MIN_ATM_WINDOW_DEFAULT),
    }


class ChainBuilder:
    def __init__(self) -> None:
        self._upstox = UpstoxMcxAdapter()
        self._delta = DeltaIndiaAdapter()

    def _profile(self, profile_id: str) -> Optional[dict]:
        profiles = get_profiles().get("profiles") or {}
        return profiles.get(profile_id) or profiles.get(profile_id.upper())

    def snapshot_expiries(self, profile_id: str) -> List[str]:
        """Expiries in scope for full-chain snapshots (wider than the trade 7-day gate)."""
        prof = self._profile(profile_id)
        if not prof:
            return []
        venue = prof.get("venue")
        if venue == "upstox_mcx":
            us = resolve_energy_underlying(profile_id, prof)
            return self._upstox.all_future_expiries(us)
        if venue == "delta_india":
            return self._delta.snapshot_expiries(prof.get("underlying_symbol") or profile_id)
        return []

    def near_expiries(self, profile_id: str, n: int = 2) -> List[str]:
        listed = self.snapshot_expiries(profile_id)
        return listed[:n] if n else listed

    def build(
        self,
        profile_id: str,
        expiry: Optional[str] = None,
        atm_window: int = 10,
        window_cfg: Optional[Dict[str, Any]] = None,
    ) -> OptionChain:
        prof = self._profile(profile_id)
        cfg = dict(_window_cfg())
        if window_cfg:
            cfg.update(window_cfg)
        cfg["min_atm_window"] = int(cfg.get("min_atm_window") or atm_window or 10)
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
            return self._upstox.build_chain(
                profile_id, underlying, expiry=expiry, atm_window=cfg["min_atm_window"], window_cfg=cfg
            )
        if venue == "delta_india":
            underlying = prof.get("underlying_symbol") or profile_id
            return self._delta.build_chain(
                profile_id, underlying, expiry=expiry, atm_window=cfg["min_atm_window"], window_cfg=cfg
            )
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

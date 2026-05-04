"""
Iron Condor advisory universe: authoritative data lives in Postgres (`iron_condor_universe_master`).
This module only holds the static SEED used once to populate an empty table plus a thin `sector_for_symbol` shim.
"""
from __future__ import annotations

from typing import Dict, Optional

# Initial seed — inserted into `iron_condor_universe_master` when the table is empty (bootstrap only).
IRON_CONDOR_UNIVERSE_SEED: Dict[str, str] = {
    "RELIANCE": "Energy",
    "TCS": "IT",
    "INFOSYS": "IT",
    "HDFCBANK": "Banking",
    "ICICIBANK": "Banking",
    "SBIN": "Banking",
    "BHARTIARTL": "Telecom",
    "KOTAKBANK": "Banking",
    "LT": "Capital Goods",
    "HINDUNILVR": "FMCG",
    "ITC": "FMCG",
    "AXISBANK": "Banking",
    "BAJFINANCE": "Financial Services",
}

# Backward-compat alias — runtime source of truth is Postgres table `iron_condor_universe_master`.
IRON_CONDOR_UNIVERSE = IRON_CONDOR_UNIVERSE_SEED


def sector_for_symbol(symbol: str) -> Optional[str]:
    """Sector from Postgres-backed universe master (lazy loads cache on first access)."""
    from backend.services.iron_condor_service import ic_sector_for_symbol

    return ic_sector_for_symbol(symbol)

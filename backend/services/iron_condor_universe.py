"""
Approved underlying universe for Iron Condor advisory (NIFTY 50 large-cap subset).
Symbol → sector; validation uses uppercased symbol keys.
"""
from __future__ import annotations

from typing import Dict, Optional

# Hardcoded per product spec (symbol, sector).
IRON_CONDOR_UNIVERSE: Dict[str, str] = {
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


def sector_for_symbol(symbol: str) -> Optional[str]:
    key = (symbol or "").strip().upper()
    return IRON_CONDOR_UNIVERSE.get(key)

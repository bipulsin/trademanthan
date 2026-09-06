"""Derive arbitrage_master.sector from Nifty sector_index / sector_instrument_key."""
from __future__ import annotations

import re
from typing import Optional

_NSE_PREFIX = re.compile(r"^NSE_INDEX\|", re.IGNORECASE)
_NIFTY_PREFIX = re.compile(r"^(Nifty|NIFTY)[\s_]*", re.IGNORECASE)

# Explicit shorts for known Upstox keys (must include Fin Service exactly).
SECTOR_INDEX_TO_LABEL = {
    "NSE_INDEX|Nifty Auto": "Auto",
    "NSE_INDEX|Nifty Bank": "Bank",
    "NSE_INDEX|Nifty Chemicals": "Chemicals",
    "NSE_INDEX|NIFTY CONSR DURBL": "CONSR DURBL",
    "NSE_INDEX|Nifty Energy": "Energy",
    "NSE_INDEX|Nifty Fin Service": "Fin Service",
    "NSE_INDEX|Nifty Financial Services": "Fin Service",
    "NSE_INDEX|Nifty FMCG": "FMCG",
    "NSE_INDEX|NIFTY HEALTHCARE": "HEALTHCARE",
    "NSE_INDEX|Nifty Infra": "Infra",
    "NSE_INDEX|Nifty IT": "IT",
    "NSE_INDEX|Nifty Media": "Media",
    "NSE_INDEX|Nifty Metal": "Metal",
    "NSE_INDEX|Nifty MS IT Telcm": "MS IT Telcm",
    "NSE_INDEX|NIFTY OIL AND GAS": "OIL AND GAS",
    "NSE_INDEX|Nifty Pharma": "Pharma",
    "NSE_INDEX|Nifty PSU Bank": "PSU Bank",
    "NSE_INDEX|Nifty Pvt Bank": "Pvt Bank",
    "NSE_INDEX|Nifty Private Bank": "Pvt Bank",
    "NSE_INDEX|Nifty Realty": "Realty",
    "NSE_INDEX|Nifty Serv Sector": "Serv Sector",
    "NSE_INDEX|Nifty Services": "Serv Sector",
    "NSE_INDEX|Nifty Trans Logis": "Trans Logis",
    "NSE_INDEX|Nifty Logistics": "Trans Logis",
    "NSE_INDEX|Nifty Consumer Durables": "CONSR DURBL",
    "NSE_INDEX|Nifty Telecom": "MS IT Telcm",
}


def sector_label_from_index(raw: Optional[str]) -> Optional[str]:
    """Map Upstox index key → short sector. Fin Service is exact."""
    key = str(raw or "").strip()
    if not key:
        return None
    mapped = SECTOR_INDEX_TO_LABEL.get(key)
    if mapped:
        return mapped
    tail = _NSE_PREFIX.sub("", key).strip()
    if not tail:
        return None
    return _NIFTY_PREFIX.sub("", tail).strip() or None

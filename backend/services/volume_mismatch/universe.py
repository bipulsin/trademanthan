"""Universe loader — reuses arbitrage_master curr-month futures."""
from __future__ import annotations

from typing import Any, Dict, List

from backend.services.vajra.job import load_arbitrage_curr_mth_universe


def load_volume_mismatch_universe() -> List[Dict[str, Any]]:
    """Current-month stock futures from arbitrage_master (same as Vajra/Smart/Premium)."""
    rows = load_arbitrage_curr_mth_universe()
    return [
        {
            "symbol": str(r.get("stock") or "").strip().upper(),
            "future_symbol": str(r.get("future_symbol") or "").strip(),
            "instrument_key": str(r.get("instrument_key") or "").strip(),
        }
        for r in rows
        if str(r.get("instrument_key") or "").strip()
    ]

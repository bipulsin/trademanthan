"""Resolve NSE FUT instrument key + lot size for a Trap-CE CSV symbol."""
from __future__ import annotations

import json
from datetime import date
from typing import Dict, Optional, Tuple

from backend.config import get_instruments_file_path
from backend.services.open_low_15m.universe import _aug_map, use_august_2026_futures
from backend.services.volume_mismatch.backtest_universe import (
    _load_fut_by_underlying,
    _resolve_front_month_fut,
)


def resolve_fut(symbol: str, session_date: date) -> Optional[Tuple[str, str]]:
    """Return (trading_symbol, instrument_key) for front-month FUT."""
    sym = (symbol or "").strip().upper()
    if not sym:
        return None
    if use_august_2026_futures(session_date):
        hit = _aug_map().get(sym)
        if hit:
            return hit
    return _resolve_front_month_fut(sym, session_date, _load_fut_by_underlying())


class LotSizeLookup:
    def __init__(self) -> None:
        self._by_key: Dict[str, int] = {}
        path = get_instruments_file_path()
        if not path.is_file():
            return
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return
        if not isinstance(data, list):
            return
        for inst in data:
            if not isinstance(inst, dict):
                continue
            ik = str(inst.get("instrument_key") or "").strip()
            lot = inst.get("lot_size") or inst.get("lotSize")
            if ik and lot:
                try:
                    self._by_key[ik] = int(lot)
                except (TypeError, ValueError):
                    continue

    def get(self, instrument_key: str) -> int:
        return int(self._by_key.get(instrument_key or "", 0) or 0)

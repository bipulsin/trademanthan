"""20-day realized vol from stored underlying daily closes."""
from __future__ import annotations

import math
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.tarang.schema import ensure_tarang_tables

SYMBOL_FOR_PROFILE = {
    "BTC": "BTCUSD",
    "ETH": "ETHUSD",
    "CL": None,
    "NG": None,
}


def realized_vol_20d(closes: List[float]) -> Optional[float]:
    """Annualized stdev of log returns; needs ≥21 closes."""
    xs = [float(c) for c in closes if c is not None and float(c) > 0]
    if len(xs) < 21:
        return None
    xs = xs[-21:]
    rets = []
    for i in range(1, len(xs)):
        if xs[i - 1] > 0 and xs[i] > 0:
            rets.append(math.log(xs[i] / xs[i - 1]))
    if len(rets) < 20:
        return None
    mean = sum(rets) / len(rets)
    var = sum((r - mean) ** 2 for r in rets) / (len(rets) - 1)
    return math.sqrt(var) * math.sqrt(365.0)


def daily_closes(symbol: str, *, asof: Optional[datetime] = None, limit: int = 40) -> List[float]:
    ensure_tarang_tables()
    db = SessionLocal()
    try:
        params: Dict[str, Any] = {"s": symbol, "lim": limit}
        q = """
            SELECT (bar_at AT TIME ZONE 'UTC')::date AS d, (array_agg(close ORDER BY bar_at DESC))[1] AS close
            FROM tarang_hist_underlying
            WHERE symbol = :s AND resolution = '1h' AND close IS NOT NULL
        """
        if asof is not None:
            q += " AND bar_at <= :asof"
            params["asof"] = asof
        q += " GROUP BY 1 ORDER BY 1 DESC LIMIT :lim"
        rows = db.execute(text(q), params).mappings().all()
        return [float(r["close"]) for r in reversed(rows) if r.get("close") is not None]
    finally:
        db.close()


def rv_for_profile(profile_id: str, *, asof: Optional[datetime] = None) -> Optional[float]:
    sym = SYMBOL_FOR_PROFILE.get(str(profile_id or "").upper())
    if not sym:
        return None
    return realized_vol_20d(daily_closes(sym, asof=asof))

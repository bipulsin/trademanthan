"""20-day realized vol from stored underlying daily closes.

The live iv_vs_rv gate uses **daily close-to-close** RV (last 1h bar per UTC day
for crypto 1h series, or native 1d bars for MCX futures). Do not switch the
gate to hourly. Hourly RV is reporting-only.
"""
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
    "CL": "CRUDEOILM",
    "NG": "NATGASMINI",
}


def realized_vol_20d(closes: List[float], *, periods_per_year: float = 365.0) -> Optional[float]:
    """Annualized sample stdev of log returns; needs ≥21 closes (20 returns)."""
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
    return math.sqrt(var) * math.sqrt(float(periods_per_year))


def realized_vol_hourly_20d(closes: List[float]) -> Optional[float]:
    """20 calendar days of hourly log returns, annualized with √(365·24). Reporting only."""
    need = 20 * 24 + 1
    xs = [float(c) for c in closes if c is not None and float(c) > 0]
    if len(xs) < need:
        return None
    xs = xs[-need:]
    rets = [math.log(xs[i] / xs[i - 1]) for i in range(1, len(xs)) if xs[i - 1] > 0 and xs[i] > 0]
    if len(rets) < 20 * 24:
        return None
    mean = sum(rets) / len(rets)
    var = sum((r - mean) ** 2 for r in rets) / (len(rets) - 1)
    return math.sqrt(var) * math.sqrt(365.0 * 24.0)


def daily_closes(symbol: str, *, asof: Optional[datetime] = None, limit: int = 40) -> List[float]:
    """Daily closes: native 1d bars if present, else last 1h close per UTC date (gate path)."""
    ensure_tarang_tables()
    db = SessionLocal()
    try:
        params: Dict[str, Any] = {"s": symbol, "lim": limit}
        extra = ""
        if asof is not None:
            extra = " AND bar_at <= :asof"
            params["asof"] = asof
        n_1d = db.execute(
            text(f"SELECT COUNT(*)::int AS n FROM tarang_hist_underlying WHERE symbol = :s AND resolution = '1d'{extra}"),
            params,
        ).scalar()
        if int(n_1d or 0) > 0:
            q = f"""
                SELECT (bar_at AT TIME ZONE 'UTC')::date AS d, (array_agg(close ORDER BY bar_at DESC))[1] AS close
                FROM tarang_hist_underlying
                WHERE symbol = :s AND resolution = '1d' AND close IS NOT NULL{extra}
                GROUP BY 1 ORDER BY 1 DESC LIMIT :lim
            """
        else:
            q = f"""
                SELECT (bar_at AT TIME ZONE 'UTC')::date AS d, (array_agg(close ORDER BY bar_at DESC))[1] AS close
                FROM tarang_hist_underlying
                WHERE symbol = :s AND resolution = '1h' AND close IS NOT NULL{extra}
                GROUP BY 1 ORDER BY 1 DESC LIMIT :lim
            """
        rows = db.execute(text(q), params).mappings().all()
        return [float(r["close"]) for r in reversed(rows) if r.get("close") is not None]
    finally:
        db.close()


def hourly_closes(symbol: str, *, asof: Optional[datetime] = None, limit: int = 520) -> List[float]:
    ensure_tarang_tables()
    db = SessionLocal()
    try:
        params: Dict[str, Any] = {"s": symbol, "lim": limit}
        q = """
            SELECT close FROM tarang_hist_underlying
            WHERE symbol = :s AND resolution = '1h' AND close IS NOT NULL
        """
        if asof is not None:
            q += " AND bar_at <= :asof"
            params["asof"] = asof
        q += " ORDER BY bar_at DESC LIMIT :lim"
        rows = db.execute(text(q), params).mappings().all()
        return [float(r["close"]) for r in reversed(rows) if r.get("close") is not None]
    finally:
        db.close()


def rv_for_profile(profile_id: str, *, asof: Optional[datetime] = None) -> Optional[float]:
    """Live gate RV: daily close-to-close, annualized √365."""
    sym = SYMBOL_FOR_PROFILE.get(str(profile_id or "").upper())
    if not sym:
        return None
    return realized_vol_20d(daily_closes(sym, asof=asof))


def rv_compare(symbol: str, *, asof: Optional[datetime] = None) -> Dict[str, Any]:
    daily = daily_closes(symbol, asof=asof)
    hourly = hourly_closes(symbol, asof=asof)
    return {
        "symbol": symbol,
        "gate_uses": "daily_close_to_close",
        "daily_n_closes": len(daily),
        "daily_rv_20d": realized_vol_20d(daily),
        "hourly_n_closes": len(hourly),
        "hourly_rv_20d": realized_vol_hourly_20d(hourly),
        "hourly_note": "Reporting only. Live iv_vs_rv uses daily_rv_20d.",
    }

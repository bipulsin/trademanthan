"""Upstox historical daily candles for MCX energy futures. No MCX website scrape."""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from backend.config import settings
from backend.database import SessionLocal
from backend.services.divtest.instruments import ensure_instrument_master
from backend.services.tarang.adapters.upstox_mcx import _expiry_ms, _is_mcx_fo
from backend.services.tarang.delta_history import persist_underlying_candles
from backend.services.tarang.schema import ensure_tarang_tables
from backend.services.upstox_service import UpstoxService

logger = logging.getLogger(__name__)

ENERGY_FUTS = (("CL", "CRUDEOILM"), ("NG", "NATGASMINI"))


def _front_future(underlying: str) -> Optional[Dict[str, Any]]:
    us = underlying.upper()
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    rows = ensure_instrument_master()
    futs = []
    for r in rows:
        if not _is_mcx_fo(r):
            continue
        if str(r.get("underlying_symbol") or "").upper() != us:
            continue
        if str(r.get("instrument_type") or "").upper() not in ("FUT", "FUTURES"):
            continue
        if not r.get("instrument_key"):
            continue
        ems = _expiry_ms(r)
        if ems is None or ems < now_ms:
            continue
        futs.append(r)
    if not futs:
        return None
    futs.sort(key=lambda r: _expiry_ms(r) or 0)
    r = futs[0]
    return {
        "instrument_key": r["instrument_key"],
        "trading_symbol": r.get("trading_symbol"),
        "expiry_ms": _expiry_ms(r),
    }


def _bars_from_upstox(candles: List[Dict[str, Any]]) -> List[List[Any]]:
    out = []
    for c in candles or []:
        ts = c.get("timestamp")
        try:
            if isinstance(ts, (int, float)):
                ts_i = int(ts)
                if ts_i > 10_000_000_000:
                    ts_i //= 1000
            else:
                dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
                ts_i = int(dt.timestamp())
        except (TypeError, ValueError):
            continue
        out.append([ts_i, c.get("open"), c.get("high"), c.get("low"), c.get("close"), c.get("volume")])
    return out


def load_mcx_futures_daily(*, days_back: int = 400) -> Dict[str, Any]:
    """Front-month CRUDEOILM / NATGASMINI daily bars via Upstox historical-candle API."""
    ensure_tarang_tables()
    ux = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    ux.reload_token_from_storage()
    if not ux.access_token:
        return {"ok": False, "error": "upstox_token_missing", "results": []}
    results = []
    for _pid, sym in ENERGY_FUTS:
        fut = _front_future(sym)
        if not fut:
            results.append({"symbol": sym, "ok": False, "error": "no_listed_future"})
            continue
        raw = ux.get_historical_candles_by_instrument_key(
            fut["instrument_key"], interval="days/1", days_back=days_back
        )
        bars = _bars_from_upstox(raw or [])
        n = persist_underlying_candles(sym, "1d", bars)
        db = SessionLocal()
        try:
            rng = db.execute(
                text(
                    """
                    SELECT MIN(bar_at) AS first, MAX(bar_at) AS last, COUNT(*)::int AS n
                    FROM tarang_hist_underlying WHERE symbol = :s AND resolution = '1d'
                    """
                ),
                {"s": sym},
            ).mappings().first()
        finally:
            db.close()
        results.append(
            {
                "symbol": sym,
                "ok": True,
                "instrument_key": fut["instrument_key"],
                "trading_symbol": fut.get("trading_symbol"),
                "fetched": len(bars),
                "inserted_or_seen": n,
                "stored_first": rng["first"].isoformat() if rng and rng.get("first") else None,
                "stored_last": rng["last"].isoformat() if rng and rng.get("last") else None,
                "stored_n": int((rng or {}).get("n") or 0),
                "source": "upstox_historical_candles days/1",
            }
        )
    return {"ok": all(r.get("ok") for r in results) if results else False, "results": results}

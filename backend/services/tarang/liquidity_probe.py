"""Monday (and in-session) MCX liquidity probe on the delta window, mini vs full."""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.tarang.adapters.upstox_mcx import UpstoxMcxAdapter
from backend.services.tarang.calendar import ist_clock, mcx_session_open
from backend.services.tarang.schema import ensure_tarang_tables
from backend.services.tarang.strike_window import years_to_expiry

logger = logging.getLogger(__name__)
IST = ZoneInfo("Asia/Kolkata")

PROBE_SPECS = [
    ("CRUDEOILM", "2026-10-15"),
    ("CRUDEOIL", "2026-10-15"),
    ("NATGASMINI", "2026-10-23"),
    ("NATURALGAS", "2026-10-23"),
]
SHORT_BAND = (0.10, 0.16)


def _leg_row(q) -> Dict[str, Any]:
    bid, ask = q.bid, q.ask
    mid = q.mid
    if mid is None and bid is not None and ask is not None:
        mid = 0.5 * (float(bid) + float(ask))
    spr = None
    if mid and mid > 0 and bid is not None and ask is not None:
        spr = 100.0 * (float(ask) - float(bid)) / mid
    meta = q.meta or {}
    return {
        "symbol": q.symbol,
        "strike": q.strike,
        "right": q.right,
        "delta": q.delta,
        "iv": q.iv,
        "oi": q.oi,
        "volume": q.volume,
        "bid": bid,
        "ask": ask,
        "bid_qty": meta.get("bid_qty"),
        "ask_qty": meta.get("ask_qty"),
        "spread_pct_of_mid": spr,
        "two_sided": bool(bid and ask and float(bid) > 0 and float(ask) > 0),
        "greeks_source": q.greeks_source,
    }


def _classify(quotes) -> Dict[str, List[Dict[str, Any]]]:
    shorts: List[Dict[str, Any]] = []
    wings: List[Dict[str, Any]] = []
    lo, hi = SHORT_BAND
    for q in quotes:
        if q.delta is None:
            continue
        ad = abs(float(q.delta))
        row = _leg_row(q)
        if lo - 1e-9 <= ad <= hi + 1e-9:
            shorts.append(row)
        elif 0.03 - 1e-9 <= ad <= 0.08 + 1e-9:
            wings.append(row)
    return {"shorts_10_16d": shorts, "wings_03_08d": wings}


def run_mcx_liquidity_probe(*, now: Optional[datetime] = None) -> Dict[str, Any]:
    """Capture mini vs full 10–16Δ shorts and long wings on the delta window."""
    ensure_tarang_tables()
    clock = ist_clock(now)
    out: Dict[str, Any] = {
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "ist": clock,
        "mcx_session_open": mcx_session_open(now),
        "underlyings": [],
    }
    if not mcx_session_open(now):
        out["skipped"] = "mcx_session_closed"
        return out
    adapter = UpstoxMcxAdapter()
    db = SessionLocal()
    try:
        for us, expiry in PROBE_SPECS:
            chain = adapter.build_chain("CL" if "CRUDE" in us else "NG", us, expiry=expiry)
            classified = _classify(chain.quotes or [])
            n_strikes = len({q.strike for q in (chain.quotes or [])})
            payload = {
                "underlying": us,
                "expiry": expiry,
                "futures_or_spot": chain.futures_or_spot,
                "n_quotes": len(chain.quotes or []),
                "n_strikes": n_strikes,
                "window": (chain.meta or {}).get("window"),
                "years_to_expiry": years_to_expiry(expiry, now),
                **classified,
            }
            db.execute(
                text(
                    """
                    INSERT INTO tarang_liquidity_probes (ist_hour, underlying_symbol, expiry_date, payload)
                    VALUES (:hour, :us, CAST(:expiry AS date), CAST(:payload AS jsonb))
                    """
                ),
                {
                    "hour": clock["ist_hour"],
                    "us": us,
                    "expiry": expiry,
                    "payload": json.dumps(payload, default=str),
                },
            )
            out["underlyings"].append(payload)
        db.commit()
    except Exception:
        db.rollback()
        logger.exception("mcx liquidity probe failed")
        out["error"] = "probe_failed"
    finally:
        db.close()
    return out

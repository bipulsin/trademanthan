#!/usr/bin/env python3
"""Backtest standalone silent accumulation vs RS first BUY flip."""
from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path

import pytz
from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backend.database import SessionLocal
from backend.services.kavach_silent_accumulation import load_ws_1m_bars, walk_forward_first_fire
from backend.services.rs_conviction_candles import load_instrument_atr_maps

IST = pytz.timezone("Asia/Kolkata")

CASES = [
    {"symbol": "NAUKRI", "session_date": "2026-07-07", "rs_buy_ist": "2026-07-07 09:41:05"},
    {"symbol": "JUBLFOOD", "session_date": "2026-07-06", "rs_buy_ist": "2026-07-06 12:06:07"},
]


def _parse_ist(s: str) -> datetime:
    dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    return IST.localize(dt)


def run_case(db, symbol: str, session_date: str, rs_buy_ist: str) -> dict:
    ikey_map, atr_map = load_instrument_atr_maps(db, {symbol})
    ik = ikey_map.get(symbol)
    if not ik:
        return {"symbol": symbol, "error": "no_instrument_key"}
    bars = load_ws_1m_bars(ik, session_date, db=db)
    if not bars:
        return {"symbol": symbol, "error": "no_ws_1m_bars", "instrument_key": ik}
    atr = atr_map.get(symbol, 2.0)
    hit = walk_forward_first_fire(bars, atr)
    rs_buy = _parse_ist(rs_buy_ist)
    out = {
        "symbol": symbol,
        "session_date": session_date,
        "instrument_key": ik,
        "bars": len(bars),
        "atr_pct": atr,
        "rs_buy_ist": rs_buy_ist,
        "signal_fire": hit.get("candle_time") if hit else None,
        "signal_detail": hit,
    }
    if hit and hit.get("candle_time"):
        try:
            sig_t = datetime.fromisoformat(hit["candle_time"].replace("Z", "+00:00"))
            if sig_t.tzinfo is None:
                sig_t = IST.localize(sig_t)
            else:
                sig_t = sig_t.astimezone(IST)
            lead_min = (rs_buy - sig_t).total_seconds() / 60.0
            out["lead_minutes_vs_rs_buy"] = round(lead_min, 1)
            out["led_rs_flip"] = lead_min > 0
        except (TypeError, ValueError):
            out["lead_minutes_vs_rs_buy"] = None
    else:
        out["led_rs_flip"] = False
    return out


def main() -> None:
    db = SessionLocal()
    try:
        results = [run_case(db, **c) for c in CASES]
    finally:
        db.close()
    print(json.dumps({"cases": results}, indent=2, default=str))


if __name__ == "__main__":
    main()

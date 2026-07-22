#!/usr/bin/env python3
"""Backfill kavach_vwap_touch_reject_log from Upstox 5m→10m (shadow only).

Uses lock symbols/directions from rs_live_kavach_audit for each session.
"""
from __future__ import annotations

import json
import time
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pytz
from sqlalchemy import text

from backend.config import settings
from backend.database import SessionLocal
from backend.services.kavach_10m import aggregate_10m_bars
from backend.services.kavach_vwap_touch_reject_log import (
    compute_touch_reject,
    ensure_vwap_touch_reject_table,
    persist_vwap_touch_reject,
)
from backend.services.relative_strength_scanner import CANDLE_INTERVAL, _parse_ist_date, _sorted_candles
from backend.services.upstox_service import UpstoxService
from backend.services.vajra.indicators import cumulative_vwap

IST = pytz.timezone("Asia/Kolkata")
OUT = Path("/tmp/ckpt_22jul_followup/D")
THROTTLE = 0.2


def session_key(d) -> str:
    if hasattr(d, "isoformat"):
        return d.isoformat()[:10]
    return str(d)[:10]


def main() -> None:
    ensure_vwap_touch_reject_table()
    OUT.mkdir(parents=True, exist_ok=True)
    db = SessionLocal()
    ux = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    pairs = list(db.execute(text("""
        SELECT DISTINCT session_date, symbol, lock_direction
        FROM rs_live_kavach_audit
        WHERE session_date BETWEEN CAST(:a AS date) AND CAST(:b AS date)
          AND lock_direction IN ('LONG','SHORT')
        ORDER BY 1,2
    """), {"a": "2026-07-08", "b": "2026-07-22"}).mappings())
    print("pairs", len(pairs))
    ikey_cache: Dict[str, Optional[str]] = {}
    events = []
    by_dir = {"LONG": 0, "SHORT": 0}
    fwd = {"LONG": [], "SHORT": []}

    def ikey(sym):
        if sym not in ikey_cache:
            row = db.execute(text(
                "SELECT currmth_future_instrument_key FROM arbitrage_master WHERE UPPER(stock)=:s LIMIT 1"
            ), {"s": sym.upper()}).fetchone()
            ikey_cache[sym] = str(row[0]) if row and row[0] else None
        return ikey_cache[sym]

    seen_day_sym = set()
    for p in pairs:
        sd, sym, direction = p["session_date"], p["symbol"], p["lock_direction"]
        key = (str(sd), sym)
        if key in seen_day_sym:
            # still need both directions if present — use per-direction
            pass
        ik = ikey(sym)
        if not ik:
            continue
        time.sleep(THROTTLE)
        raw = ux.get_historical_candles_by_instrument_key(
            ik, interval=CANDLE_INTERVAL, days_back=5, range_end_date=sd
        )
        c5 = _sorted_candles(raw) if raw else []
        today = [c for c in c5 if session_key(_parse_ist_date(c.get("timestamp"))) == session_key(sd)]
        bars = aggregate_10m_bars(today) if len(today) >= 2 else []
        if not bars:
            continue
        highs = [float(b["high"]) for b in bars]
        lows = [float(b["low"]) for b in bars]
        closes = [float(b["close"]) for b in bars]
        vols = [float(b.get("volume") or 0) for b in bars]
        vwaps = cumulative_vwap(highs, lows, closes, vols)
        for i, b in enumerate(bars):
            flags = compute_touch_reject(
                direction=direction, high=highs[i], low=lows[i], close=closes[i], vwap=vwaps[i]
            )
            be = b.get("bar_end")
            metrics = {
                "bar_evaluated_at": be,
                "bar_open": b.get("open"),
                "bar_high": highs[i],
                "bar_low": lows[i],
                "bar_close": closes[i],
                "price": closes[i],
                "vwap": vwaps[i],
            }
            persist_vwap_touch_reject(
                db, symbol=sym, lock_direction=direction, metrics=metrics, source="backfill"
            )
            if flags["vwap_touch_reject"]:
                by_dir[direction] += 1
                # forward 3/6 bars close pts
                fwd_pts = {}
                for n in (3, 6):
                    if i + n < len(closes):
                        if direction == "LONG":
                            fwd_pts[f"n{n}"] = closes[i + n] - closes[i]
                        else:
                            fwd_pts[f"n{n}"] = closes[i] - closes[i + n]
                ev = {
                    "session_date": str(sd),
                    "symbol": sym,
                    "direction": direction,
                    "bar_end": str(be),
                    "close": closes[i],
                    "vwap": vwaps[i],
                    "wick_pts": flags["vwap_wick_through_pts"],
                    **{f"fwd_{k}": v for k, v in fwd_pts.items()},
                }
                events.append(ev)
                fwd[direction].append(ev)
        db.commit()
        seen_day_sym.add(key)
        if len(seen_day_sym) % 25 == 0:
            print("progress", len(seen_day_sym), "rejects", len(events))

    def avg(xs):
        return sum(xs) / len(xs) if xs else None

    summary = {
        "backfill_window": "2026-07-13→22 (audit lock pairs; audit starts 13-Jul)",
        "reject_events": len(events),
        "by_direction": by_dir,
        "long_avg_fwd_n3": avg([e["fwd_n3"] for e in fwd["LONG"] if "fwd_n3" in e]),
        "short_avg_fwd_n3": avg([e["fwd_n3"] for e in fwd["SHORT"] if "fwd_n3" in e]),
        "long_avg_fwd_n6": avg([e["fwd_n6"] for e in fwd["LONG"] if "fwd_n6" in e]),
        "short_avg_fwd_n6": avg([e["fwd_n6"] for e in fwd["SHORT"] if "fwd_n6" in e]),
        "forward_logging": "live via persist_live_kavach_audit → kavach_vwap_touch_reject_log",
        "gate": False,
    }
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "D_backfill_summary.json").write_text(json.dumps(summary, indent=2))
    (OUT / "D_backfill_events.json").write_text(json.dumps(events[:500], indent=2, default=str))
    print("SUMMARY", summary)
    db.close()


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Validate 5m-based momentum ignition signals against REST historical candles.

Usage (repo root):
  PYTHONPATH=. python3 scripts/validate_momentum_ignition.py --days 10 --symbols 20

Order-flow (WS depth) cannot be backtested — run forward paper log after deploy.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pytz

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from backend.config import settings  # noqa: E402
from backend.database import SessionLocal  # noqa: E402
from backend.services.kavach_momentum_ignition import (  # noqa: E402
    coincident_confirmation,
    pullback_depth_contraction,
)
from backend.services.rs_conviction_config import DEFAULTS  # noqa: E402
from backend.services.rs_conviction_signals import (  # noqa: E402
    accumulation_signal,
    normalized_vwap_slope,
)
from backend.services.upstox_service import UpstoxService  # noqa: E402
from sqlalchemy import text  # noqa: E402

IST = pytz.timezone("Asia/Kolkata")


def _load_universe(limit: int):
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT stock, currmth_future_instrument_key
                FROM arbitrage_master
                WHERE currmth_future_instrument_key IS NOT NULL
                ORDER BY stock
                LIMIT :n
                """
            ),
            {"n": limit},
        ).fetchall()
    finally:
        db.close()
    return [(r.stock, r.currmth_future_instrument_key) for r in rows]


def _forward_return(candles, idx, bars=3):
    if idx + bars >= len(candles):
        return None
    c0 = float(candles[idx].get("close") or 0)
    c1 = float(candles[idx + bars].get("close") or 0)
    if c0 <= 0:
        return None
    return (c1 - c0) / c0 * 100.0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=10)
    ap.add_argument("--symbols", type=int, default=15)
    ap.add_argument("--side", default="BULL")
    args = ap.parse_args()

    ux = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    ux.reload_token_from_storage()
    if not ux.access_token:
        print(json.dumps({"ok": False, "error": "No Upstox token — run with credentials for REST backtest"}))
        return 1

    universe = _load_universe(args.symbols)
    cfg = DEFAULTS
    results = {"signals": {}, "samples": 0, "universe": len(universe)}

    for sym, ik in universe:
        candles = ux.get_historical_candles_by_instrument_key(
            ik.replace(":", "|"), interval="minutes/5", days_back=min(args.days + 5, 31)
        )
        if not candles or len(candles) < 60:
            continue
        atr_pct = 1.2
        hits = {"slope": 0, "slope_hit": 0, "accum": 0, "accum_hit": 0, "pullback": 0, "pullback_hit": 0}
        for i in range(40, len(candles) - 4):
            window = candles[: i + 1]
            slope = normalized_vwap_slope(window, atr_pct, cfg)
            accum, _, _ = accumulation_signal(window, args.side, cfg)
            pb, _ = pullback_depth_contraction(window, args.side, atr_pct)
            conf, _ = coincident_confirmation(window, args.side)
            fwd = _forward_return(candles, i, 3)
            if fwd is None:
                continue
            results["samples"] += 1
            bull = args.side.upper() == "BULL"
            moved = fwd > 0.15 if bull else fwd < -0.15
            if slope >= 50:
                hits["slope"] += 1
                hits["slope_hit"] += 1 if moved else 0
            if accum >= 80:
                hits["accum"] += 1
                hits["accum_hit"] += 1 if moved else 0
            if pb >= 60:
                hits["pullback"] += 1
                hits["pullback_hit"] += 1 if moved else 0
            if conf >= 40:
                pass
        results["signals"][sym] = hits

    def _rate(k, h):
        n = sum(v.get(k, 0) for v in results["signals"].values())
        hit = sum(v.get(h, 0) for v in results["signals"].values())
        return round(hit / n, 3) if n else None

    summary = {
        "slope_precision_3bar": _rate("slope", "slope_hit"),
        "accum_precision_3bar": _rate("accum", "accum_hit"),
        "pullback_precision_3bar": _rate("pullback", "pullback_hit"),
        "samples": results["samples"],
        "recommendation": (
            "Keep ignition_ui_enabled=false until forward WS order-flow validation completes. "
            "Enable 5m components if precision >= 0.55 on slope+accum."
        ),
    }
    print(json.dumps({"ok": True, "summary": summary, "per_symbol": results["signals"]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

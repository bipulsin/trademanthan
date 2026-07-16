#!/usr/bin/env python3
"""Characterize which sessions were vulnerable to global-index 10m mis-pairing.

NSE has 75 five-minute bars/day (odd). Under the old aggregate_10m_bars stepper,
a session was misaligned iff its first bar index in the fetch buffer was odd —
equivalently, when an odd number of complete prior session-days sat before it.

With CANDLE_DAYS_BACK=5 (typical live fetch), that means: among the trading days
present in the buffer, every other day (by count of complete prior days) was
vulnerable. This script prints per-day first_today_idx parity for a symbol.
"""
from __future__ import annotations

import argparse
import sys
from collections import Counter
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", default="UPL")
    ap.add_argument("--from-date", default="2026-07-13")
    ap.add_argument("--to-date", default="2026-07-16")
    ap.add_argument("--days-back", type=int, default=5)
    args = ap.parse_args()

    from backend.config import settings
    from backend.database import SessionLocal
    from backend.services.relative_strength_scanner import (
        CANDLE_INTERVAL,
        _parse_ist_date,
        _sorted_candles,
    )
    from backend.services.rs_conviction_candles import load_instrument_atr_maps
    from backend.services.upstox_service import UpstoxService

    db = SessionLocal()
    try:
        ik = load_instrument_atr_maps(db, {args.symbol.upper()})[0].get(args.symbol.upper())
    finally:
        db.close()
    if not ik:
        print("no instrument", args.symbol)
        return 1

    end = date.fromisoformat(args.to_date)
    start = date.fromisoformat(args.from_date)
    svc = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    # Pull enough history to cover from_date with days_back context
    span = (end - start).days + args.days_back + 5
    candles = _sorted_candles(
        svc.get_historical_candles_by_instrument_key(
            ik, interval=CANDLE_INTERVAL, days_back=max(span, args.days_back)
        )
        or []
    )
    by_day = Counter(_parse_ist_date(c.get("timestamp")) for c in candles)
    print(f"symbol={args.symbol} ik={ik} bars={len(candles)}")
    print(f"days_in_buffer={dict(sorted((k or '?', v) for k, v in by_day.items()))}")
    print("date\tbars\tfirst_idx\tparity\tvulnerable_under_old_global_pair")

    d = start
    while d <= end:
        ds = d.isoformat()
        # Simulate a live fetch ending on d with days_back
        fetch_start = d - timedelta(days=args.days_back + 3)
        sliced = [
            c
            for c in candles
            if ( _parse_ist_date(c.get("timestamp")) or "" ) >= fetch_start.isoformat()
            and ( _parse_ist_date(c.get("timestamp")) or "" ) <= ds
        ]
        # Prefer last `days_back` trading days only (approximate Upstox days_back)
        dates = sorted({_parse_ist_date(c.get("timestamp")) for c in sliced if _parse_ist_date(c.get("timestamp"))})
        keep = set(dates[-args.days_back :]) if len(dates) > args.days_back else set(dates)
        sliced = [c for c in sliced if _parse_ist_date(c.get("timestamp")) in keep]
        idxs = [i for i, c in enumerate(sliced) if _parse_ist_date(c.get("timestamp")) == ds]
        if not idxs:
            print(f"{ds}\t0\t-\t-\tno_data")
        else:
            fi = idxs[0]
            odd = fi % 2 == 1
            nb = sum(1 for c in sliced if _parse_ist_date(c.get("timestamp")) == ds)
            print(f"{ds}\t{nb}\t{fi}\t{'ODD' if odd else 'EVEN'}\t{'YES' if odd else 'no'}")
        d += timedelta(days=1)
    print(
        "\nRule of thumb: with complete 75-bar prior days, vulnerable iff "
        "(# complete prior days in buffer) is odd."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

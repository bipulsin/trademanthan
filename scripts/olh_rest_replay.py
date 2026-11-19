#!/usr/bin/env python3
"""Safe off-prod OLH REST replay: today's 5m for 3 NSE_EQ + Nifty 50.

Uses the same code path as Phase A/B (``_fetch_rest_5m``): today → V3 intraday only;
previous days → historical. Does not write screen rows or restart services.

Usage (from repo root):
  python scripts/olh_rest_replay.py
  python scripts/olh_rest_replay.py --symbols RELIANCE,TCS,INFY
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def main() -> int:
    parser = argparse.ArgumentParser(description="OLH REST 5m replay (read-only)")
    parser.add_argument(
        "--symbols",
        default="RELIANCE,TCS,INFY",
        help="Comma-separated equity symbols (default RELIANCE,TCS,INFY)",
    )
    parser.add_argument(
        "--session",
        default="",
        help="Session date YYYY-MM-DD (default: today IST)",
    )
    args = parser.parse_args()

    import pytz

    from backend.services.open_high_low.config import NIFTY_INDEX_KEY
    from backend.services.open_low_high.screener import (
        _fetch_rest_5m,
        _resolve_equity_key,
        _token_tail,
        _upstox,
        _today,
    )

    IST = pytz.timezone("Asia/Kolkata")
    session = date.fromisoformat(args.session) if args.session else _today()
    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]

    ux = _upstox()
    print(
        json.dumps(
            {
                "session": session.isoformat(),
                "token_last4": _token_tail(getattr(ux, "access_token", None)),
                "nifty_key": NIFTY_INDEX_KEY,
            },
            indent=2,
        )
    )

    # Resolve equity keys via universe when possible.
    rows_by_sym = {}
    try:
        from backend.services.open_high_low.data import load_universe

        for u in load_universe():
            rows_by_sym[str(u.get("symbol") or "").upper()] = u
    except Exception as exc:
        print(f"universe load skipped: {exc}")

    targets = []
    for sym in symbols:
        row = rows_by_sym.get(sym) or {"underlying_key": ""}
        ik = _resolve_equity_key(ux, row, sym)
        targets.append((sym, ik or f"(unresolved:{sym})"))
    targets.append(("NIFTY", NIFTY_INDEX_KEY))

    for label, ik in targets:
        print("---")
        print(f"symbol={label} instrument_key={ik}")
        if not ik or ik.startswith("(unresolved"):
            print("status=SKIP reason=no_instrument_key")
            continue
        try:
            bars = _fetch_rest_5m(ux, ik, session)
            day_bars = [b for b in bars if b.session_date == session]
            print(
                f"status=OK candle_count={len(bars)} session_bars={len(day_bars)} "
                f"first={day_bars[0].ts.isoformat() if day_bars else None} "
                f"last={day_bars[-1].ts.isoformat() if day_bars else None}"
            )
            if day_bars:
                b0 = day_bars[0]
                print(
                    f"snippet o={b0.open} h={b0.high} l={b0.low} c={b0.close} "
                    f"ts={b0.ts.isoformat()}"
                )
            else:
                print("body_snippet=(no session bars — empty or prior days only)")
        except Exception as exc:
            print(f"status=ERROR error={exc}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

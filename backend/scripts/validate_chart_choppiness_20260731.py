#!/usr/bin/env python3
"""Retrovalidate chart_choppiness on a session for named symbols.

Usage (local or paperclip, needs DB + Upstox creds):
  python -m backend.scripts.validate_chart_choppiness_20260731
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

SYMBOLS = [
    "APLAPOLLO",
    "HYUNDAI",
    "ASHOKLEY",
    "BAJAJFINSV",
    "KALYANKJIL",
    "SWIGGY",
    "BAJFINANCE",
    "BOSCHLTD",
    "ETERNAL",
    "PREMIERENE",
    "WIPRO",
]
SESSION = "2026-07-31"


def _load_candles(db, symbol: str, session_date: str) -> Optional[List[Dict[str, Any]]]:
    from backend.config import settings
    from backend.services.relative_strength_scanner import (
        CANDLE_INTERVAL,
        _sorted_candles,
    )
    from backend.services.rs_conviction_candles import load_instrument_atr_maps
    from backend.services.upstox_service import UpstoxService

    ikey_map, _ = load_instrument_atr_maps(db, {symbol})
    ikey = ikey_map.get(symbol)
    if not ikey:
        print(f"  NO_IKEY {symbol}", file=sys.stderr)
        return None
    # Need the target session day + prior days for EMA warm-up.
    # When run on a weekend/holiday, small days_back windows can omit the
    # last trading session (observed: days_back=5 from Sat 1-Aug omitted 31-Jul).
    days_back = 12
    raw = UpstoxService(
        settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET
    ).get_historical_candles_by_instrument_key(
        ikey, interval=CANDLE_INTERVAL, days_back=days_back
    )
    if not raw:
        return None
    return _sorted_candles(raw)


def _fmt_ts(ts: Optional[str]) -> str:
    if not ts:
        return "?"
    try:
        from backend.services.kavach_volume import _parse_ist

        dt = _parse_ist(ts)
        if dt:
            return dt.strftime("%H:%M")
    except Exception:
        pass
    return str(ts)[:16]


def main() -> int:
    from backend.database import SessionLocal
    from backend.services.chart_choppiness import (
        choppiness_summary,
        evaluate_chart_choppiness,
    )

    db = SessionLocal()
    reports: List[Dict[str, Any]] = []
    try:
        for sym in SYMBOLS:
            print(f"\n=== {sym} ===", flush=True)
            candles = _load_candles(db, sym, SESSION)
            if not candles:
                reports.append({"symbol": sym, "error": "no_candles"})
                print("  no candles")
                continue
            ev = evaluate_chart_choppiness(
                candles, session_date=SESSION, symbol=sym
            )
            summary = choppiness_summary(ev)
            reports.append(
                {
                    "summary": summary,
                    "bootstrap_crosses": [
                        {
                            "bar": c.bar_idx,
                            "ts": _fmt_ts(c.timestamp),
                            "dir": c.direction,
                            "kind": c.kind,
                        }
                        for c in ev.bootstrap_crosses
                    ],
                    "body_crosses": [
                        {
                            "bar": c.bar_idx,
                            "ts": _fmt_ts(c.timestamp),
                            "dir": c.direction,
                            "kind": c.kind,
                        }
                        for c in ev.all_body_crosses
                    ],
                    "ema5_crosses": [
                        {
                            "bar": x["bar_idx"],
                            "ts": _fmt_ts(x.get("timestamp")),
                            "dir": x["direction"],
                        }
                        for x in ev.ema5_vwap_crosses
                    ],
                    "timeline": [
                        {
                            "bar": t.bar_idx,
                            "ts": _fmt_ts(t.timestamp),
                            "A": t.cond_a_on,
                            "B_n": t.cond_b_count,
                            "B": t.cond_b_on,
                            "flag": t.combined_on,
                            "cross": t.body_cross,
                            "note": t.note,
                        }
                        for t in ev.timeline
                    ],
                }
            )
            print(f"  bars={ev.bars_n}")
            print(f"  bootstrap: flagged={ev.bootstrap_flagged} | {ev.bootstrap_note}")
            for c in ev.bootstrap_crosses:
                print(f"    boot cross @{_fmt_ts(c.timestamp)} {c.direction} ({c.kind})")
            print(f"  body-crosses ({len(ev.all_body_crosses)}):")
            for c in ev.all_body_crosses:
                print(f"    {_fmt_ts(c.timestamp)} bar{c.bar_idx} {c.direction} ({c.kind})")
            print(f"  Condition B EMA5/VWAP crosses={ev.cond_b_count} on={ev.cond_b_final}")
            for x in ev.ema5_vwap_crosses:
                print(f"    EMA5x @{_fmt_ts(x.get('timestamp'))} {x['direction']}")
            # Compact A state changes
            prev = None
            print("  Condition A / combined toggles:")
            for t in ev.timeline:
                if prev is None or t.cond_a_on != prev.cond_a_on or t.combined_on != prev.combined_on:
                    print(
                        f"    {_fmt_ts(t.timestamp)} A={'ON' if t.cond_a_on else 'OFF'} "
                        f"B={t.cond_b_count} flag={'CHOP' if t.combined_on else 'ok'} "
                        f"cross={t.body_cross or '-'} ({t.note})"
                    )
                prev = t
            print(
                f"  FINAL: A={ev.cond_a_final} B={ev.cond_b_final}({ev.cond_b_count}) "
                f"combined={'CHOPPY' if ev.combined_final else 'not choppy'} "
                f"toggles={summary['state_toggles']} A_on_bars={summary['cond_a_on_bars']}/{ev.bars_n}"
            )
    finally:
        db.close()

    out_dir = Path("docs/diagnostics/chart_choppiness_validate_20260731")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "evidence.json"
    out_path.write_text(json.dumps({"session": SESSION, "reports": reports}, indent=2, default=str))
    print(f"\nWrote {out_path}")

    # Sanity table
    print("\n=== SANITY TABLE ===")
    print(f"{'symbol':12} {'boot':5} {'bodyX':5} {'emaX':5} {'A_end':5} {'B_end':5} {'FLAG':8} {'A_on%':6} {'toggles'}")
    for r in reports:
        if r.get("error"):
            print(f"{r['symbol']:12} ERROR {r['error']}")
            continue
        s = r["summary"]
        pct = 100.0 * s["cond_a_on_bars"] / max(1, s["bars_n"])
        print(
            f"{s['symbol']:12} {str(s['bootstrap_flagged']):5} {s['body_cross_n']:5} "
            f"{s['ema5_vwap_cross_n']:5} {str(s['cond_a_final']):5} {str(s['cond_b_final']):5} "
            f"{'CHOPPY' if s['combined_final'] else 'ok':8} {pct:5.0f}% {s['state_toggles']}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

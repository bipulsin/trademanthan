#!/usr/bin/env python3
"""Weekly ATR READY-suppress report (trader-facing).

Counts checklist renders suppressed this week via
``inputs.atr_ready_suppress_fired`` (and ``atr_ready_suppress_would``), then
simulates forward R for first-fired trades per symbol×session using the same
exit model as ``scripts/backtest_scoring_gate_v2.py``.

Usage (paperclip app container)::

    PYTHONPATH=/app /opt/venv/bin/python /app/scripts/report_atr_ready_suppress_weekly.py

    # custom window (IST session dates):
    SGB_P1_START=2026-07-20 SGB_P1_END=2026-07-24 \\
      PYTHONPATH=/app /opt/venv/bin/python /app/scripts/report_atr_ready_suppress_weekly.py

    # write JSON summary:
    SGB_OUT=/tmp/atr_suppress_weekly \\
      PYTHONPATH=/app /opt/venv/bin/python /app/scripts/report_atr_ready_suppress_weekly.py

Env:
  SGB_P1_START / SGB_P1_END — inclusive session dates
      (default: last 5 weekdays ending today IST)
  SGB_OUT — optional directory for JSON artifact
"""
from __future__ import annotations

import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

# Repo root on sys.path for `scripts.backtest_scoring_gate_v2` + backend imports.
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from scripts.backtest_scoring_gate_v2 import (  # noqa: E402
    IST,
    WARMUP_FROM,
    aggregate,
    build_10m,
    f,
    hm,
    ist,
    load_1m,
    load_kavach_audit,
    load_symbol_candidates,
    pick_instrument,
    q,
    simulate_trade,
)


def _default_window():
    today = datetime.now(IST).date()
    days = []
    d = today
    while len(days) < 5:
        if d.weekday() < 5:
            days.append(d)
        d -= timedelta(days=1)
    return min(days).isoformat(), today.isoformat()


def main():
    start = os.environ.get("SGB_P1_START")
    end = os.environ.get("SGB_P1_END")
    if not start or not end:
        start, end = _default_window()

    print(f"[report] ATR READY suppress weekly  {start} → {end}")
    rows = q(
        """
        SELECT session_date::text AS session_date, symbol, direction,
               rendered_state, logged_at,
               inputs->>'confidence' AS grade,
               inputs->>'trade_entry' AS trade_entry,
               inputs->>'trade_sl' AS trade_sl,
               inputs->'dwell_entry_shadow'->'distance'->>'lot' AS lot,
               inputs->>'atr_ready_suppress_fired' AS atr_ready_suppress_fired,
               inputs->>'atr_ready_suppress_would' AS atr_ready_suppress_would,
               inputs->>'atr_consumed_pct' AS atr_consumed_pct,
               inputs->>'atr_progression_increasing' AS atr_progression_increasing,
               inputs->>'atr_consumed_pct_from_open' AS atr_consumed_pct_from_open
        FROM kavach_ready_consistency_log
        WHERE session_date BETWEEN CAST(:a AS date) AND CAST(:b AS date)
        ORDER BY session_date, symbol, logged_at
        """,
        a=start,
        b=end,
    )
    fired = [
        r for r in rows if str(r.get("atr_ready_suppress_fired") or "").lower() == "true"
    ]
    would = [
        r for r in rows if str(r.get("atr_ready_suppress_would") or "").lower() == "true"
    ]

    by_day_fired = defaultdict(int)
    by_sym_fired = defaultdict(int)
    for r in fired:
        by_day_fired[str(r["session_date"])] += 1
        by_sym_fired[(r.get("symbol") or "").upper()] += 1

    print(f"  total consistency rows: {len(rows)}")
    print(f"  suppress fired (live):  {len(fired)}")
    print(f"  suppress would:         {len(would)}")
    print("  by session (fired):")
    for d in sorted(by_day_fired):
        print(f"    {d}: {by_day_fired[d]}")
    print("  top symbols (fired):")
    for sym, n in sorted(by_sym_fired.items(), key=lambda x: -x[1])[:10]:
        print(f"    {sym}: {n}")

    trades = []
    if fired:
        warmup = (datetime.fromisoformat(start) - timedelta(days=10)).date().isoformat()
        cand = load_symbol_candidates()
        need = defaultdict(set)
        for r in fired:
            need[str(r["session_date"])].add((r.get("symbol") or "").upper())
        all_iks = sorted(
            {ik for syms in need.values() for s in syms for ik in cand.get(s, [])}
        )
        rows_1m = load_1m(all_iks, warmup, end) if all_iks else []
        bars = build_10m(rows_1m) if rows_1m else {}
        audit = load_kavach_audit(start, end)
        audit_prices = defaultdict(list)
        for a in audit:
            p, t = f(a["price"]), ist(a["bar_evaluated_at"])
            if p and t:
                audit_prices[(str(a["session_date"]), a["symbol"])].append((t, p))
        sym_ik = {}
        for day, syms in need.items():
            for s in syms:
                ik, _ = pick_instrument(s, day, cand.get(s, []), bars, audit_prices)
                if ik:
                    sym_ik[(day, s)] = ik

        first_seen = set()
        for r in sorted(
            fired, key=lambda x: (str(x["session_date"]), ist(x["logged_at"]))
        ):
            day = str(r["session_date"])
            sym = (r.get("symbol") or "").upper()
            key = (day, sym)
            if key in first_seen:
                continue
            first_seen.add(key)
            entry, stop = f(r.get("trade_entry")), f(r.get("trade_sl"))
            lot = int(f(r.get("lot")) or 0)
            ik = sym_ik.get(key)
            db = bars.get(ik, {}).get(day) if ik else None
            if not (entry and stop and db):
                continue
            sig = ist(r["logged_at"])
            sim = simulate_trade(db, sig, r.get("direction") or "LONG", entry, stop)
            if not sim:
                continue
            trades.append(
                {
                    "session": day,
                    "symbol": sym,
                    "direction": r.get("direction"),
                    "grade": r.get("grade"),
                    "signal_ist": hm(sig),
                    "atr_consumed_pct": f(
                        r.get("atr_consumed_pct") or r.get("atr_consumed_pct_from_open")
                    ),
                    "lot": lot,
                    **sim,
                    "pnl_inr": round(sim["pts"] * lot, 0) if lot else None,
                }
            )

    agg = aggregate(trades)
    print("  first-trade sim (fired suppresses):")
    print(f"    {json.dumps(agg, indent=2)}")

    summary = {
        "window": {"start": start, "end": end},
        "n_rows": len(rows),
        "n_fired": len(fired),
        "n_would": len(would),
        "by_session_fired": dict(by_day_fired),
        "top_symbols_fired": dict(
            sorted(by_sym_fired.items(), key=lambda x: -x[1])[:20]
        ),
        "first_trade_agg": agg,
        "first_trades_sample": trades[:20],
        "warmup_note": WARMUP_FROM,
    }
    out_dir = os.environ.get("SGB_OUT")
    if out_dir:
        Path(out_dir).mkdir(parents=True, exist_ok=True)
        path = Path(out_dir) / "atr_ready_suppress_weekly.json"
        path.write_text(json.dumps(summary, indent=2, default=str))
        print(f"  wrote {path}")
    return summary


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""OLH dress rehearsal (staging/offline). Does NOT deploy to production.

Preferred calendar day: 2026-11-24 (NSE holiday — Guru Nanak Jayanti).
Data source default: synthetic fixtures from 2026-11-23 schema (+ optional recorded frames).

Usage (repo root):
  python scripts/olh_dress_rehearsal.py
  python scripts/olh_dress_rehearsal.py --mode synthetic --out /tmp/olh_rehearsal.json
  python scripts/olh_dress_rehearsal.py --mode recorded --session 2026-11-23
  python scripts/olh_dress_rehearsal.py --faults-only

Never run production deploy during 08:45–09:35 IST on a trading day.
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
    p = argparse.ArgumentParser(description="OLH dress rehearsal (no prod deploy)")
    p.add_argument("--session", default="2026-11-23", help="Source session YYYY-MM-DD")
    p.add_argument(
        "--mode",
        choices=("synthetic", "recorded"),
        default="synthetic",
        help="WS source (recorded requires OLH_RAW_RECORD_DIR frames)",
    )
    p.add_argument("--faults-only", action="store_true", help="Only run fault-injection table")
    p.add_argument("--rest-delay", type=float, default=0.0, help="REST mock delay seconds")
    p.add_argument("--rest-fail-rate", type=float, default=0.0, help="REST mock fail rate 0..1")
    p.add_argument("--out", default="", help="Write JSON report path")
    args = p.parse_args()

    from backend.services.open_low_high.fault_injection import run_all_fault_scenarios
    from backend.services.open_low_high.raw_recording import estimate_daily_bytes
    from backend.services.open_low_high.rehearsal import RestMockKnob, run_dress_rehearsal
    from backend.services.open_low_high.silent_stall import eq_timeline_20261123

    print("=== OLH dress rehearsal (NO PRODUCTION DEPLOY) ===")
    print("Holiday dress day: 2026-11-24 (NSE closed). Jobs for 2026-11-25 remain registered.")
    print("Size estimate:", json.dumps(estimate_daily_bytes(), indent=2))
    print("23-Nov EQ timeline:", json.dumps(eq_timeline_20261123(), indent=2)[:800], "...")

    if args.faults_only:
        report = run_all_fault_scenarios()
    else:
        report = run_dress_rehearsal(
            source_session=date.fromisoformat(args.session),
            mode=args.mode,
            rest_knobs=RestMockKnob(
                delay_sec=args.rest_delay,
                fail_rate=args.rest_fail_rate,
            ),
            run_faults=True,
        )

    text = json.dumps(report, indent=2, default=str)
    print(text)
    if args.out:
        Path(args.out).write_text(text)
        print(f"Wrote {args.out}")

    # Print compact fault table
    table = report.get("table") or report.get("fault_table") or []
    if table:
        print("\nFault injection (expected vs actual):")
        print(f"{'scenario':<32} {'pass':<5} expected → actual")
        for row in table:
            print(
                f"{row.get('scenario',''):<32} {str(row.get('pass')):<5} "
                f"{row.get('expected','')} → {row.get('actual','')}"
            )
    return 0 if (report.get("ok", True) or report.get("passed", 0)) else 1


if __name__ == "__main__":
    raise SystemExit(main())

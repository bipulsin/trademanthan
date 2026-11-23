#!/usr/bin/env python3
"""Independent OLH morning watchdog process (separate from app APScheduler).

Run on paperclip/host cron (trading days) at 09:21, 09:26, 09:30 IST:
  21 9 * * 1-5 cd /path/TradeManthan && /opt/venv/bin/python scripts/olh_watchdog.py --label 0921
  26 9 * * 1-5 ... --label 0926
  30 9 * * 1-5 ... --label 0930

Skips holidays/weekends. Does not deploy.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def main() -> int:
    p = argparse.ArgumentParser(description="OLH independent watchdog")
    p.add_argument("--label", default="", help="e.g. 0921 / 0926 / 0930")
    p.add_argument("--no-alert", action="store_true")
    args = p.parse_args()

    from backend.services.open_low_high.watchdog import nearest_watchdog_slot, run_watchdog_check

    label = args.label or nearest_watchdog_slot().strftime("%H%M")
    report = run_watchdog_check(label=label, send_alert=not args.no_alert)
    print(json.dumps(report, indent=2, default=str))
    return 1 if report.get("red") else 0


if __name__ == "__main__":
    raise SystemExit(main())

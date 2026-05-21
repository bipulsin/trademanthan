#!/usr/bin/env python3
"""Replay saved ChartInk inbox payloads into scan.html (intraday_stock_options)."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.services.chartink_scan_bridge import chartink_payload_for_scan, run_scan_chartink_ingest
from backend.services.daily_futures_service import (
    chartink_bearish_webhook_inbox_dir,
    chartink_webhook_inbox_dir,
    parse_daily_futures_chartink_webhook_body,
)


def _replay_file(path: Path, direction: str, dry_run: bool) -> int:
    raw = path.read_bytes()
    if not raw.strip():
        return 0
    try:
        inner = parse_daily_futures_chartink_webhook_body(raw, None)
    except Exception as exc:
        print(f"skip {path.name}: parse failed: {exc}")
        return 0
    payload = chartink_payload_for_scan(inner, direction=direction)
    stocks = (payload.get("stocks") or "").strip()
    if not stocks:
        print(f"skip {path.name}: no stocks")
        return 0
    print(f"{'dry-run' if dry_run else 'ingest'} {path.name} -> {len(stocks.split(','))} symbols ({direction})")
    if not dry_run:
        run_scan_chartink_ingest(payload, direction)
    return 1


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", help="Filter inbox files by YYYYMMDD prefix in filename (UTC)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    total = 0
    for direction, inbox in (("bullish", chartink_webhook_inbox_dir()), ("bearish", chartink_bearish_webhook_inbox_dir())):
        if not inbox.exists():
            continue
        for path in sorted(inbox.glob("*")):
            if not path.is_file():
                continue
            if args.date and args.date not in path.name:
                continue
            total += _replay_file(path, direction, args.dry_run)
    print(f"done files={total}")


if __name__ == "__main__":
    main()

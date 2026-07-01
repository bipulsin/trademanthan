#!/usr/bin/env python3
"""One-shot Relative Strength scan (full arbitrage_master universe → Top-5 snapshot).

Also invoked by deploy-paperclip GitHub Actions workflow after production deploy.
"""

import argparse
import json
import sys

from backend.services.relative_strength_scanner import run_relative_strength_scan


def main() -> int:
    ap = argparse.ArgumentParser(description="Run Relative Strength Scanner once.")
    ap.add_argument(
        "--cache-only",
        action="store_true",
        help="Use in-process candle cache only (no direct Upstox fetches).",
    )
    ap.add_argument(
        "--fetch",
        action="store_true",
        help="Allow direct Upstox candle fetches (recommended off-hours / EOD).",
    )
    args = ap.parse_args()
    cache_only = True if args.cache_only else (False if args.fetch else None)
    out = run_relative_strength_scan(scan_trigger="manual_cli", cache_only=cache_only)
    print(json.dumps(out, indent=2, default=str))
    return 0 if out.get("ok") else 1


if __name__ == "__main__":
    sys.exit(main())

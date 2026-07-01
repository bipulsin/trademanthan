#!/usr/bin/env python3
"""One-shot: roll arbitrage_master currmth/nextmth from instruments JSON (no LTP)."""
from __future__ import annotations

import argparse
import sys

from backend.services.arbitrage_daily_setup_scheduler import run_arbitrage_metadata_roll_now


def main() -> int:
    ap = argparse.ArgumentParser(description="Roll arbitrage_master futures metadata only")
    ap.add_argument(
        "--no-roll-window",
        action="store_true",
        help="Pick 1st/2nd upcoming expiry even during roll window",
    )
    args = ap.parse_args()
    out = run_arbitrage_metadata_roll_now(apply_roll_window=not args.no_roll_window)
    print(out)
    return 0 if out.get("success") else 1


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""Repair CommDiv LIVE trade_log rows with free-text symbols / qty=1.

Usage (paperclip app container)::

    docker compose exec -T app python scripts/repair_commdiv_unresolved_trade_log.py
    docker compose exec -T app python scripts/repair_commdiv_unresolved_trade_log.py --dry-run

Only updates source=commodities_div rows; Kavach / tradelog_ui journals are untouched.
"""
from __future__ import annotations

import argparse
import json
import sys

from backend.services.commodities_div.trade_log_repair import (
    repair_unresolved_commdiv_live_trade_logs,
)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Report changes without writing",
    )
    args = p.parse_args(argv)
    changes = repair_unresolved_commdiv_live_trade_logs(dry_run=args.dry_run)
    print(json.dumps({"dry_run": args.dry_run, "count": len(changes), "changes": changes}, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

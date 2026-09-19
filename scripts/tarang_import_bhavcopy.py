#!/usr/bin/env python3
"""Import MCX Bhavcopy CSVs into tarang_hist_eod. Same report as POST /api/tarang/bhavcopy/import."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.services.tarang.bhavcopy import import_bhavcopy_path  # noqa: E402
from backend.services.tarang.eod_reconstruct import persist_reconstruction  # noqa: E402
from backend.services.tarang.schema import ensure_tarang_tables  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("paths", nargs="+", help="Bhavcopy CSV paths")
    ap.add_argument("--reconstruct", action="store_true")
    args = ap.parse_args()
    ensure_tarang_tables()
    reports = []
    for p in args.paths:
        reports.append(import_bhavcopy_path(p))
    out = {"imports": reports}
    if args.reconstruct:
        out["reconstruction"] = persist_reconstruction()
    print(json.dumps(out, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

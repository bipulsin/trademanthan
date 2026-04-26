#!/usr/bin/env python3
"""
Replay a saved ChartInk Daily Futures bearish raw payload (same as POST handler background).

Usage (from repo root):
  python3 backend/scripts/replay_daily_futures_bearish_raw.py
  python3 backend/scripts/replay_daily_futures_bearish_raw.py /path/to/file.raw.bear.json

If no path is given, uses the newest *.raw.bear.json in the bearish inbox.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

import backend.env_bootstrap  # noqa: F401

from backend.services.daily_futures_service import (  # noqa: E402
    chartink_bearish_webhook_inbox_dir,
    normalize_symbols_from_payload,
    process_chartink_webhook_bearish,
)


def _parse_payload(raw: bytes) -> object:
    s = raw.decode("utf-8", errors="replace").strip()
    if not s:
        return None
    if s.startswith("{") or s.startswith("["):
        return json.loads(s)
    return s


def main() -> None:
    if len(sys.argv) > 1:
        path = Path(sys.argv[1]).resolve()
        if not path.is_file():
            print("Not a file:", path, file=sys.stderr)
            sys.exit(1)
    else:
        d = chartink_bearish_webhook_inbox_dir()
        files = sorted(
            d.glob("*.raw.bear.json") if d.exists() else [],
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        if not files:
            print("No *.raw.bear.json in", d, file=sys.stderr)
            sys.exit(1)
        path = files[0]

    raw_bytes = path.read_bytes()
    pl = _parse_payload(raw_bytes)
    if pl is None:
        print("Empty payload", path, file=sys.stderr)
        sys.exit(1)
    syms = normalize_symbols_from_payload(pl)
    if not syms:
        print("No symbols in payload from", path, file=sys.stderr)
        print(json.dumps(pl, indent=2, default=str)[:2000])
        sys.exit(1)
    print("File:", path)
    print("Symbols:", syms)
    out = process_chartink_webhook_bearish(syms)
    print(json.dumps(out, indent=2, default=str))


if __name__ == "__main__":
    main()

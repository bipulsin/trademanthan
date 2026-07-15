#!/usr/bin/env python3
"""Export shortlist ADX joined to kavach_universe_vwap_scan (research only).

  python3 scripts/export_shortlist_adx.py
  python3 scripts/export_shortlist_adx.py -o /tmp/shortlist-adx.json
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
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "-o",
        "--output",
        default=str(ROOT / "docs" / "diagnostics" / "shortlist-adx-steep-outside-lock-20260713-15.json"),
        help="Output JSON path",
    )
    ap.add_argument("--pace", type=float, default=0.15, help="Seconds between Upstox fetches")
    args = ap.parse_args()

    from backend.services.kavach_shortlist_adx_export import build_shortlist_adx_export

    print("Building shortlist ADX export (Upstox + scan join)…", flush=True)
    data = build_shortlist_adx_export(pace_sec=args.pace)
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")
    if not data.get("ok"):
        print(f"FAILED: {data.get('error')}", file=sys.stderr)
        print(f"Wrote {out}", flush=True)
        return 1
    pairs = data.get("pairs") or []
    n_rows = sum(len(p.get("rows") or []) for p in pairs)
    n_adx = sum(
        1
        for p in pairs
        for r in (p.get("rows") or [])
        if r.get("adx_14") is not None
    )
    print(
        f"ok pairs={len(pairs)} rows={n_rows} adx_nonnull={n_adx} → {out}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

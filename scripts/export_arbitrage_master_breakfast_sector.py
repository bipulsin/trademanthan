#!/usr/bin/env python3
"""Export arbitrage_master grouped by Breakfast strategy sector label."""
from __future__ import annotations

import csv
import json
import subprocess
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.services.breakfast_strategy.universe import SECTOR_UNIVERSE
from backend.services.sector_movers import (
    SECTOR_INDEX_INSTRUMENT_ALIASES,
    UPSTOX_SECTOR_INDEX_KEYS,
    normalize_sector_instrument_key,
)

OUTPUT = Path("/Users/bipulsahay/Downloads/arbitrage_master_by_breakfast_sector.csv")

REMOTE_PY = (
    "import json; from sqlalchemy import text; from backend.database import SessionLocal; "
    "db = SessionLocal(); "
    "rows = db.execute(text('SELECT stock, sector, sector_index FROM arbitrage_master "
    "ORDER BY sector_index, stock')).mappings().all(); "
    "print(json.dumps([dict(r) for r in rows])); db.close()"
)


def build_breakfast_index_to_label() -> dict[str, str]:
    breakfast_labels = {label for label, _ in SECTOR_UNIVERSE}
    out: dict[str, str] = {}
    for label in breakfast_labels:
        ikey = UPSTOX_SECTOR_INDEX_KEYS.get(label)
        if not ikey:
            continue
        out[str(ikey).strip()] = label
        norm = normalize_sector_instrument_key(ikey)
        if norm:
            out[str(norm).strip()] = label
    for alias, canon in SECTOR_INDEX_INSTRUMENT_ALIASES.items():
        lbl = out.get(str(canon).strip()) or out.get(
            str(normalize_sector_instrument_key(canon) or "").strip()
        )
        if lbl:
            out[str(alias).strip()] = lbl
            norm_alias = normalize_sector_instrument_key(alias)
            if norm_alias:
                out[str(norm_alias).strip()] = lbl
    return out


def fetch_rows() -> list[dict]:
    ssh_script = ROOT / "scripts" / "paperclip-ssh.sh"
    remote_cmd = f"docker exec twcto-app-1 python -c {json.dumps(REMOTE_PY)}"
    proc = subprocess.run(
        [str(ssh_script), remote_cmd],
        capture_output=True,
        text=True,
        check=True,
    )
    stdout = proc.stdout.strip()
    for line in reversed(stdout.splitlines()):
        line = line.strip()
        if line.startswith("["):
            return json.loads(line)
    raise RuntimeError(f"No JSON array in SSH output:\n{stdout}\n{proc.stderr}")


def map_row(row: dict, index_to_label: dict[str, str]) -> dict:
    stock = str(row.get("stock") or "").strip()
    arbitrage_sector = str(row.get("sector") or "").strip()
    raw_index = str(row.get("sector_index") or "").strip()
    norm_index = normalize_sector_instrument_key(raw_index) or raw_index
    label = index_to_label.get(raw_index) or index_to_label.get(norm_index)
    if label:
        breakfast_label = label
        in_universe = "Y"
    else:
        breakfast_label = "OUTSIDE"
        in_universe = "N"
    return {
        "stock": stock,
        "arbitrage_sector": arbitrage_sector,
        "sector_index": raw_index,
        "breakfast_sector_label": breakfast_label,
        "in_breakfast_universe": in_universe,
        "_sort_index": norm_index,
    }


def main() -> int:
    index_to_label = build_breakfast_index_to_label()
    rows = fetch_rows()
    mapped = [map_row(r, index_to_label) for r in rows]
    mapped.sort(
        key=lambda r: (
            r["breakfast_sector_label"],
            r["_sort_index"],
            r["stock"].upper(),
        )
    )

    fieldnames = [
        "stock",
        "arbitrage_sector",
        "sector_index",
        "breakfast_sector_label",
        "in_breakfast_universe",
    ]
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(mapped)

    counts = Counter(r["breakfast_sector_label"] for r in mapped)
    in_count = sum(1 for r in mapped if r["in_breakfast_universe"] == "Y")
    print(f"file: {OUTPUT}")
    print(f"rows: {len(mapped)}")
    print(f"in_breakfast_universe: {in_count}")
    print(f"outside: {len(mapped) - in_count}")
    print("per breakfast_sector_label:")
    for label, n in sorted(counts.items(), key=lambda x: (-x[1], x[0])):
        print(f"  {label}: {n}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Compare arbitrage_master sector_index vs Breakfast SECTOR_UNIVERSE."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.breakfast_strategy.universe import (
    SECTOR_UNIVERSE,
    load_arbitrage_by_sector,
    sector_index_key_for_label,
)
from backend.services.sector_movers import UPSTOX_SECTOR_INDEX_KEYS, normalize_sector_instrument_key

# Manual nearest-match hints for arb-only keys
ARB_TO_BREAKFAST_HINT: dict[str, tuple[str, str]] = {
    "NSE_INDEX|Nifty Pvt Bank": ("Nifty Private Bank", "now in Breakfast universe"),
    "NSE_INDEX|Nifty Bank": ("Nifty Private Bank", "legacy Bank index — use Pvt Bank for F&O privates; PSU separate"),
    "NSE_INDEX|Nifty Pvt Bank": ("Nifty Private Bank", "exact match"),
    "NSE_INDEX|Nifty Chemicals": ("—", "not in Breakfast 15; add if needed"),
    "NSE_INDEX|Nifty Trans Logis": ("Nifty Infra", "logistics → closest thematic: Infra"),
    "NSE_INDEX|Nifty Serv Sector": ("Nifty Services", "exact match"),
    "NSE_INDEX|Nifty MS IT Telcm": ("Nifty Telecom", "exact match"),
}


def _hint(raw_key: str, sector: str) -> str:
    rk = str(raw_key or "").strip()
    if rk in ARB_TO_BREAKFAST_HINT:
        lbl, why = ARB_TO_BREAKFAST_HINT[rk]
        return f"{lbl} ({why})"
    sector_u = (sector or "").upper()
    for lbl, _ in SECTOR_UNIVERSE:
        if sector_u and sector_u.replace("_", "") in lbl.upper().replace(" ", ""):
            return f"{lbl} (sector column `{sector}` overlap)"
    return "no close match — review manually"


def main() -> None:
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT TRIM(sector_index) AS sector_index, TRIM(sector) AS sector,
                       COUNT(*) AS n
                FROM arbitrage_master
                WHERE sector_index IS NOT NULL AND TRIM(sector_index) <> ''
                GROUP BY 1, 2
                ORDER BY 1
                """
            )
        ).mappings().all()
    finally:
        db.close()

    bf_labels = {label: sector_index_key_for_label(label) for label, _ in SECTOR_UNIVERSE}
    bf_keys_norm = {
        normalize_sector_instrument_key(k) for k in bf_labels.values() if k
    }

    arb_by_norm: dict[str, dict] = {}
    for r in rows:
        raw = str(r["sector_index"] or "").strip()
        nk = normalize_sector_instrument_key(raw)
        arb_by_norm[nk] = {"raw_key": raw, "sector": r.get("sector"), "n": int(r["n"] or 0)}

    print("=== In arbitrage_master but NOT in Breakfast SECTOR_UNIVERSE ===\n")
    print(f"{'sector_index':<42} {'sector':<14} {'#':>4}  suggestion")
    print("-" * 95)
    for nk in sorted(arb_by_norm, key=lambda k: arb_by_norm[k]["raw_key"]):
        if nk in bf_keys_norm:
            continue
        r = arb_by_norm[nk]
        print(
            f"{r['raw_key']:<42} {str(r.get('sector') or ''):<14} {r['n']:>4}  "
            f"{_hint(r['raw_key'], str(r.get('sector') or ''))}"
        )

    by = load_arbitrage_by_sector()
    print("\n=== In Breakfast SECTOR_UNIVERSE — stock count in arbitrage_master ===\n")
    print(f"{'Breakfast label':<28} {'Upstox key':<34} {'#':>4}  status")
    print("-" * 95)
    for label, _yh in SECTOR_UNIVERSE:
        key = sector_index_key_for_label(label) or "MISSING"
        n = len(by.get(key, [])) if key != "MISSING" else 0
        if n > 0:
            status = "OK"
        elif key == "MISSING":
            status = "NO UPSTOX KEY"
        else:
            status = "EMPTY — dropped at rank (no F&O tagged)"
        print(f"{label:<28} {key:<34} {n:>4}  {status}")

    print("\n=== Upstox sector keys NOT in Breakfast and NOT in arbitrage_master ===\n")
    arb_raw = {r["raw_key"] for r in arb_by_norm.values()}
    bf_label_set = {lbl for lbl, _ in SECTOR_UNIVERSE}
    for lbl, ikey in sorted(UPSTOX_SECTOR_INDEX_KEYS.items()):
        if lbl in bf_label_set:
            continue
        in_arb = ikey in arb_raw or normalize_sector_instrument_key(ikey) in arb_by_norm
        if not in_arb:
            print(f"  {lbl:<28} {ikey}")

    print(f"\nBreakfast sectors: {len(SECTOR_UNIVERSE)} | With stocks: "
          f"{sum(1 for lbl,_ in SECTOR_UNIVERSE if len(by.get(sector_index_key_for_label(lbl) or '',[]))>0)} | "
          f"arbitrage_master distinct keys: {len(arb_by_norm)}")


if __name__ == "__main__":
    main()

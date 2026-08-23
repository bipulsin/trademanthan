#!/usr/bin/env python3
"""
Ensure ``sector`` column on arbitrage_master / arbitrage_lowliquid and backfill from FnO Liquid CSV.

Also backfills missing ``sector_index`` from backend/fno_sector_mapping.csv, then CSV-sector → index map.

Usage:
  PYTHONPATH=. python backend/scripts/apply_fno_liquid_sector_to_arbitrage.py \\
      --csv /path/to/FnO\\ Liquid.csv [--dry-run]
"""
from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path
from typing import Dict, Optional, Tuple

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import inspect, text  # noqa: E402

from backend.database import engine  # noqa: E402
from backend.services.fno_sector_mapping_csv import load_fno_sector_index_map  # noqa: E402
from backend.services.sector_movers import (  # noqa: E402
    equity_sector_index_instrument_key,
    normalize_sector_instrument_key,
)

# FnO Liquid CSV ``sector`` label → Upstox sector index (when not in fno_sector_mapping.csv)
CSV_SECTOR_TO_INDEX: Dict[str, str] = {
    "financials": "NSE_INDEX|Nifty Fin Service",
    "i.t": "NSE_INDEX|Nifty MS IT Telcm",
    "bank": "NSE_INDEX|Nifty Bank",
    "healthcare": "NSE_INDEX|NIFTY HEALTHCARE",
    "auto": "NSE_INDEX|Nifty Auto",
    "metals & mining": "NSE_INDEX|Nifty Metal",
    "industrials": "NSE_INDEX|Nifty Infra",
    "consumer discretionary": "NSE_INDEX|NIFTY CONSR DURBL",
    "fmcg": "NSE_INDEX|Nifty FMCG",
    "power & utilities": "NSE_INDEX|Nifty Energy",
    "aerospace & defence": "NSE_INDEX|Nifty Infra",
    "energy": "NSE_INDEX|Nifty Energy",
    "building materials": "NSE_INDEX|Nifty Infra",
    "realty": "NSE_INDEX|Nifty Realty",
    "transportation": "NSE_INDEX|Nifty Infra",
    "telecom-service": "NSE_INDEX|Nifty MS IT Telcm",
    "telecom": "NSE_INDEX|Nifty MS IT Telcm",
    "services": "NSE_INDEX|Nifty Fin Service",
    "textiles": "NSE_INDEX|Nifty Infra",
    "chemicals": "NSE_INDEX|Nifty Infra",
}

TABLES = ("arbitrage_master", "arbitrage_lowliquid")

# Symbols not in FnO Liquid CSV / fno_sector_mapping.csv
SYMBOL_SECTOR_OVERRIDES: Dict[str, Tuple[str, str]] = {
    "COCHINSHIP": ("industrials", "NSE_INDEX|Nifty Infra"),
}


def _load_fno_liquid_sectors(path: Path) -> Dict[str, str]:
    with path.open(encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise SystemExit(f"CSV has no header: {path}")
        sym_key = sec_key = None
        for k in reader.fieldnames:
            kl = (k or "").strip().lower()
            if kl == "symbol":
                sym_key = k
            if kl == "sector":
                sec_key = k
        if not sym_key or not sec_key:
            raise SystemExit(f"Need Symbol + sector columns; got {reader.fieldnames}")
        out: Dict[str, str] = {}
        for row in reader:
            sym = (row.get(sym_key) or "").strip().upper()
            sec = (row.get(sec_key) or "").strip()
            if sym and sec:
                out[sym] = sec
        return out


def _ensure_sector_column(conn, table: str) -> bool:
    insp = inspect(conn)
    cols = {c["name"] for c in insp.get_columns(table)}
    if "sector" in cols:
        return False
    conn.execute(text(f"ALTER TABLE {table} ADD COLUMN sector TEXT"))
    return True


def _sector_index_for_symbol(
    sym: str,
    sector_name: Optional[str],
    fno_map: Dict[str, str],
) -> Optional[str]:
    if sym in fno_map:
        return normalize_sector_instrument_key(fno_map[sym])
    static = equity_sector_index_instrument_key(sym)
    if static:
        return normalize_sector_instrument_key(static)
    if sector_name:
        key = CSV_SECTOR_TO_INDEX.get(sector_name.strip().lower())
        if key:
            return normalize_sector_instrument_key(key)
    return None


def _index_to_csv_sector_map(
    liquid_sectors: Dict[str, str],
    fno_map: Dict[str, str],
) -> Dict[str, str]:
    """Map sector_index instrument_key → FnO Liquid CSV sector label."""
    out: Dict[str, str] = {}
    for sym, sec in liquid_sectors.items():
        idx = fno_map.get(sym)
        if idx:
            out[idx] = sec
    return out


def _apply_table(
    conn,
    table: str,
    liquid_sectors: Dict[str, str],
    fno_map: Dict[str, str],
    index_to_csv: Dict[str, str],
    *,
    dry_run: bool,
) -> Tuple[int, int, int]:
    """Returns (sector_updated, sector_index_updated, still_missing_sector)."""
    rows = conn.execute(
        text(f"SELECT stock FROM {table} ORDER BY stock")
    ).fetchall()
    sector_up = 0
    idx_up = 0
    missing_sector = 0

    for (stock,) in rows:
        sym = str(stock or "").strip().upper()
        if not sym:
            continue
        row = conn.execute(
            text(
                f"""
                SELECT sector, sector_index
                FROM {table}
                WHERE UPPER(TRIM(stock)) = :s
                """
            ),
            {"s": sym},
        ).mappings().first()
        if not row:
            continue

        cur_sector = (row.get("sector") or "").strip()
        cur_idx = (row.get("sector_index") or "").strip()
        csv_sector = liquid_sectors.get(sym, "").strip()
        override = SYMBOL_SECTOR_OVERRIDES.get(sym)

        new_idx = cur_idx
        if not new_idx:
            if override:
                new_idx = normalize_sector_instrument_key(override[1]) or override[1]
            elif sym in fno_map:
                new_idx = normalize_sector_instrument_key(fno_map[sym]) or fno_map[sym]
            else:
                new_idx = _sector_index_for_symbol(sym, csv_sector, fno_map) or ""

        new_sector = cur_sector
        if csv_sector:
            new_sector = csv_sector
        elif override:
            new_sector = override[0]
        elif new_idx and new_idx in index_to_csv:
            new_sector = index_to_csv[new_idx]
        elif not new_sector and new_idx:
            new_sector = new_idx.split("|", 1)[-1].strip()

        if not new_sector:
            missing_sector += 1

        needs_sector = new_sector and new_sector != cur_sector
        needs_idx = new_idx and new_idx != cur_idx

        if dry_run:
            if needs_sector or needs_idx:
                print(f"  {table} {sym}: sector={new_sector!r} sector_index={new_idx!r}")
            continue

        if needs_sector:
            conn.execute(
                text(f"UPDATE {table} SET sector = :sec WHERE UPPER(TRIM(stock)) = :s"),
                {"s": sym, "sec": new_sector},
            )
            sector_up += 1
        if needs_idx:
            conn.execute(
                text(f"UPDATE {table} SET sector_index = :idx WHERE UPPER(TRIM(stock)) = :s"),
                {"s": sym, "idx": new_idx},
            )
            idx_up += 1

    return sector_up, idx_up, missing_sector


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--csv", required=True, type=Path)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    if not args.csv.is_file():
        raise SystemExit(f"CSV not found: {args.csv}")

    liquid_sectors = _load_fno_liquid_sectors(args.csv)
    fno_map = load_fno_sector_index_map()
    index_to_csv = _index_to_csv_sector_map(liquid_sectors, fno_map)
    print(f"csv_sectors={len(liquid_sectors)} fno_sector_mapping={len(fno_map)} index_to_csv={len(index_to_csv)}")

    with engine.begin() as conn:
        for table in TABLES:
            added = _ensure_sector_column(conn, table)
            if added:
                print(f"{table}: added column sector")
            elif not args.dry_run:
                print(f"{table}: sector column already present")

        totals = {"sector": 0, "idx": 0, "missing": 0}
        for table in TABLES:
            s, i, m = _apply_table(
                conn, table, liquid_sectors, fno_map, index_to_csv, dry_run=args.dry_run
            )
            totals["sector"] += s
            totals["idx"] += i
            totals["missing"] += m
            print(f"{table}: sector_updated={s} sector_index_updated={i} still_missing_sector={m}")

    if not args.dry_run:
        with engine.connect() as conn:
            for table in TABLES:
                n = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
                sf = conn.execute(
                    text(
                        f"""
                        SELECT COUNT(*) FROM {table}
                        WHERE sector IS NOT NULL AND length(trim(sector)) > 0
                        """
                    )
                ).scalar()
                si = conn.execute(
                    text(
                        f"""
                        SELECT COUNT(*) FROM {table}
                        WHERE sector_index IS NOT NULL AND length(trim(sector_index)) > 0
                        """
                    )
                ).scalar()
                print(f"verify {table}: rows={n} sector_filled={sf} sector_index_filled={si}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

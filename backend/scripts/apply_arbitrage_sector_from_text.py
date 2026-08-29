#!/usr/bin/env python3
"""
Update arbitrage_master.sector + sector_index from a grouped text file.

Format (blank lines ignored):
  ==SECTOR_NAME
  SYMBOL1
  SYMBOL2
  ==NEXT_SECTOR
  ...

Lines starting with ``==`` set the sector name (prefix stripped). Following lines are NSE symbols
until the next ``==`` line. Optional TradingView suffix ``1!`` is stripped from symbols.

Usage:
  PYTHONPATH=. python backend/scripts/apply_arbitrage_sector_from_text.py \\
      --file /path/to/arbitrage_master_by_sector.txt [--dry-run]
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import Dict, List, Tuple

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import text  # noqa: E402

from backend.database import engine  # noqa: E402
from backend.services.sector_movers import normalize_sector_instrument_key  # noqa: E402

# == header (uppercase) → Upstox sector_index instrument_key
SECTOR_HEADER_TO_INDEX: Dict[str, str] = {
    "AUTO": "NSE_INDEX|Nifty Auto",
    "CONSUMER_DURABLES": "NSE_INDEX|NIFTY CONSR DURBL",
    "ENERGY": "NSE_INDEX|Nifty Energy",
    "FINANCIALS": "NSE_INDEX|Nifty Fin Service",
    "FMCG": "NSE_INDEX|Nifty FMCG",
    "HEALTHCARE": "NSE_INDEX|NIFTY HEALTHCARE",
    "INDUSTRIALS": "NSE_INDEX|Nifty Infra",
    "IT": "NSE_INDEX|Nifty IT",
    "METAL": "NSE_INDEX|Nifty Metal",
    "PSU_BANK": "NSE_INDEX|Nifty PSU Bank",
    "PVT_BANK": "NSE_INDEX|Nifty Pvt Bank",
    "REALITY": "NSE_INDEX|Nifty Realty",
    "REALTY": "NSE_INDEX|Nifty Realty",
    "SERVICES": "NSE_INDEX|Nifty Serv Sector",
    "COMMODITIES": "NSE_INDEX|Nifty Chemicals",
    "CHEMICALS": "NSE_INDEX|Nifty Chemicals",
    "OIL_GAS": "NSE_INDEX|NIFTY OIL AND GAS",
    "TELECOM": "NSE_INDEX|Nifty MS IT Telcm",
}

_TV_SUFFIX = re.compile(r"1!$")


def _normalize_symbol(raw: str) -> str:
    s = (raw or "").strip().upper()
    s = _TV_SUFFIX.sub("", s)
    return s.strip()


def _sector_index_for_header(header: str) -> str:
    key = (header or "").strip().upper().replace(" ", "_")
    raw = SECTOR_HEADER_TO_INDEX.get(key)
    if not raw:
        raise ValueError(f"No sector_index mapping for header {header!r}")
    return normalize_sector_instrument_key(raw) or raw


def _header_name(line: str) -> str:
    if line.startswith("=="):
        return line[2:].strip()
    return line.strip()


def _is_sector_header_line(line: str) -> bool:
    if line.startswith("=="):
        return True
    key = line.strip().upper().replace(" ", "_")
    return key in SECTOR_HEADER_TO_INDEX


def parse_sector_file(path: Path) -> List[Tuple[str, str, List[str]]]:
    """
    Returns list of (sector_name, sector_index, [symbols]).
    Sector headers: ``==NAME`` or a known sector label line (e.g. Commodities).
    """
    blocks: List[Tuple[str, str, List[str]]] = []
    cur_name: str | None = None
    cur_syms: List[str] = []

    def _flush(name: str | None, syms: List[str]) -> None:
        if not name or not syms:
            return
        idx = _sector_index_for_header(name)
        blocks.append((name.strip(), idx, syms))

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if _is_sector_header_line(line):
            _flush(cur_name, cur_syms)
            cur_name = _header_name(line)
            cur_syms = []
            continue
        if cur_name is None:
            raise ValueError(f"Symbol {line!r} before first sector header")
        sym = _normalize_symbol(line)
        if sym:
            cur_syms.append(sym)

    _flush(cur_name, cur_syms)
    return blocks


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--file", required=True, type=Path)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    if not args.file.is_file():
        raise SystemExit(f"File not found: {args.file}")

    blocks = parse_sector_file(args.file)
    sym_to_sector: Dict[str, Tuple[str, str]] = {}
    for sector_name, sector_index, symbols in blocks:
        for sym in symbols:
            if sym in sym_to_sector:
                prev = sym_to_sector[sym]
                raise SystemExit(
                    f"Duplicate symbol {sym}: {prev[0]!r} vs {sector_name!r}"
                )
            sym_to_sector[sym] = (sector_name, sector_index)

    print(f"blocks={len(blocks)} symbols={len(sym_to_sector)}")
    for sector_name, sector_index, symbols in blocks:
        print(f"  {sector_name} [{sector_index}] n={len(symbols)}")

    with engine.connect() as conn:
        master_syms = {
            r[0]
            for r in conn.execute(
                text(
                    "SELECT UPPER(TRIM(stock)) FROM arbitrage_master "
                    "WHERE stock IS NOT NULL AND length(trim(stock)) > 0"
                )
            ).fetchall()
        }

    missing_in_file = sorted(master_syms - set(sym_to_sector))
    extra_in_file = sorted(set(sym_to_sector) - master_syms)
    if missing_in_file:
        print(f"warning: {len(missing_in_file)} in DB not in file:", missing_in_file)
    if extra_in_file:
        print(f"warning: {len(extra_in_file)} in file not in DB:", extra_in_file)

    updated = 0
    if args.dry_run:
        print("dry-run: no DB writes")
        return 0

    with engine.begin() as conn:
        for sym, (sector_name, sector_index) in sorted(sym_to_sector.items()):
            if sym not in master_syms:
                continue
            rc = conn.execute(
                text(
                    """
                    UPDATE arbitrage_master
                    SET sector = :sector, sector_index = :idx
                    WHERE UPPER(TRIM(stock)) = :sym
                    """
                ),
                {"sym": sym, "sector": sector_name, "idx": sector_index},
            ).rowcount
            updated += int(rc or 0)

    print(f"updated={updated}")
    with engine.connect() as conn:
        sample = conn.execute(
            text(
                """
                SELECT sector, sector_index, COUNT(*) AS n
                FROM arbitrage_master
                GROUP BY sector, sector_index
                ORDER BY sector
                """
            )
        ).mappings().all()
        for r in sample:
            print(dict(r))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

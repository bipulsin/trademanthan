#!/usr/bin/env python3
"""
Apply swing-trading CSV columns into iron_condor_universe_master.

CSV column INFY maps to Postgres symbol INFOSYS (same underlying as sheet).

Usage (from repo root):
  python scripts/ic_universe_master_apply_sheet9.py
  python scripts/ic_universe_master_apply_sheet9.py --csv ./scripts/ic_universe_sheet9.csv
"""
from __future__ import annotations

import argparse
import csv
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import text  # noqa: E402

from backend.database import SessionLocal  # noqa: E402
from backend.services.iron_condor_service import (  # noqa: E402
    ensure_iron_condor_tables,
    refresh_ic_universe_master_memory,
)

# Sheet uses NSE ticker INFY; Iron Condor universe uses INFOSYS.
SHEET_SYM_TO_DB = {"INFY": "INFOSYS"}
DEFAULT_CSV = ROOT / "scripts" / "ic_universe_sheet9.csv"


def parse_dd_mon_yy(cell: str):
    s = (cell or "").strip()
    if not s:
        return None
    for fmt in ("%d-%b-%y", "%d-%B-%y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"Unparseable date {cell!r}")


def db_symbol(sheet_symbol: str) -> str:
    u = sheet_symbol.strip().upper()
    return SHEET_SYM_TO_DB.get(u, u)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", type=Path, default=DEFAULT_CSV, help="Sheet9 CSV path")
    args = ap.parse_args()
    path = args.csv
    if not path.is_file():
        print(f"CSV not found: {path}", file=sys.stderr)
        return 2

    upd = text(
        """
        UPDATE iron_condor_universe_master SET
          curr_month_open = :curr_month_open,
          prev_mth_close = :prev_mth_close,
          prev_mth_expiry = :prev_mth_expiry,
          curr_mth_expiry = :curr_mth_expiry,
          nxt_earning_date = :nxt_earning_date,
          updated_at = CURRENT_TIMESTAMP
        WHERE symbol = :symbol
        """
    )

    with path.open(newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        sheet_rows = list(rdr)

    db = SessionLocal()
    try:
        ensure_iron_condor_tables()
        touched = 0
        missing: list[str] = []
        for row in sheet_rows:
            raw_sym = (row.get("symbol") or "").strip()
            if not raw_sym:
                continue
            sym = db_symbol(raw_sym)
            curr_open = (row.get("curr_month_open") or "").strip()
            prev_close = (row.get("prev_mth_close") or "").strip()
            try:
                pme = parse_dd_mon_yy(row.get("prev_mth_expiry") or "")
                cme = parse_dd_mon_yy(row.get("curr_mth_expiry") or "")
                ned = parse_dd_mon_yy(row.get("nxt_earning_date") or "")
            except ValueError as e:
                print(f"SKIP {raw_sym}->{sym}: {e}", file=sys.stderr)
                continue
            chk = db.execute(
                text("SELECT 1 FROM iron_condor_universe_master WHERE symbol = :s"), {"s": sym}
            ).scalar()
            if not chk:
                missing.append(sym)
                continue
            db.execute(
                upd,
                {
                    "symbol": sym,
                    "curr_month_open": float(curr_open) if curr_open else None,
                    "prev_mth_close": float(prev_close) if prev_close else None,
                    "prev_mth_expiry": pme,
                    "curr_mth_expiry": cme,
                    "nxt_earning_date": ned,
                },
            )
            touched += 1
        db.commit()
        refresh_ic_universe_master_memory(db)
        print(f"Updated {touched} row(s).")
        if missing:
            print(f"Symbols not in master (skipped): {', '.join(missing)}", file=sys.stderr)
    finally:
        db.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""
Sync arbitrage_master to an FnO Liquid CSV symbol list.

- Symbols in CSV but missing from arbitrage_master → INSERT into arbitrage_master
- Symbols in arbitrage_master but not in CSV → copy full row to arbitrage_lowliquid, then DELETE from master

Usage (repo root / container):
  PYTHONPATH=. python backend/scripts/sync_arbitrage_master_from_fno_liquid_csv.py \\
      --csv /tmp/fno_liquid.csv [--dry-run] [--skip-daily-setup]
"""
from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path
from typing import Set

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import text  # noqa: E402

from backend.database import engine  # noqa: E402


def _load_csv_symbols(path: Path) -> Set[str]:
    with path.open(encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise SystemExit(f"CSV has no header: {path}")
        sym_key = None
        for k in reader.fieldnames:
            if (k or "").strip().lower() == "symbol":
                sym_key = k
                break
        if not sym_key:
            raise SystemExit(f"No Symbol column in {path}; got {reader.fieldnames}")
        out: Set[str] = set()
        for row in reader:
            s = (row.get(sym_key) or "").strip().upper()
            if s:
                out.add(s)
        return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--csv", required=True, type=Path, help="FnO Liquid CSV path")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument(
        "--skip-daily-setup",
        action="store_true",
        help="Do not refresh Upstox keys/LTPs for newly inserted symbols",
    )
    args = ap.parse_args()
    if not args.csv.is_file():
        raise SystemExit(f"CSV not found: {args.csv}")

    csv_syms = _load_csv_symbols(args.csv)
    print(f"csv_symbols={len(csv_syms)}")

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS arbitrage_lowliquid
                (LIKE arbitrage_master INCLUDING ALL)
                """
            )
        )
        # Ensure PK on stock if LIKE did not attach one on older PG / empty template
        conn.execute(
            text(
                """
                DO $$
                BEGIN
                  IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint
                    WHERE conrelid = 'public.arbitrage_lowliquid'::regclass
                      AND contype = 'p'
                  ) THEN
                    ALTER TABLE arbitrage_lowliquid
                      ADD CONSTRAINT arbitrage_lowliquid_pkey PRIMARY KEY (stock);
                  END IF;
                EXCEPTION WHEN duplicate_table OR duplicate_object THEN
                  NULL;
                END $$;
                """
            )
        )

        master = {
            r[0]
            for r in conn.execute(text("SELECT UPPER(TRIM(stock)) FROM arbitrage_master")).fetchall()
        }
        to_add = sorted(csv_syms - master)
        to_move = sorted(master - csv_syms)
        print(f"master_before={len(master)} to_add={len(to_add)} to_move={len(to_move)}")
        print("to_add:", to_add)
        print("to_move:", to_move)

        if args.dry_run:
            print("dry-run: no writes")
            return 0

        moved = 0
        for s in to_move:
            conn.execute(
                text("DELETE FROM arbitrage_lowliquid WHERE UPPER(TRIM(stock)) = :s"),
                {"s": s},
            )
            conn.execute(
                text(
                    """
                    INSERT INTO arbitrage_lowliquid
                    SELECT * FROM arbitrage_master
                    WHERE UPPER(TRIM(stock)) = :s
                    """
                ),
                {"s": s},
            )
            deleted = conn.execute(
                text("DELETE FROM arbitrage_master WHERE UPPER(TRIM(stock)) = :s"),
                {"s": s},
            ).rowcount
            moved += int(deleted or 0)

        added = 0
        for s in to_add:
            r = conn.execute(
                text(
                    """
                    INSERT INTO arbitrage_master (stock)
                    VALUES (:s)
                    ON CONFLICT (stock) DO NOTHING
                    """
                ),
                {"s": s},
            )
            added += int(r.rowcount or 0)

        master_after = conn.execute(text("SELECT COUNT(*) FROM arbitrage_master")).scalar()
        low_after = conn.execute(text("SELECT COUNT(*) FROM arbitrage_lowliquid")).scalar()
        print(f"moved={moved} added={added} master_after={master_after} lowliquid_after={low_after}")

    if to_add and not args.skip_daily_setup:
        from backend.services.arbitrage_daily_setup_scheduler import (  # noqa: E402
            run_arbitrage_daily_setup_now,
        )

        out = run_arbitrage_daily_setup_now()
        print("run_arbitrage_daily_setup_now:", out)

        with engine.connect() as conn:
            for s in to_add:
                row = conn.execute(
                    text(
                        """
                        SELECT stock,
                               stock_instrument_key,
                               currmth_future_instrument_key,
                               nextmth_future_instrement_key
                        FROM arbitrage_master
                        WHERE UPPER(TRIM(stock)) = :s
                        """
                    ),
                    {"s": s},
                ).mappings().first()
                print("verify_add", dict(row) if row else {"stock": s, "missing": True})

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

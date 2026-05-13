#!/usr/bin/env python3
"""
Delete Smart Futures picker rows from smart_futures_daily (and matching watchlist keys).

Match fut_symbol with a flexible ILIKE pattern: tokens from the search string joined by %.

Usage (repo root on server with DATABASE_URL):
  PYTHONPATH=. python3 backend/scripts/delete_smart_futures_daily_by_fut_symbol.py \\
    --fut-symbol "BHEL FUT 26 MAY 26" --dry-run
  PYTHONPATH=. python3 backend/scripts/delete_smart_futures_daily_by_fut_symbol.py \\
    --fut-symbol "BHEL FUT 26 MAY 26" --execute
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from sqlalchemy import bindparam, text

from backend.database import SessionLocal


def _ilike_tokens(s: str) -> str:
    """BHEL FUT 26 MAY 26 -> %BHEL%FUT%26%MAY%26%"""
    parts = [p for p in re.split(r"\s+", s.strip()) if p]
    if not parts:
        raise ValueError("empty fut_symbol pattern")
    return "%" + "%".join(parts) + "%"


def main() -> int:
    p = argparse.ArgumentParser(description="Delete smart_futures_daily rows by fut_symbol match.")
    p.add_argument(
        "--fut-symbol",
        type=str,
        default="BHEL FUT 26 MAY 26",
        help="Display/substring match for fut_symbol (token order preserved, case-insensitive).",
    )
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--dry-run", action="store_true", help="Print matching rows only; no deletes.")
    g.add_argument("--execute", action="store_true", help="Perform DELETE after listing matches.")
    args = p.parse_args()

    pattern = _ilike_tokens(args.fut_symbol)
    db = SessionLocal()
    try:
        sel = db.execute(
            text(
                """
                SELECT id, session_date, stock, fut_symbol, fut_instrument_key, order_status
                  FROM smart_futures_daily
                 WHERE fut_symbol IS NOT NULL
                   AND fut_symbol ILIKE :pat
                 ORDER BY session_date DESC, id DESC
                """
            ),
            {"pat": pattern},
        ).mappings().all()

        if not sel:
            print(f"No rows found for fut_symbol ILIKE {pattern!r}")
            return 1

        print(f"Matched {len(sel)} row(s) (pattern ILIKE {pattern!r}):")
        for row in sel:
            print(
                f"  id={row['id']} session_date={row['session_date']} stock={row['stock']!r} "
                f"fut_symbol={row['fut_symbol']!r} ikey={row['fut_instrument_key']!r} "
                f"order_status={row['order_status']!r}"
            )

        if args.dry_run:
            print("--dry-run: no changes.")
            return 0

        ikeys = sorted({str(r["fut_instrument_key"]) for r in sel if r.get("fut_instrument_key")})
        ids = [int(r["id"]) for r in sel]

        if ikeys:
            del_wl = (
                text(
                    "DELETE FROM smart_futures_watchlist WHERE fut_instrument_key IN :ikeys RETURNING id"
                ).bindparams(bindparam("ikeys", expanding=True))
            )
            wr = db.execute(del_wl, {"ikeys": ikeys}).fetchall()
            print(f"Deleted {len(wr)} smart_futures_watchlist row(s).")

        del_daily = (
            text("DELETE FROM smart_futures_daily WHERE id IN :ids RETURNING id").bindparams(
                bindparam("ids", expanding=True)
            )
        )
        dr = db.execute(del_daily, {"ids": ids}).fetchall()
        print(f"Deleted {len(dr)} smart_futures_daily row(s): {[r[0] for r in dr]}")
        db.commit()
        return 0
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())

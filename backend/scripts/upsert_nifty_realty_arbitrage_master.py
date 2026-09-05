#!/usr/bin/env python3
"""
Ensure Nifty Realty F&O constituents live in arbitrage_master (not lowliquid),
then refresh EQ/FUT keys via the standard metadata roll (no LTP).

Nifty Realty has 10 index names; only these six have NSE stock futures:
  DLF, LODHA, GODREJPROP, OBEROIRLTY, PHOENIXLTD, PRESTIGE

Sector label matches existing master rows: REALITY.
sector_index: NSE_INDEX|Nifty Realty.

Usage (repo root / app container):
  PYTHONPATH=. python backend/scripts/upsert_nifty_realty_arbitrage_master.py
  PYTHONPATH=. python backend/scripts/upsert_nifty_realty_arbitrage_master.py --skip-roll
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import bindparam, inspect, text  # noqa: E402

from backend.database import engine  # noqa: E402

REALTY_FNO = (
    "DLF",
    "LODHA",
    "GODREJPROP",
    "OBEROIRLTY",
    "PHOENIXLTD",
    "PRESTIGE",
)
SECTOR = "REALITY"
SECTOR_INDEX = "NSE_INDEX|Nifty Realty"


def _common_columns(conn) -> list[str]:
    insp = inspect(conn)
    if not insp.has_table("arbitrage_lowliquid"):
        return []
    master = {c["name"] for c in insp.get_columns("arbitrage_master")}
    low = {c["name"] for c in insp.get_columns("arbitrage_lowliquid")}
    shared = [c["name"] for c in insp.get_columns("arbitrage_master") if c["name"] in low]
    return shared


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--skip-roll",
        action="store_true",
        help="Do not refresh Upstox EQ/FUT keys after the SQL upsert",
    )
    args = ap.parse_args()

    with engine.begin() as conn:
        for s in REALTY_FNO:
            conn.execute(
                text(
                    """
                    INSERT INTO arbitrage_master (stock, sector, sector_index, sector_instrument_key)
                    VALUES (:s, :sector, :idx, :idx)
                    ON CONFLICT (stock) DO NOTHING
                    """
                ),
                {"s": s, "sector": SECTOR, "idx": SECTOR_INDEX},
            )

        shared = _common_columns(conn)
        copy_cols = [
            c
            for c in shared
            if c
            not in (
                "stock",
                "sector",
                "sector_index",
                "sector_instrument_key",
            )
        ]
        if copy_cols:
            sets = ", ".join(f"{c} = l.{c}" for c in copy_cols)
            conn.execute(
                text(
                    f"""
                    UPDATE arbitrage_master m
                    SET {sets}
                    FROM arbitrage_lowliquid l
                    WHERE UPPER(TRIM(m.stock)) = UPPER(TRIM(l.stock))
                      AND UPPER(TRIM(m.stock)) IN :syms
                    """
                ).bindparams(bindparam("syms", expanding=True)),
                {"syms": list(REALTY_FNO)},
            )
            deleted = conn.execute(
                text(
                    """
                    DELETE FROM arbitrage_lowliquid
                    WHERE UPPER(TRIM(stock)) IN :syms
                    """
                ).bindparams(bindparam("syms", expanding=True)),
                {"syms": list(REALTY_FNO)},
            ).rowcount
            print(f"copied_from_lowliquid then deleted={deleted}")
        else:
            print("arbitrage_lowliquid missing or no shared columns; skip move")

        tagged = conn.execute(
            text(
                """
                UPDATE arbitrage_master
                SET sector = :sector,
                    sector_index = :idx,
                    sector_instrument_key = :idx
                WHERE UPPER(TRIM(stock)) IN :syms
                """
            ).bindparams(bindparam("syms", expanding=True)),
            {"sector": SECTOR, "idx": SECTOR_INDEX, "syms": list(REALTY_FNO)},
        ).rowcount
        print(f"sector_tagged={tagged} sector={SECTOR} sector_index={SECTOR_INDEX}")

    if not args.skip_roll:
        from backend.services.arbitrage_daily_setup_scheduler import (  # noqa: E402
            run_arbitrage_metadata_roll_now,
        )

        out = run_arbitrage_metadata_roll_now(apply_roll_window=True)
        print("run_arbitrage_metadata_roll_now:", out)

    with engine.begin() as conn:
        if inspect(conn).has_table("car_nifty200"):
            for s in REALTY_FNO:
                conn.execute(
                    text(
                        """
                        INSERT INTO car_nifty200 (stock, stock_instrument_key, stock_ltp)
                        SELECT m.stock, m.stock_instrument_key, m.stock_ltp
                        FROM arbitrage_master m
                        WHERE UPPER(TRIM(m.stock)) = :s
                        ON CONFLICT (stock) DO UPDATE SET
                            stock_instrument_key = EXCLUDED.stock_instrument_key,
                            stock_ltp = EXCLUDED.stock_ltp
                        """
                    ),
                    {"s": s},
                )
            print("car_nifty200 upserted for Realty F&O names")

        rows = conn.execute(
            text(
                """
                SELECT stock, sector, sector_index,
                       stock_instrument_key,
                       currmth_future_instrument_key,
                       nextmth_future_instrement_key
                FROM arbitrage_master
                WHERE UPPER(TRIM(stock)) IN :syms
                ORDER BY stock
                """
            ).bindparams(bindparam("syms", expanding=True)),
            {"syms": list(REALTY_FNO)},
        ).mappings().all()
        for r in rows:
            print("verify", dict(r))

        leftover = conn.execute(
            text(
                """
                SELECT stock FROM arbitrage_lowliquid
                WHERE UPPER(TRIM(stock)) IN :syms
                ORDER BY stock
                """
            ).bindparams(bindparam("syms", expanding=True)),
            {"syms": list(REALTY_FNO)},
        ).fetchall()
        print("still_in_lowliquid:", [x[0] for x in leftover])

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

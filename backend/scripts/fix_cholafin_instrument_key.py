#!/usr/bin/env python3
"""
One-time fix: Update CHOLAFIN's instrument_key from instruments JSON in all DB tables.
Reads nse_instruments.json, finds instrument_key for CHOLAFIN (NSE_EQ), then updates:
- arbitrage_master.stock_instrument_key
- car_nifty200.stock_instrument_key
- arbitrage_order.stock_instrument_key (where it was the old key)
"""
import sys
import os
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))
os.chdir(project_root)

SYMBOL = "CHOLAFIN"


def get_instrument_key_from_file(instruments_path: Path):
    """Get instrument_key for SYMBOL from instruments JSON (NSE_EQ segment)."""
    if not instruments_path.exists():
        return None
    import json
    with open(instruments_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        return None
    sym_upper = SYMBOL.strip().upper()
    variations = [sym_upper, sym_upper + "-EQ"]
    for inst in data:
        if (inst.get("segment") or "").strip() != "NSE_EQ":
            continue
        ts = (inst.get("trading_symbol") or inst.get("tradingsymbol") or "").strip().upper()
        ts_clean = ts.replace("-EQ", "").replace("-FUT", "").replace("-OPT", "")
        if ts in variations or ts_clean == sym_upper:
            ik = inst.get("instrument_key")
            if ik:
                return ik
    return None


def main():
    from backend.database import engine
    from backend.config import get_instruments_file_path
    from sqlalchemy import text

    instruments_path = get_instruments_file_path()
    new_key = get_instrument_key_from_file(instruments_path)
    if not new_key:
        print(f"ERROR: Could not find instrument_key for {SYMBOL} in {instruments_path}")
        return 1

    print(f"Found instrument_key for {SYMBOL} in instruments file: {new_key}")

    with engine.begin() as conn:
        # Get current key from arbitrage_master (old/wrong key)
        row = conn.execute(
            text("SELECT stock_instrument_key FROM arbitrage_master WHERE stock = :s"),
            {"s": SYMBOL},
        ).mappings().first()
        old_key = (row["stock_instrument_key"] or "").strip() if row else None

        if old_key == new_key:
            print(f"No change: arbitrage_master already has {new_key}")
            return 0

        # 1. arbitrage_master
        r1 = conn.execute(
            text("UPDATE arbitrage_master SET stock_instrument_key = :new WHERE stock = :s"),
            {"new": new_key, "s": SYMBOL},
        )
        print(f"arbitrage_master: updated {r1.rowcount} row(s)")

        # 2. car_nifty200
        r2 = conn.execute(
            text("UPDATE car_nifty200 SET stock_instrument_key = :new WHERE stock = :s"),
            {"new": new_key, "s": SYMBOL},
        )
        print(f"car_nifty200: updated {r2.rowcount} row(s)")

        # 3. arbitrage_order (where stock_instrument_key was the old key)
        if old_key:
            r3 = conn.execute(
                text("UPDATE arbitrage_order SET stock_instrument_key = :new WHERE stock_instrument_key = :old"),
                {"new": new_key, "old": old_key},
            )
            print(f"arbitrage_order: updated {r3.rowcount} row(s)")
        else:
            print("arbitrage_order: no old key to replace, skipped")

    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

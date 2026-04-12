#!/usr/bin/env python3
"""
Recalculate smart_futures_daily.sector_score using arbitrage_master.sector_index (Upstox
instrument key) when present, else the same fallbacks as smart_futures_picker.sector_score.

  sector_raw = (1d_return * 0.6) + (5d_return * 0.4)
  sector_score = clamp(sector_raw / 5.0, -1, 1)

Also refreshes final_cms = cms * (1 + 0.5 * sector_score) * (1 + combined_sentiment).

Usage (repo root):
  PYTHONPATH=. python backend/scripts/recalculate_smart_futures_daily_sector_score.py
  PYTHONPATH=. python backend/scripts/recalculate_smart_futures_daily_sector_score.py --dry-run
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import text  # noqa: E402

from backend.database import SessionLocal  # noqa: E402
from backend.services.smart_futures_picker.sector_score import (  # noqa: E402
    compute_sector_score_from_instrument_key,
    resolve_sector_instrument_key_for_stock,
)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true", help="Print changes only, do not commit")
    ap.add_argument(
        "--sleep-secs",
        type=float,
        default=0.15,
        help="Pause between distinct Upstox sector index calls (default 0.15)",
    )
    args = ap.parse_args()

    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT s.id, s.stock, s.cms, s.combined_sentiment, m.sector_index
                FROM smart_futures_daily s
                LEFT JOIN arbitrage_master m
                  ON UPPER(TRIM(m.stock)) = UPPER(TRIM(s.stock))
                ORDER BY s.id
                """
            )
        ).fetchall()
    finally:
        db.close()

    if not rows:
        print("No rows in smart_futures_daily.")
        return 0

    score_by_ikey: dict[str, float] = {}
    sleep_secs = max(0.0, float(args.sleep_secs))

    def sector_score_cached(stock: str, sector_index: object) -> float:
        st = str(stock or "").strip().upper()
        si = str(sector_index).strip() if sector_index else None
        ikey = resolve_sector_instrument_key_for_stock(st, si or None)
        if not ikey:
            return 0.0
        if ikey in score_by_ikey:
            return score_by_ikey[ikey]
        if sleep_secs and score_by_ikey:
            time.sleep(sleep_secs)
        sc = compute_sector_score_from_instrument_key(ikey)
        score_by_ikey[ikey] = sc
        return sc

    updates: list[tuple] = []
    for rid, stock, cms, comb_sent, sector_index in rows:
        si = str(sector_index).strip() if sector_index else None
        ss = sector_score_cached(stock, sector_index)
        try:
            cms_f = float(cms) if cms is not None else 0.0
        except (TypeError, ValueError):
            cms_f = 0.0
        try:
            comb = float(comb_sent) if comb_sent is not None else 0.0
        except (TypeError, ValueError):
            comb = 0.0
        final_cms = cms_f * (1.0 + 0.5 * ss) * (1.0 + comb)
        src = si if si else "(resolve)"
        updates.append((rid, stock, ss, final_cms, src))

    print(f"rows={len(updates)} distinct_sector_keys={len(score_by_ikey)} dry_run={args.dry_run}")
    for rid, stock, ss, fc, src in updates[:20]:
        print(f"  id={rid} stock={stock!r} sector={src!r} sector_score={ss:.6f} final_cms={fc:.6f}")
    if len(updates) > 20:
        print(f"  ... and {len(updates) - 20} more")

    if args.dry_run:
        return 0

    dbw = SessionLocal()
    try:
        for rid, stock, sector_score, final_cms, _src in updates:
            dbw.execute(
                text(
                    """
                    UPDATE smart_futures_daily
                    SET sector_score = :ss,
                        final_cms = :fc,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = :id
                    """
                ),
                {"id": rid, "ss": sector_score, "fc": final_cms},
            )
        dbw.commit()
    except Exception as e:
        dbw.rollback()
        print(f"ERROR: {e}")
        return 1
    finally:
        dbw.close()

    print("Committed updates.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

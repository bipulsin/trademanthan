#!/usr/bin/env python3
"""
Backfill volume_mismatch_signals in PostgreSQL for past session dates.

Live page reads from DB via GET /volume-mismatch-futures/signals; backtest writes
only to volume_mismatch_backtest.json. Use this script to sync historical sessions.

Usage::

    PYTHONPATH=. python backend/scripts/backfill_volume_mismatch_signals.py \\
        --date 2026-06-05

    PYTHONPATH=. python backend/scripts/backfill_volume_mismatch_signals.py \\
        --from-date 2026-05-01 --to-date 2026-06-06 --only-missing

    PYTHONPATH=. python backend/scripts/backfill_volume_mismatch_signals.py \\
        --date 2026-06-05 --dry-run
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List, Set

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import backend.env_bootstrap  # noqa: F401,E402

from backend.config import settings  # noqa: E402
from backend.database import SessionLocal  # noqa: E402
from backend.services.market_holiday import refresh_holiday_dates_from_db  # noqa: E402
from backend.services.upstox_service import UpstoxService  # noqa: E402
from backend.services.volume_mismatch.backtest import BACKTEST_DEFAULT_FROM  # noqa: E402
from backend.services.volume_mismatch.backtest_universe import (  # noqa: E402
    load_volume_mismatch_universe_for_session,
)
from backend.services.volume_mismatch.repository import upsert_signal  # noqa: E402
from backend.services.volume_mismatch.scanner import (  # noqa: E402
    collect_volume_mismatch_signals_for_date,
)
from backend.services.volume_mismatch.tables import ensure_volume_mismatch_signals_table  # noqa: E402
from backend.services.volume_mismatch.universe import load_volume_mismatch_universe  # noqa: E402

logger = logging.getLogger(__name__)


def _load_holiday_dates(upstox: UpstoxService, from_date: date, to_date: date) -> Set[date]:
    holidays = refresh_holiday_dates_from_db()
    if holidays:
        return holidays
    out: Set[date] = set()
    for year in range(from_date.year, to_date.year + 1):
        for dstr in upstox.get_market_holidays(year) or []:
            try:
                out.add(date.fromisoformat(str(dstr)[:10]))
            except ValueError:
                continue
    return out


def _iter_trading_days(d0: date, d1: date, holiday_dates: Set[date]) -> List[date]:
    out: List[date] = []
    d = d0
    while d <= d1:
        if d.weekday() < 5 and d not in holiday_dates:
            out.append(d)
        d += timedelta(days=1)
    return out


def _dates_with_signals(db, from_date: date, to_date: date) -> Dict[date, int]:
    from sqlalchemy import text

    ensure_volume_mismatch_signals_table(db)
    rows = db.execute(
        text(
            """
            SELECT trade_date, COUNT(*) AS cnt
            FROM volume_mismatch_signals
            WHERE trade_date >= :d0 AND trade_date <= :d1
            GROUP BY trade_date
            """
        ),
        {"d0": from_date, "d1": to_date},
    ).fetchall()
    return {r[0]: int(r[1]) for r in rows}


def backfill_session(
    session_date: date,
    *,
    use_backtest_universe: bool = True,
    dry_run: bool = False,
    max_workers: int = 24,
) -> Dict[str, object]:
    if use_backtest_universe:
        universe = load_volume_mismatch_universe_for_session(session_date)
    else:
        universe = load_volume_mismatch_universe()

    upstox = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    if not getattr(upstox, "access_token", None):
        return {"success": False, "error": "Upstox token unavailable", "trade_date": str(session_date)}

    signals = collect_volume_mismatch_signals_for_date(
        upstox,
        universe,
        session_date,
        max_workers=max_workers,
    )
    for row in signals:
        row["entry_status"] = "WAITING"

    symbols = sorted({str(r.get("symbol") or "") for r in signals if r.get("symbol")})
    summary: Dict[str, object] = {
        "success": True,
        "trade_date": str(session_date),
        "universe_count": len(universe),
        "signal_count": len(signals),
        "long_count": sum(1 for s in signals if s.get("direction") == "LONG"),
        "short_count": sum(1 for s in signals if s.get("direction") == "SHORT"),
        "symbols": symbols,
        "dry_run": dry_run,
    }

    if dry_run:
        return summary

    db = SessionLocal()
    try:
        ensure_volume_mismatch_signals_table(db)
        for row in signals:
            upsert_signal(db, session_date, row)
        db.commit()
    except Exception as e:
        db.rollback()
        logger.error("Backfill persist failed for %s: %s", session_date, e, exc_info=True)
        return {"success": False, "error": str(e), "trade_date": str(session_date)}
    finally:
        db.close()

    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", help="Single session YYYY-MM-DD")
    parser.add_argument(
        "--from-date",
        default=BACKTEST_DEFAULT_FROM.isoformat(),
        help=f"Range start (default {BACKTEST_DEFAULT_FROM.isoformat()})",
    )
    parser.add_argument(
        "--to-date",
        default=date.today().isoformat(),
        help="Range end YYYY-MM-DD (default today)",
    )
    parser.add_argument(
        "--only-missing",
        action="store_true",
        help="Skip dates that already have rows in volume_mismatch_signals",
    )
    parser.add_argument(
        "--live-universe",
        action="store_true",
        help="Use current arbitrage_master universe (default: session front-month, matches backtest)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Collect signals without DB write")
    parser.add_argument("--max-workers", type=int, default=24)
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    if args.date:
        session_dates = [date.fromisoformat(args.date)]
    else:
        from_date = date.fromisoformat(args.from_date)
        to_date = date.fromisoformat(args.to_date)
        if from_date > to_date:
            print("from_date must be <= to_date", file=sys.stderr)
            return 1
        upstox = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
        holidays = _load_holiday_dates(upstox, from_date, to_date)
        session_dates = _iter_trading_days(from_date, to_date, holidays)

    if args.only_missing and session_dates and not args.dry_run:
        db = SessionLocal()
        try:
            existing = _dates_with_signals(db, session_dates[0], session_dates[-1])
        finally:
            db.close()
        session_dates = [d for d in session_dates if existing.get(d, 0) == 0]
        logger.info("After --only-missing filter: %s session(s) to backfill", len(session_dates))

    if not session_dates:
        print("Nothing to backfill.")
        return 0

    use_backtest_universe = not args.live_universe
    results: List[Dict[str, object]] = []
    for sd in session_dates:
        logger.info("Backfilling %s (backtest_universe=%s)", sd, use_backtest_universe)
        res = backfill_session(
            sd,
            use_backtest_universe=use_backtest_universe,
            dry_run=args.dry_run,
            max_workers=args.max_workers,
        )
        results.append(res)
        if res.get("success"):
            logger.info(
                "%s: %s signals (%s)",
                sd,
                res.get("signal_count"),
                ", ".join(str(s) for s in (res.get("symbols") or [])),
            )
        else:
            logger.error("%s failed: %s", sd, res.get("error"))

    failed = [r for r in results if not r.get("success")]
    total_signals = sum(int(r.get("signal_count") or 0) for r in results if r.get("success"))
    print(
        f"Done: {len(results) - len(failed)}/{len(results)} days, "
        f"{total_signals} signals{' (dry-run)' if args.dry_run else ''}"
    )
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())

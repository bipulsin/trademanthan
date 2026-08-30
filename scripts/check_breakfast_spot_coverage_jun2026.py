#!/usr/bin/env python3
"""Report June 2026 spot 5m candle availability for Breakfast Strategy universe."""
from __future__ import annotations

import json
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.config import settings
from backend.services.breakfast_strategy.backtest import iter_session_dates
from backend.services.breakfast_strategy.candles import (
    default_cache_dir,
    ensure_5m_cached,
    session_has_stock_bars,
)
from backend.services.breakfast_strategy.universe import (
    build_instrument_indexes,
    load_arbitrage_by_sector,
    resolve_eq_spot_with_fut_lot,
)
from backend.services.upstox_service import UpstoxService

DATE_FROM = date(2026, 6, 1)
DATE_TO = date(2026, 6, 30)


def main() -> int:
    upstox = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    if not getattr(upstox, "access_token", None):
        print(json.dumps({"error": "Upstox token not configured"}))
        return 1

    session_dates = iter_session_dates(DATE_FROM, DATE_TO)
    stocks_by_sector = load_arbitrage_by_sector()
    fut_by_und, eq_by_symbol = build_instrument_indexes()
    cache_dir = default_cache_dir()

    syms = sorted(
        {
            str(m.get("stock") or "").upper()
            for members in stocks_by_sector.values()
            for m in members
            if m.get("stock")
        }
    )

    per_day_ok: dict[str, int] = {}
    per_sym_days: dict[str, int] = defaultdict(int)
    missing_syms: set[str] = set()

    for sd in session_dates:
        ok = 0
        for sym in syms:
            eq = resolve_eq_spot_with_fut_lot(sym, fut_by_und=fut_by_und, eq_by_symbol=eq_by_symbol)
            if not eq or not eq.instrument_key:
                missing_syms.add(sym)
                continue
            candles = ensure_5m_cached(
                upstox,
                cache_dir,
                eq.instrument_key,
                range_end=DATE_TO,
                range_start=DATE_FROM,
                session_dates=session_dates,
            )
            if session_has_stock_bars(candles, sd):
                ok += 1
                per_sym_days[sym] += 1
        per_day_ok[sd.isoformat()] = ok

    total_syms = len(syms)
    full_coverage_syms = sum(1 for s in syms if per_sym_days.get(s, 0) == len(session_dates))
    report = {
        "window": f"{DATE_FROM.isoformat()}..{DATE_TO.isoformat()}",
        "session_days": len(session_dates),
        "universe_symbols": total_syms,
        "symbols_with_full_june_spot_coverage": full_coverage_syms,
        "symbols_never_having_spot_bars": sorted(missing_syms),
        "per_session_spot_ready_symbol_count": per_day_ok,
        "min_per_day": min(per_day_ok.values()) if per_day_ok else 0,
        "max_per_day": max(per_day_ok.values()) if per_day_ok else 0,
    }
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

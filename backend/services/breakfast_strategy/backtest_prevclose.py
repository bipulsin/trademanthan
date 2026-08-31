"""May–Aug 2026 prev-close ranking backtest. Writes a dedicated artifact only.

Never writes breakfast_strategy_trades or breakfast_live_signals.
"""
from __future__ import annotations

import logging
from calendar import monthrange
from datetime import date, timedelta
from typing import Any, Dict, Iterator, List, Tuple

from backend.config import settings
from backend.services.breakfast_strategy.backtest import (
    _summary,
    collect_instrument_keys,
    iter_session_dates,
    warm_candle_cache,
    write_artifact,
)
from backend.services.breakfast_strategy.candles import default_cache_dir, load_cached_5m
from backend.services.breakfast_strategy.config import (
    PREVCLOSE_ARTIFACT_NAME,
    PREVCLOSE_DATE_FROM,
    PREVCLOSE_DATE_TO,
    PREVCLOSE_FUTURES_FROM,
    PREVCLOSE_SECTORS_TO_PICK,
    STOCKS_PER_SECTOR,
)
from backend.services.breakfast_strategy.engine import NIFTY50_KEY, TradeResult
from backend.services.breakfast_strategy.engine_prevclose import simulate_session_day_prevclose
from backend.services.breakfast_strategy.universe import (
    build_instrument_indexes,
    load_arbitrage_by_sector,
)
from backend.services.upstox_service import UpstoxService

logger = logging.getLogger(__name__)

COMPARABILITY_CAVEAT = (
    "Experimental parallel backtest. Nifty/sector rank = 9:20 close vs prev session close. "
    "Picks 2 sectors × 2 stocks (vs Primary 1×2, Live 2×3). "
    "May–Jul 2026 cash/spot proxy; Aug 2026 stock futures. "
    "Does not affect Live lock or Primary/History results."
)


def _month_windows(start: date, end: date) -> Iterator[Tuple[date, date]]:
    """Inclusive calendar-month slices so 5m fetches stay within Upstox's 31-day cap."""
    y, m = start.year, start.month
    while date(y, m, 1) <= end:
        last = monthrange(y, m)[1]
        yield max(start, date(y, m, 1)), min(end, date(y, m, last))
        if m == 12:
            y, m = y + 1, 1
        else:
            m += 1


def run_prevclose_backtest(
    *,
    date_from: date = PREVCLOSE_DATE_FROM,
    date_to: date = PREVCLOSE_DATE_TO,
    force_fetch: bool = False,
) -> Dict[str, Any]:
    upstox = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    if not getattr(upstox, "access_token", None):
        raise RuntimeError("Upstox access token not configured")

    cache_dir = default_cache_dir()
    stocks_by_sector = load_arbitrage_by_sector()
    fut_by_und, eq_by_symbol = build_instrument_indexes()
    session_dates = iter_session_dates(date_from, date_to)
    instrument_keys = collect_instrument_keys(
        session_dates,
        stocks_by_sector,
        fut_by_und,
        eq_by_symbol,
        spot_proxy_fallback=True,
    )
    eq_index_keys = {k for k in instrument_keys if k.startswith("NSE_EQ|") or k.startswith("NSE_INDEX|")}
    fo_index_keys = {k for k in instrument_keys if k.startswith("NSE_FO|") or k.startswith("NSE_INDEX|")}

    warm_stats: Dict[str, Any] = {"instruments": len(instrument_keys), "months": []}
    for ms, me in _month_windows(date_from, date_to):
        month_sessions = iter_session_dates(ms, me)
        if not month_sessions:
            continue
        # Spot months: EQ+index only. Aug futures: FO+index. Skip expired FO on earlier months.
        keys = fo_index_keys if me >= PREVCLOSE_FUTURES_FROM else eq_index_keys
        logger.info(
            "prevclose cache warm %s → %s (%s sessions, %s keys)",
            ms, me, len(month_sessions), len(keys),
        )
        month_stats = warm_candle_cache(
            upstox,
            cache_dir,
            keys,
            range_start=ms,
            range_end=me,
            session_dates=month_sessions,
            force=force_fetch,
        )
        warm_stats["months"].append({"from": ms.isoformat(), "to": me.isoformat(), **month_stats})

    candles_by_key = {ik: load_cached_5m(cache_dir, ik) for ik in instrument_keys}

    sector_candles = {
        ik: candles_by_key.get(ik, [])
        for ik in candles_by_key
        if ik.startswith("NSE_INDEX|")
    }
    nifty_candles = candles_by_key.get(NIFTY50_KEY, [])

    all_results: List[TradeResult] = []
    day_log: List[Dict[str, Any]] = []
    for sd in session_dates:
        spot = sd < PREVCLOSE_FUTURES_FROM
        day_trades = simulate_session_day_prevclose(
            sd,
            nifty_candles=nifty_candles,
            sector_candles=sector_candles,
            stock_candles_by_key=candles_by_key,
            stocks_by_sector=stocks_by_sector,
            fut_by_und=fut_by_und,
            eq_by_symbol=eq_by_symbol,
            upstox=upstox,
            pnl_cap_enabled=False,
            spot_proxy_fallback=spot,
            sectors_to_pick=PREVCLOSE_SECTORS_TO_PICK,
            stocks_per_sector=STOCKS_PER_SECTOR,
        )
        all_results.extend(day_trades)
        day_log.append(
            {
                "session_date": sd.isoformat(),
                "trades": len(day_trades),
                "symbols": [t.symbol for t in day_trades],
                "price_source": "spot_proxy" if spot else "futures",
            }
        )
        logger.info(
            "prevclose %s %s trades=%s",
            sd.isoformat(),
            "spot" if spot else "fut",
            len(day_trades),
        )

    rows = [t.to_db_row(mode="backtest") for t in all_results]
    price_src_counts: Dict[str, int] = {}
    for t in rows:
        ps = str(t.get("price_source") or "futures")
        price_src_counts[ps] = price_src_counts.get(ps, 0) + 1

    summary = _summary(rows)
    doc: Dict[str, Any] = {
        "strategy": "breakfast_prevclose",
        "date_from": date_from.isoformat(),
        "date_to": date_to.isoformat(),
        "sectors_to_pick": PREVCLOSE_SECTORS_TO_PICK,
        "stocks_per_sector": STOCKS_PER_SECTOR,
        "nifty_sector_rank_metric": "vs_prev_close",
        "stock_rank_metric": "vs_prev_close",
        "spot_proxy_through": (PREVCLOSE_FUTURES_FROM - timedelta(days=1)).isoformat(),
        "futures_from": PREVCLOSE_FUTURES_FROM.isoformat(),
        "pnl_cap_enabled": False,
        "session_days": len(session_dates),
        "warm_stats": warm_stats,
        "persist": {"inserted": 0, "skipped": 0, "table": None},
        "price_source_counts": price_src_counts,
        "comparability_caveat": COMPARABILITY_CAVEAT,
        "summary": summary,
        "day_log": day_log,
        "trades": rows,
    }
    artifact_path = write_artifact(doc, basename=PREVCLOSE_ARTIFACT_NAME)
    doc["artifact_path"] = str(artifact_path)
    return doc

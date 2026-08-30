"""12-month rolling history artifact — seed, append months, rollup."""
from __future__ import annotations

import json
import logging
from calendar import monthrange
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from backend.services.breakfast_strategy.backtest import (
    _summary,
    find_artifact,
    iter_session_dates,
    run_backtest,
    write_artifact,
)
from backend.services.breakfast_strategy.config import (
    ARTIFACT_NAME,
    DATE_FROM,
    DATE_TO,
    OOS_SPOT_ARTIFACT_NAME,
)

logger = logging.getLogger(__name__)
IST = __import__("pytz").timezone("Asia/Kolkata")

HISTORY_ARTIFACT_NAME = "breakfast_strategy_history.json"

COMPARABILITY_CAVEAT = (
    "Spot-proxy months use cash prices where futures history is unavailable; P&L is sized at "
    "futures-equivalent lot quantity. VWAP and volume tie-breaks use the same spot candle series. "
    "Not directly comparable to the Jul–Aug futures-priced primary dataset (basis divergence + "
    "VWAP-on-spot vs VWAP-on-futures). Over 12 months, raw spot prices may be distorted by "
    "corporate actions (splits/bonuses) — Upstox candles are unadjusted OHLC."
)

ROLLING_MONTHS_BACKWARD: List[str] = [
    "2026-05",
    "2026-04",
    "2026-03",
    "2026-02",
    "2026-01",
    "2025-12",
    "2025-11",
    "2025-10",
    "2025-09",
    "2025-08",
    "2025-07",
    "2025-06",
]


def month_calendar_bounds(period_label: str) -> Tuple[date, date]:
    y, m = period_label.split("-")
    year, mon = int(y), int(m)
    last = monthrange(year, mon)[1]
    return date(year, mon, 1), date(year, mon, last)


def _history_path() -> Path:
    for base in (
        Path("/home/ubuntu/trademanthan/data"),
        Path(__file__).resolve().parents[3] / "data",
    ):
        p = base / HISTORY_ARTIFACT_NAME
        if p.parent.is_dir():
            return p
    return Path(__file__).resolve().parents[3] / "data" / HISTORY_ARTIFACT_NAME


def load_history() -> Dict[str, Any]:
    p = _history_path()
    if not p.is_file():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:
        logger.warning("history read %s: %s", p, e)
        return {}


def save_history(doc: Dict[str, Any]) -> Path:
    doc["updated_at"] = datetime.now(IST).isoformat()
    p = _history_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(doc, indent=2, default=str), encoding="utf-8")
    return p


def _month_entry(
    *,
    period_label: str,
    date_from: date,
    date_to: date,
    price_source: str,
    mode: str,
    status: str,
    summary: Dict[str, Any],
    trades: Optional[List[Dict[str, Any]]] = None,
    coverage: Optional[Dict[str, Any]] = None,
    error: Optional[str] = None,
) -> Dict[str, Any]:
    return {
        "period_label": period_label,
        "date_from": date_from.isoformat(),
        "date_to": date_to.isoformat(),
        "price_source": price_source,
        "mode": mode,
        "status": status,
        "summary": summary,
        "coverage": coverage or {},
        "trades": trades or [],
        "error": error,
    }


def rollup_spot_proxy_months(months: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Combined stats for spot-proxy rolling months only (excludes Jul–Aug futures seed)."""
    eligible = [
        m
        for m in months
        if m.get("status") == "complete"
        and m.get("price_source") == "spot_proxy"
        and m.get("period_label") in ROLLING_MONTHS_BACKWARD + ["2026-06"]
    ]
    all_trades: List[Dict[str, Any]] = []
    for m in eligible:
        all_trades.extend(m.get("trades") or [])
    if not all_trades:
        return _summary([])
    s = _summary(all_trades)
    s["months_included"] = len(eligible)
    s["period_labels"] = [m.get("period_label") for m in eligible]
    return s


def rebuild_history_doc(months: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {
        "strategy": "breakfast",
        "comparability_caveat": COMPARABILITY_CAVEAT,
        "months": months,
        "spot_proxy_rollup": rollup_spot_proxy_months(months),
        "rolling_months_target": ROLLING_MONTHS_BACKWARD,
    }


def seed_history_from_existing() -> Dict[str, Any]:
    """Pre-populate Jul–Aug futures + Jun-2026 spot-proxy before year run."""
    months: List[Dict[str, Any]] = []
    by_label: Dict[str, Dict[str, Any]] = {}

    primary = find_artifact(ARTIFACT_NAME)
    if primary and primary.is_file():
        doc = json.loads(primary.read_text(encoding="utf-8"))
        trades = (doc.get("variants") or {}).get("false", {}).get("trades") or doc.get("trades") or []
        summary = (doc.get("variants") or {}).get("false", {}).get("summary") or doc.get("summary") or _summary(trades)
        entry = _month_entry(
            period_label="2026-07-08",
            date_from=DATE_FROM,
            date_to=DATE_TO,
            price_source="futures",
            mode="backtest",
            status="complete",
            summary=summary,
            trades=trades,
            coverage={"note": "Primary in-sample window Jul 29 – Aug 28 2026"},
        )
        months.append(entry)
        by_label[entry["period_label"]] = entry

    jun = find_artifact(OOS_SPOT_ARTIFACT_NAME)
    if jun and jun.is_file():
        doc = json.loads(jun.read_text(encoding="utf-8"))
        trades = (doc.get("variants") or {}).get("false", {}).get("trades") or doc.get("trades") or []
        summary = (doc.get("variants") or {}).get("false", {}).get("summary") or doc.get("summary") or _summary(trades)
        d0, d1 = month_calendar_bounds("2026-06")
        entry = _month_entry(
            period_label="2026-06",
            date_from=d0,
            date_to=d1,
            price_source="spot_proxy",
            mode="backtest_oos_spot",
            status="complete",
            summary=summary,
            trades=trades,
            coverage=doc.get("coverage") or {"symbols_with_full_coverage": doc.get("session_days")},
        )
        months.append(entry)
        by_label["2026-06"] = entry

    # Preserve any completed rolling months from prior partial run
    existing = load_history()
    for m in existing.get("months") or []:
        pl = str(m.get("period_label") or "")
        if pl in ROLLING_MONTHS_BACKWARD and pl not in by_label:
            months.append(m)
            by_label[pl] = m

    # Sort: futures seed first, then descending period_label
    def _sort_key(m: Dict[str, Any]) -> Tuple[int, str]:
        pl = str(m.get("period_label") or "")
        if pl == "2026-07-08":
            return (0, pl)
        return (1, pl)

    months.sort(key=_sort_key, reverse=True)
    doc = rebuild_history_doc(months)
    save_history(doc)
    return doc


def upsert_month_in_history(month_entry: Dict[str, Any]) -> Dict[str, Any]:
    doc = load_history()
    months: List[Dict[str, Any]] = list(doc.get("months") or [])
    pl = month_entry.get("period_label")
    months = [m for m in months if m.get("period_label") != pl]
    months.append(month_entry)

    def _sort_key(m: Dict[str, Any]) -> Tuple[int, str]:
        label = str(m.get("period_label") or "")
        if label == "2026-07-08":
            return (0, label)
        return (1, label)

    months.sort(key=_sort_key, reverse=True)
    doc = rebuild_history_doc(months)
    save_history(doc)
    return doc


def check_month_spot_coverage(
    period_label: str,
    *,
    upstox: Any,
    cache_dir: Any,
    stocks_by_sector: Dict[str, Any],
    fut_by_und: Dict[str, Any],
    eq_by_symbol: Dict[str, Any],
) -> Dict[str, Any]:
    from collections import defaultdict

    from backend.services.breakfast_strategy.candles import ensure_5m_cached, session_has_stock_bars
    from backend.services.breakfast_strategy.universe import resolve_eq_spot_with_fut_lot

    d0, d1 = month_calendar_bounds(period_label)
    session_dates = iter_session_dates(d0, d1)
    syms = sorted(
        {
            str(m.get("stock") or "").upper()
            for members in stocks_by_sector.values()
            for m in members
            if m.get("stock")
        }
    )
    per_day_ok: Dict[str, int] = {}
    per_sym_days: Dict[str, int] = defaultdict(int)
    missing_syms: set[str] = set()
    sym_candles: Dict[str, List] = {}

    for sym in syms:
        eq = resolve_eq_spot_with_fut_lot(sym, fut_by_und=fut_by_und, eq_by_symbol=eq_by_symbol)
        if not eq or not eq.instrument_key:
            missing_syms.add(sym)
            continue
        sym_candles[sym] = ensure_5m_cached(
            upstox,
            cache_dir,
            eq.instrument_key,
            range_end=d1,
            range_start=d0,
            session_dates=session_dates,
        )

    for sd in session_dates:
        ok = 0
        for sym in syms:
            candles = sym_candles.get(sym)
            if candles and session_has_stock_bars(candles, sd):
                ok += 1
                per_sym_days[sym] += 1
        per_day_ok[sd.isoformat()] = ok

    full = sum(1 for s in syms if per_sym_days.get(s, 0) == len(session_dates) and len(session_dates) > 0)
    # Session days where most of the universe had spot bars (exclude thin/holiday gaps).
    active_session_days = sum(1 for n in per_day_ok.values() if n >= max(1, len(syms) // 2))
    symbols_ready_active_days = sum(
        1 for s in syms
        if active_session_days > 0
        and per_sym_days.get(s, 0) >= active_session_days
    )
    return {
        "period_label": period_label,
        "date_from": d0.isoformat(),
        "date_to": d1.isoformat(),
        "session_days": len(session_dates),
        "active_session_days": active_session_days,
        "universe_symbols": len(syms),
        "symbols_full_month_coverage": full,
        "symbols_active_month_coverage": symbols_ready_active_days,
        "symbols_never_ready": sorted(missing_syms),
        "min_spot_ready_per_day": min(per_day_ok.values()) if per_day_ok else 0,
        "max_spot_ready_per_day": max(per_day_ok.values()) if per_day_ok else 0,
    }


def run_spot_proxy_month(
    period_label: str,
    *,
    persist_db: bool = True,
    force_fetch: bool = False,
) -> Dict[str, Any]:
    """Run one calendar month spot-proxy backtest and append to history artifact."""
    from backend.config import settings
    from backend.services.breakfast_strategy.candles import default_cache_dir
    from backend.services.breakfast_strategy.universe import build_instrument_indexes, load_arbitrage_by_sector
    from backend.services.upstox_service import UpstoxService

    d0, d1 = month_calendar_bounds(period_label)
    upstox = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    stocks_by_sector = load_arbitrage_by_sector()
    fut_by_und, eq_by_symbol = build_instrument_indexes()
    cache_dir = default_cache_dir()

    coverage: Dict[str, Any] = {}
    try:
        coverage = check_month_spot_coverage(
            period_label,
            upstox=upstox,
            cache_dir=cache_dir,
            stocks_by_sector=stocks_by_sector,
            fut_by_und=fut_by_und,
            eq_by_symbol=eq_by_symbol,
        )
    except Exception as e:
        logger.exception("coverage check %s: %s", period_label, e)
        coverage = {"error": str(e)}

    upsert_month_in_history(
        _month_entry(
            period_label=period_label,
            date_from=d0,
            date_to=d1,
            price_source="spot_proxy",
            mode="backtest_oos_spot",
            status="running",
            summary=_summary([]),
            coverage=coverage,
        )
    )

    try:
        out = run_backtest(
            date_from=d0,
            date_to=d1,
            mode="backtest_oos_spot",
            force_fetch=force_fetch,
            persist_db=persist_db,
            pnl_cap_enabled=False,
            write_artifact_file=False,
            spot_proxy_fallback=True,
        )
        trades = (out.get("variants") or {}).get("false", {}).get("trades") or out.get("trades") or []
        for t in trades:
            t["period_label"] = period_label
        if persist_db and trades:
            from backend.services.breakfast_strategy.persist import persist_trades

            persist_trades(trades, mode="backtest_oos_spot")
        summary = (out.get("variants") or {}).get("false", {}).get("summary") or out.get("summary") or _summary(trades)
        entry = _month_entry(
            period_label=period_label,
            date_from=d0,
            date_to=d1,
            price_source="spot_proxy",
            mode="backtest_oos_spot",
            status="complete",
            summary=summary,
            trades=trades,
            coverage=coverage,
        )
        upsert_month_in_history(entry)
        return entry
    except Exception as e:
        logger.exception("month run failed %s: %s", period_label, e)
        entry = _month_entry(
            period_label=period_label,
            date_from=d0,
            date_to=d1,
            price_source="spot_proxy",
            mode="backtest_oos_spot",
            status="failed",
            summary=_summary([]),
            coverage=coverage,
            error=str(e),
        )
        upsert_month_in_history(entry)
        return entry

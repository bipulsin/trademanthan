"""Main backtest runner for Open-Low 15m strategy."""
from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytz

from backend.config import settings
from backend.services.market_holiday import refresh_holiday_dates_from_db
from backend.services.open_low_15m.config import (
    ARTIFACT_NAME,
    DATE_FROM,
    DATE_TO,
    TP_R_LEVELS,
)
from backend.services.open_low_15m.db import ensure_open_low_tables, upsert_trades
from backend.services.open_low_15m.simulate import detect_setup, simulate_trade
from backend.services.open_low_15m.universe import load_open_low_universe_for_session
from backend.services.smart_futures_picker.position_sizing import get_futures_lot_size_by_instrument_key
from backend.services.upstox_service import UpstoxService
from backend.services.volume_mismatch.candles import (
    BacktestDailyCache,
    _daily_candle_date,
    fetch_candles_cached,
)
from backend.services.volume_mismatch.candle_cache import VolumeMismatchCandleCache, default_cache_dir

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

STRATEGY_CRITERIA = (
    "LONG on current-month stock FUT: first 15m (09:15–09:30) OPEN≈LOW, "
    "entry on break of setup HIGH, SL at setup LOW or 50% range if candle > 2×ATR(14), "
    "force exit 15:15 IST. TP variants 1R/1.5R/2R/3R tested independently."
)


def _load_holiday_dates(upstox: UpstoxService, d0: date, d1: date) -> set[date]:
    holidays = refresh_holiday_dates_from_db()
    if holidays:
        return holidays
    out: set[date] = set()
    for year in range(d0.year, d1.year + 1):
        for dstr in upstox.get_market_holidays(year) or []:
            try:
                out.add(date.fromisoformat(str(dstr)[:10]))
            except ValueError:
                continue
    return out


def _iter_trading_days(d0: date, d1: date, holidays: set[date]) -> List[date]:
    out: List[date] = []
    d = d0
    while d <= d1:
        if d.weekday() < 5 and d not in holidays:
            out.append(d)
        d += timedelta(days=1)
    return out


def _daily_closes_before(bars: List[Dict[str, Any]], session_date: date) -> List[float]:
    out: List[float] = []
    for c in bars:
        d = _daily_candle_date(c)
        if d is None or d >= session_date:
            continue
        try:
            cl = float(c.get("close") or 0)
        except (TypeError, ValueError):
            continue
        if cl > 0:
            out.append(cl)
    return out


def _aggregate_metrics(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not rows:
        return {
            "total_trades": 0,
            "wins": 0,
            "losses": 0,
            "win_rate": 0.0,
            "avg_r": 0.0,
            "max_drawdown_r": 0.0,
            "avg_holding_minutes": 0.0,
            "best_r": 0.0,
            "worst_r": 0.0,
        }
    rs = [float(r.get("r_realized") or 0) for r in rows]
    wins = sum(1 for x in rs if x > 0)
    losses = len(rs) - wins
    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    for x in rs:
        equity += x
        peak = max(peak, equity)
        max_dd = max(max_dd, peak - equity)
    hold = [int(r.get("holding_minutes") or 0) for r in rows]
    return {
        "total_trades": len(rows),
        "wins": wins,
        "losses": losses,
        "win_rate": round(100.0 * wins / len(rows), 2),
        "avg_r": round(sum(rs) / len(rs), 4),
        "max_drawdown_r": round(max_dd, 4),
        "avg_holding_minutes": round(sum(hold) / len(hold), 1),
        "best_r": round(max(rs), 4),
        "worst_r": round(min(rs), 4),
    }


def _by_tp(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for tp in TP_R_LEVELS:
        sub = [r for r in rows if r.get("tp_variant") == tp]
        m = _aggregate_metrics(sub)
        m["tp_hits"] = sum(1 for r in sub if r.get("tp_hit"))
        out[tp] = m
    return out


def _by_sl_type(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for slt in ("primary", "alternative"):
        sub = [r for r in rows if r.get("sl_type") == slt]
        out[slt] = _aggregate_metrics(sub)
    return out


def _daily_summary(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    by_date: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        d = str(r.get("session_date") or "")
        by_date.setdefault(d, []).append(r)
    out: List[Dict[str, Any]] = []
    for d in sorted(by_date):
        sub = by_date[d]
        m = _aggregate_metrics(sub)
        out.append({"session_date": d, **m})
    return out


def _mark_top_gainers(setups: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    by_date: Dict[str, List[Dict[str, Any]]] = {}
    for s in setups:
        by_date.setdefault(s["session_date"], []).append(s)
    for d, items in by_date.items():
        if not items:
            continue
        best = max(items, key=lambda x: float(x.get("first_15m_gain_pct") or 0))
        best_sym = best.get("symbol")
        for it in items:
            it["is_top_gainer"] = it.get("symbol") == best_sym
    return setups


def run_open_low_15m_backtest(
    from_date: date = DATE_FROM,
    to_date: date = DATE_TO,
    *,
    run_id: Optional[str] = None,
    out_path: Optional[Path] = None,
    write_db: bool = True,
    tp_filter: Optional[str] = None,
) -> Dict[str, Any]:
    if from_date > to_date:
        return {"ok": False, "error": "from_date must be <= to_date", "rows": []}

    upstox = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    if not getattr(upstox, "access_token", None):
        return {"ok": False, "error": "Upstox token unavailable", "rows": []}

    rid = run_id or f"open_low_15m_{datetime.now(IST).strftime('%Y%m%d_%H%M%S')}"
    holidays = _load_holiday_dates(upstox, from_date, to_date)
    session_days = _iter_trading_days(from_date, to_date, holidays)
    persistent = VolumeMismatchCandleCache(cache_dir=default_cache_dir())
    daily_cache = BacktestDailyCache(persistent_cache=persistent)

    all_trades: List[Dict[str, Any]] = []
    setups_found = 0
    errors: List[Dict[str, str]] = []

    tp_variants = [tp_filter] if tp_filter in TP_R_LEVELS else list(TP_R_LEVELS.keys())

    for session_date in session_days:
        universe = load_open_low_universe_for_session(session_date)
        day_setups: List[Dict[str, Any]] = []
        for u in universe:
            ik = u.get("instrument_key") or ""
            sym = u.get("symbol") or ""
            if not ik:
                continue
            try:
                prev_close, _ = daily_cache.previous_close(upstox, ik, session_date)
                daily_bars, _ = daily_cache.daily_bars_for_bb(upstox, ik, session_date, min_closes=10)
                closes_before = _daily_closes_before(daily_bars, session_date)
                m15 = fetch_candles_cached(
                    upstox,
                    ik,
                    "minutes/15",
                    days_back=5,
                    range_end_date=session_date,
                    allow_rest=True,
                )
                if not m15:
                    continue
                lot = get_futures_lot_size_by_instrument_key(ik) or 1
                setup = detect_setup(
                    symbol=sym,
                    future_symbol=u.get("future_symbol") or sym,
                    instrument_key=ik,
                    session_date=session_date,
                    candles_15m=m15,
                    prev_close=float(prev_close or 0),
                    daily_closes_before=closes_before,
                    lot_size=lot,
                )
                if setup:
                    day_setups.append(setup)
            except Exception as e:
                errors.append({"session_date": session_date.isoformat(), "symbol": sym, "error": str(e)})
                logger.debug("open_low scan %s %s: %s", session_date, sym, e)

        day_setups = _mark_top_gainers(day_setups)
        setups_found += len(day_setups)

        for setup in day_setups:
            ik = setup["instrument_key"]
            m15 = fetch_candles_cached(
                upstox,
                ik,
                "minutes/15",
                days_back=5,
                range_end_date=session_date,
                allow_rest=True,
            )
            for tp in tp_variants:
                trade = simulate_trade(setup, m15 or [], tp)
                if trade:
                    trade["run_id"] = rid
                    trade["is_top_gainer"] = setup.get("is_top_gainer", False)
                    all_trades.append(trade)

        logger.info(
            "open_low_15m %s: universe=%s setups=%s trades=%s",
            session_date,
            len(universe),
            len(day_setups),
            len(all_trades),
        )

    summary = _aggregate_metrics(all_trades)
    doc = {
        "ok": True,
        "algo": "open_low_15m_backtest",
        "run_id": rid,
        "strategy_criteria": STRATEGY_CRITERIA,
        "generated_at": datetime.now(IST).isoformat(),
        "from_date": from_date.isoformat(),
        "to_date": to_date.isoformat(),
        "force_exit_ist": "15:15",
        "summary": summary,
        "by_tp_variant": _by_tp(all_trades),
        "by_sl_type": _by_sl_type(all_trades),
        "daily_summary": _daily_summary(all_trades),
        "setups_found": setups_found,
        "errors": errors,
        "rows": all_trades,
    }

    if write_db:
        ensure_open_low_tables()
        upsert_trades(rid, all_trades)

    if out_path is None:
        root = Path(__file__).resolve().parents[2]
        for candidate in (
            Path("/home/ubuntu/trademanthan/data") / ARTIFACT_NAME,
            root / "backend" / "data" / ARTIFACT_NAME,
            root / "data" / ARTIFACT_NAME,
        ):
            candidate.parent.mkdir(parents=True, exist_ok=True)
            out_path = candidate
            break

    if out_path is not None:
        tmp = out_path.with_suffix(".json.tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(doc, f, indent=2, default=str)
        tmp.replace(out_path)
        doc["artifact_path"] = str(out_path)

    return doc

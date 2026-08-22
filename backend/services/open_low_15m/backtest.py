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
from backend.services.open_low_15m.candles import (
    ensure_m15_for_session,
    native_first_15m_bar,
    reset_fetch_caches,
)
from backend.services.open_low_15m.signals import evaluate_setup
from backend.services.open_low_15m.simulate import simulate_trade
from backend.services.open_low_15m.universe import load_open_low_universe_for_session
from backend.services.volume_mismatch.candle_cache import VolumeMismatchCandleCache, default_cache_dir
from backend.services.volume_mismatch.candles import BacktestDailyCache
from backend.services.smart_futures_picker.position_sizing import get_futures_lot_size_by_instrument_key
from backend.services.upstox_service import UpstoxService

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

STRATEGY_CRITERIA = (
    "LONG on current-month stock FUT: native 09:15–09:30 15m candle OPEN≈LOW (not 10m aggregate), "
    "intraday EMA10/VWAP/Supertrend filters, entry on break of setup HIGH, "
    "SL touch at setup LOW or 50% range if candle > 2×ATR(14), force exit 15:15 IST. "
    "TP variants 1R/1.5R/2R/3R tested independently."
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


def _first_bar_ohlc(first: Dict[str, Any]) -> Dict[str, float]:
    def _fx(v: Any) -> float:
        try:
            return float(v)
        except (TypeError, ValueError):
            return 0.0

    return {
        "open": _fx(first.get("open")),
        "high": _fx(first.get("high")),
        "low": _fx(first.get("low")),
        "close": _fx(first.get("close")),
    }


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


def _write_incremental_artifact(
    path: Optional[Path],
    *,
    doc: Dict[str, Any],
) -> None:
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump({**doc, "partial": True}, f, indent=2, default=str)
    tmp.replace(path)


def _load_artifact(path: Path) -> Optional[Dict[str, Any]]:
    if not path.is_file():
        return None
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else None
    except Exception as e:
        logger.warning("open_low read artifact %s: %s", path, e)
        return None


def run_open_low_15m_backtest(
    from_date: date = DATE_FROM,
    to_date: date = DATE_TO,
    *,
    run_id: Optional[str] = None,
    out_path: Optional[Path] = None,
    write_db: bool = True,
    tp_filter: Optional[str] = None,
    day_pause_sec: float = 2.0,
    symbol_pause_sec: float = 0.12,
    merge_into: bool = False,
    reverse_order: bool = False,
    full_replace: bool = False,
) -> Dict[str, Any]:
    if from_date > to_date:
        return {"ok": False, "error": "from_date must be <= to_date", "rows": []}

    upstox = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    if not getattr(upstox, "access_token", None):
        return {"ok": False, "error": "Upstox token unavailable", "rows": []}

    reset_fetch_caches()
    rid = run_id or f"open_low_15m_{datetime.now(IST).strftime('%Y%m%d_%H%M%S')}"
    holidays = _load_holiday_dates(upstox, from_date, to_date)
    session_days = _iter_trading_days(from_date, to_date, holidays)
    if reverse_order:
        session_days = list(reversed(session_days))
    persistent = VolumeMismatchCandleCache(cache_dir=default_cache_dir())
    daily_cache = BacktestDailyCache(persistent_cache=persistent)

    replace_dates = {d.isoformat() for d in session_days}
    base_doc: Optional[Dict[str, Any]] = None

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

    if full_replace:
        base_doc = None
        merge_into = False

    if merge_into and out_path is not None and not full_replace:
        base_doc = _load_artifact(out_path)

    chunk_setups = 0
    errors: List[Dict[str, str]] = []
    day_scan_log: List[Dict[str, Any]] = []

    if base_doc and merge_into:
        all_trades = [
            r for r in (base_doc.get("rows") or []) if str(r.get("session_date") or "") not in replace_dates
        ]
        day_scan_log = [
            d for d in (base_doc.get("day_scan_log") or []) if str(d.get("session_date") or "") not in replace_dates
        ]
        errors = list(base_doc.get("errors") or [])
        rid = str(base_doc.get("run_id") or rid)
    else:
        all_trades = []

    tp_variants = [tp_filter] if tp_filter in TP_R_LEVELS else list(TP_R_LEVELS.keys())

    import time as _time

    def _artifact_from_date() -> str:
        if base_doc and merge_into:
            return str(base_doc.get("from_date") or from_date.isoformat())
        return from_date.isoformat()

    def _artifact_to_date() -> str:
        if base_doc and merge_into:
            return str(base_doc.get("to_date") or to_date.isoformat())
        return to_date.isoformat()

    def _build_doc(*, partial: bool) -> Dict[str, Any]:
        return {
            "ok": True,
            "algo": "open_low_15m_backtest",
            "run_id": rid,
            "strategy_criteria": STRATEGY_CRITERIA,
            "generated_at": datetime.now(IST).isoformat(),
            "from_date": _artifact_from_date(),
            "to_date": _artifact_to_date(),
            "force_exit_ist": "15:15",
            "trading_days_total": base_doc.get("trading_days_total") if base_doc else len(session_days),
            "trading_days_scanned": len(day_scan_log),
            "chunk_from": from_date.isoformat(),
            "chunk_to": to_date.isoformat(),
            "reverse_order": reverse_order,
            "partial": partial,
            "summary": _aggregate_metrics(all_trades),
            "by_tp_variant": _by_tp(all_trades),
            "by_sl_type": _by_sl_type(all_trades),
            "daily_summary": _daily_summary(all_trades),
            "day_scan_log": day_scan_log,
            "setups_found": sum(int(d.get("setups") or 0) for d in day_scan_log),
            "errors": errors,
            "rows": all_trades,
        }

    for session_date in session_days:
        universe = load_open_low_universe_for_session(session_date)
        day_setups: List[Dict[str, Any]] = []
        setup_rejects: List[Dict[str, Any]] = []
        m15_miss = 0
        for u in universe:
            ik = u.get("instrument_key") or ""
            sym = u.get("symbol") or ""
            if not ik:
                continue
            try:
                prev_close, _ = daily_cache.previous_close(upstox, ik, session_date)
                m15, fetched = ensure_m15_for_session(
                    upstox,
                    persistent,
                    ik,
                    session_date,
                    symbol_pause_sec=symbol_pause_sec,
                    force_refetch=full_replace,
                )
                if fetched:
                    m15_miss += 1
                first = native_first_15m_bar(m15 or [], session_date)
                if not m15 or first is None:
                    continue
                lot = get_futures_lot_size_by_instrument_key(ik) or 1
                setup, reject_reason = evaluate_setup(
                    symbol=sym,
                    future_symbol=u.get("future_symbol") or sym,
                    instrument_key=ik,
                    session_date=session_date,
                    candles_15m=m15,
                    prev_close=float(prev_close or 0),
                    lot_size=lot,
                )
                if setup:
                    day_setups.append(setup)
                elif reject_reason == "open_not_low":
                    ohlc = _first_bar_ohlc(first)
                    setup_rejects.append(
                        {
                            "symbol": sym,
                            "reason": reject_reason,
                            **ohlc,
                        }
                    )
                elif reject_reason not in ("no_native_15m_bar", "bad_open"):
                    ohlc = _first_bar_ohlc(first)
                    setup_rejects.append(
                        {
                            "symbol": sym,
                            "reason": reject_reason,
                            **ohlc,
                        }
                    )
            except Exception as e:
                errors.append({"session_date": session_date.isoformat(), "symbol": sym, "error": str(e)})
                logger.debug("open_low scan %s %s: %s", session_date, sym, e)

        day_setups = _mark_top_gainers(day_setups)
        chunk_setups += len(day_setups)

        day_trades_before = len(all_trades)
        for setup in day_setups:
            ik = setup["instrument_key"]
            sd = date.fromisoformat(setup["session_date"])
            m15, _ = ensure_m15_for_session(
                upstox, persistent, ik, sd, symbol_pause_sec=0.0, force_refetch=full_replace,
            )
            for tp in tp_variants:
                trade = simulate_trade(setup, m15 or [], tp)
                if trade:
                    trade["run_id"] = rid
                    trade["is_top_gainer"] = setup.get("is_top_gainer", False)
                    all_trades.append(trade)

        day_scan_log.append(
            {
                "session_date": session_date.isoformat(),
                "universe": len(universe),
                "setups": len(day_setups),
                "trades_added": len(all_trades) - day_trades_before,
                "trades_total": len(all_trades),
                "m15_api_fetches": m15_miss,
                "setup_rejects": setup_rejects[:40],
            }
        )
        day_scan_log.sort(key=lambda x: str(x.get("session_date") or ""))
        logger.info(
            "open_low_15m %s: universe=%s setups=%s trades=%s m15_fetch=%s",
            session_date,
            len(universe),
            len(day_setups),
            len(all_trades),
            m15_miss,
        )

        _write_incremental_artifact(out_path, doc=_build_doc(partial=True))

        if day_pause_sec > 0:
            _time.sleep(day_pause_sec)

    doc = _build_doc(partial=False)
    if base_doc and merge_into:
        doc["trading_days_total"] = base_doc.get("trading_days_total") or len(day_scan_log)

    if write_db:
        ensure_open_low_tables()
        upsert_trades(rid, all_trades)

    if out_path is not None:
        tmp = out_path.with_suffix(".json.tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(doc, f, indent=2, default=str)
        tmp.replace(out_path)
        doc["artifact_path"] = str(out_path)

    return doc

"""Backtest runner — warm 5m cache, simulate, persist, write JSON artifact."""
from __future__ import annotations

import json
import logging
from datetime import date, datetime, time as dt_time, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from backend.config import settings
from backend.services.breakfast_strategy.candles import default_cache_dir, ensure_5m_cached
from backend.services.breakfast_strategy.config import ARTIFACT_NAME, DATE_FROM, DATE_TO, PNL_CAP_INR
from backend.services.breakfast_strategy.engine import NIFTY50_KEY, TradeResult, simulate_session_day
from backend.services.breakfast_strategy.persist import clear_backtest_trades, persist_trades
from backend.services.breakfast_strategy.universe import (
    SECTOR_UNIVERSE,
    build_instrument_indexes,
    load_arbitrage_by_sector,
    resolve_eq_spot_with_fut_lot,
    resolve_stock_instrument,
    sector_index_key_for_label,
)
from backend.services.market_holiday import is_nse_holiday_ist
from backend.services.upstox_service import UpstoxService

logger = logging.getLogger(__name__)
IST = __import__("pytz").timezone("Asia/Kolkata")


def iter_session_dates(start: date, end: date) -> List[date]:
    out: List[date] = []
    d = start
    while d <= end:
        if d.weekday() < 5:
            noon = IST.localize(datetime.combine(d, dt_time(12, 0)))
            if not is_nse_holiday_ist(noon):
                out.append(d)
        d += timedelta(days=1)
    return out


def collect_instrument_keys(
    session_dates: List[date],
    stocks_by_sector: Dict[str, List[Dict[str, str]]],
    fut_by_und: Dict[str, Any],
    eq_by_symbol: Dict[str, Any],
    *,
    spot_proxy_fallback: bool = False,
) -> Set[str]:
    keys: Set[str] = {NIFTY50_KEY}
    for label, _y in SECTOR_UNIVERSE:
        ik = sector_index_key_for_label(label)
        if ik:
            keys.add(ik)
    seen_syms: Set[str] = set()
    for members in stocks_by_sector.values():
        for m in members:
            seen_syms.add(str(m.get("stock") or "").upper())
    for sd in session_dates:
        for sym in seen_syms:
            if not sym:
                continue
            ref = resolve_stock_instrument(sym, sd, fut_by_und=fut_by_und, eq_by_symbol=eq_by_symbol)
            if ref and ref.instrument_key:
                keys.add(ref.instrument_key)
            if spot_proxy_fallback:
                eq = resolve_eq_spot_with_fut_lot(sym, fut_by_und=fut_by_und, eq_by_symbol=eq_by_symbol)
                if eq and eq.instrument_key:
                    keys.add(eq.instrument_key)
    return keys


def _artifact_paths() -> List[Path]:
    root = Path(__file__).resolve().parents[3]
    return [
        Path("/home/ubuntu/trademanthan/data") / ARTIFACT_NAME,
        root / "data" / ARTIFACT_NAME,
        root / "backend" / "data" / ARTIFACT_NAME,
    ]


def write_artifact(doc: Dict[str, Any], *, basename: Optional[str] = None) -> Path:
    name = basename or ARTIFACT_NAME
    for base in (
        Path("/home/ubuntu/trademanthan/data"),
        Path(__file__).resolve().parents[3] / "data",
        Path(__file__).resolve().parents[3] / "backend" / "data",
    ):
        p = base / name
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(doc, indent=2, default=str), encoding="utf-8")
        return p
    raise RuntimeError("could not write breakfast artifact")


def find_artifact(basename: Optional[str] = None) -> Optional[Path]:
    name = basename or ARTIFACT_NAME
    for base in (
        Path("/home/ubuntu/trademanthan/data"),
        Path(__file__).resolve().parents[3] / "data",
        Path(__file__).resolve().parents[3] / "backend" / "data",
    ):
        p = base / name
        if p.is_file():
            return p
    return None


def _summary(trades: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not trades:
        return {
            "total_trades": 0,
            "win_rate_pct": 0.0,
            "total_pnl_inr": 0.0,
            "avg_pnl_inr": 0.0,
            "avg_r": 0.0,
            "long_trades": 0,
            "short_trades": 0,
            "long_pnl_inr": 0.0,
            "short_pnl_inr": 0.0,
            "by_sector": [],
        }
    wins = sum(1 for t in trades if float(t.get("pnl_inr") or 0) > 0)
    total_pnl = sum(float(t.get("pnl_inr") or 0) for t in trades)
    longs = [t for t in trades if t.get("direction") == "long"]
    shorts = [t for t in trades if t.get("direction") == "short"]
    rs: List[float] = []
    for t in trades:
        ep = float(t.get("entry_price") or 0)
        sl = float(t.get("sl_price") or 0)
        pnl = float(t.get("pnl_points") or 0)
        risk = abs(ep - sl)
        if risk > 0:
            rs.append(pnl / risk)
    by_sector: Dict[str, Dict[str, Any]] = {}
    for t in trades:
        sec = str(t.get("sector") or "Unknown")
        b = by_sector.setdefault(sec, {"sector": sec, "trades": 0, "pnl_inr": 0.0, "wins": 0})
        b["trades"] += 1
        p = float(t.get("pnl_inr") or 0)
        b["pnl_inr"] += p
        if p > 0:
            b["wins"] += 1
    sector_rows = [
        {
            "sector": sec,
            "trades": b["trades"],
            "win_rate_pct": round(100.0 * b["wins"] / b["trades"], 1) if b["trades"] else 0,
            "pnl_inr": round(b["pnl_inr"], 2),
        }
        for sec, b in sorted(by_sector.items(), key=lambda x: -x[1]["pnl_inr"])
    ]
    return {
        "total_trades": len(trades),
        "win_rate_pct": round(100.0 * wins / len(trades), 1),
        "total_pnl_inr": round(total_pnl, 2),
        "avg_pnl_inr": round(total_pnl / len(trades), 2),
        "avg_r": round(sum(rs) / len(rs), 3) if rs else 0.0,
        "long_trades": len(longs),
        "short_trades": len(shorts),
        "long_pnl_inr": round(sum(float(t.get("pnl_inr") or 0) for t in longs), 2),
        "short_pnl_inr": round(sum(float(t.get("pnl_inr") or 0) for t in shorts), 2),
        "by_sector": sector_rows,
    }


def warm_candle_cache(
    upstox: UpstoxService,
    cache_dir: Path,
    instrument_keys: Set[str],
    *,
    range_start: date,
    range_end: date,
    session_dates: List[date],
    force: bool = False,
) -> Dict[str, int]:
    n = 0
    for ik in sorted(instrument_keys):
        ensure_5m_cached(
            upstox,
            cache_dir,
            ik,
            range_end=range_end,
            range_start=range_start,
            session_dates=session_dates,
            force=force,
        )
        n += 1
    return {"instruments": len(instrument_keys), "fetch_attempts": n}


def _simulate_session_range(
    session_dates: List[date],
    *,
    nifty_candles: List[Dict[str, Any]],
    sector_candles: Dict[str, List[Dict[str, Any]]],
    stock_candles_by_key: Dict[str, List[Dict[str, Any]]],
    stocks_by_sector: Dict[str, List[Dict[str, str]]],
    fut_by_und: Dict[str, Any],
    eq_by_symbol: Dict[str, Any],
    upstox: Any,
    pnl_cap_enabled: bool,
    spot_proxy_fallback: bool = False,
) -> tuple[List[TradeResult], List[Dict[str, Any]]]:
    all_results: List[TradeResult] = []
    day_log: List[Dict[str, Any]] = []
    for sd in session_dates:
        day_trades = simulate_session_day(
            sd,
            nifty_candles=nifty_candles,
            sector_candles=sector_candles,
            stock_candles_by_key=stock_candles_by_key,
            stocks_by_sector=stocks_by_sector,
            fut_by_und=fut_by_und,
            eq_by_symbol=eq_by_symbol,
            upstox=upstox,
            pnl_cap_enabled=pnl_cap_enabled,
            spot_proxy_fallback=spot_proxy_fallback,
        )
        all_results.extend(day_trades)
        day_log.append(
            {
                "session_date": sd.isoformat(),
                "trades": len(day_trades),
                "symbols": [t.symbol for t in day_trades],
            }
        )
    return all_results, day_log


def apply_cap_variant(doc: Dict[str, Any], pnl_cap_enabled: bool) -> Dict[str, Any]:
    """Return artifact view for cap on/off without re-running the backtest."""
    key = "true" if pnl_cap_enabled else "false"
    variants = doc.get("variants")
    if not isinstance(variants, dict) or key not in variants:
        out = dict(doc)
        out["pnl_cap_enabled"] = pnl_cap_enabled
        return out
    v = variants[key]
    out = dict(doc)
    out["pnl_cap_enabled"] = pnl_cap_enabled
    out["summary"] = v.get("summary") or {}
    out["trades"] = v.get("trades") or []
    out["day_log"] = v.get("day_log") or doc.get("day_log") or []
    return out


def run_backtest(
    *,
    date_from: date = DATE_FROM,
    date_to: date = DATE_TO,
    mode: str = "backtest",
    force_fetch: bool = False,
    persist_db: bool = True,
    pnl_cap_enabled: bool = False,
    artifact_basename: Optional[str] = None,
    write_artifact_file: bool = True,
    spot_proxy_fallback: bool = False,
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
        spot_proxy_fallback=spot_proxy_fallback,
    )

    warm_stats = warm_candle_cache(
        upstox,
        cache_dir,
        instrument_keys,
        range_start=date_from,
        range_end=date_to,
        session_dates=session_dates,
        force=force_fetch,
    )

    cleared = 0
    if persist_db and mode == "backtest":
        cleared = clear_backtest_trades()

    candles_by_key: Dict[str, List[Dict[str, Any]]] = {}
    for ik in instrument_keys:
        candles_by_key[ik] = ensure_5m_cached(
            upstox,
            cache_dir,
            ik,
            range_end=date_to,
            range_start=date_from,
            session_dates=session_dates,
        )

    sector_candles = {
        ik: candles_by_key.get(ik, [])
        for ik in candles_by_key
        if ik.startswith("NSE_INDEX|")
    }
    nifty_candles = candles_by_key.get(NIFTY50_KEY, [])

    sim_common = dict(
        nifty_candles=nifty_candles,
        sector_candles=sector_candles,
        stock_candles_by_key=candles_by_key,
        stocks_by_sector=stocks_by_sector,
        fut_by_und=fut_by_und,
        eq_by_symbol=eq_by_symbol,
        upstox=upstox,
    )
    all_results_off, day_log_off = _simulate_session_range(
        session_dates, **sim_common, pnl_cap_enabled=False, spot_proxy_fallback=spot_proxy_fallback
    )
    all_results_on, day_log_on = _simulate_session_range(
        session_dates, **sim_common, pnl_cap_enabled=True, spot_proxy_fallback=spot_proxy_fallback
    )

    rows_off = [t.to_db_row(mode=mode) for t in all_results_off]
    rows_on = [t.to_db_row(mode=mode) for t in all_results_on]
    active_rows = rows_on if pnl_cap_enabled else rows_off
    active_day_log = day_log_on if pnl_cap_enabled else day_log_off

    persist_stats = {"inserted": 0, "skipped": 0}
    if persist_db and active_rows:
        persist_stats = persist_trades(active_rows, mode=mode)

    variants = {
        "false": {
            "summary": _summary(rows_off),
            "trades": rows_off,
            "day_log": day_log_off,
        },
        "true": {
            "summary": _summary(rows_on),
            "trades": rows_on,
            "day_log": day_log_on,
        },
    }
    summary = variants["true" if pnl_cap_enabled else "false"]["summary"]
    price_src_counts: Dict[str, int] = {}
    for t in active_rows:
        ps = str(t.get("price_source") or "futures")
        price_src_counts[ps] = price_src_counts.get(ps, 0) + 1
    doc = {
        "strategy": "breakfast",
        "date_from": date_from.isoformat(),
        "date_to": date_to.isoformat(),
        "mode": mode,
        "spot_proxy_fallback": spot_proxy_fallback,
        "price_source_counts": price_src_counts,
        "stock_rank_metric": "vs_prev_close",
        "pnl_cap_enabled": pnl_cap_enabled,
        "pnl_cap_inr": PNL_CAP_INR,
        "session_days": len(session_dates),
        "warm_stats": warm_stats,
        "cleared_backtest_rows": cleared,
        "persist": persist_stats,
        "variants": variants,
        "summary": summary,
        "day_log": active_day_log,
        "trades": active_rows,
    }
    if write_artifact_file:
        artifact_path = write_artifact(doc, basename=artifact_basename)
        doc["artifact_path"] = str(artifact_path)
    return doc


def run_forward_today(*, persist_db: bool = True) -> Dict[str, Any]:
    """Forward paper run for today's IST session (same engine, mode=forward)."""
    today = datetime.now(IST).date()
    return run_backtest(
        date_from=today,
        date_to=today,
        mode="forward",
        force_fetch=True,
        persist_db=persist_db,
    )

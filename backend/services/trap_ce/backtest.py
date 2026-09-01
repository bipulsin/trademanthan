"""Run Trap-CE backtest over CSV rows."""
from __future__ import annotations

import json
import logging
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional

from backend.services.trap_ce.candles import default_cache_dir, fetch_session_10m
from backend.services.trap_ce.config import (
    DEFAULT_CSV,
    SKIP_NO_FUT,
    SKIP_NO_LOT,
    SKIP_SHORT,
)
from backend.services.trap_ce.csv_signals import load_trap_ce_csv
from backend.services.trap_ce.simulate import simulate_trap_ce_long
from backend.services.trap_ce.universe import LotSizeLookup, resolve_leg

logger = logging.getLogger(__name__)


def _bucket(r: Dict[str, Any]) -> str:
    b = str(r.get("bucket") or "").lower()
    if b in ("stock", "eq"):
        return "stock"
    if b == "fut":
        return "fut"
    if str(r.get("instrument_kind") or "").lower() in ("eq", "stock"):
        return "stock"
    return "fut"


def summarize(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    taken = [r for r in rows if r.get("taken")]
    fut_taken = [r for r in taken if _bucket(r) != "stock"]
    stock_taken = [r for r in taken if _bucket(r) == "stock"]
    skipped = [r for r in rows if not r.get("taken")]
    wins = [r for r in fut_taken if r.get("win")]
    stock_wins = [r for r in stock_taken if r.get("win")]
    r_vals = [float(r.get("r_realized") or 0) for r in fut_taken]
    r_stock = [float(r.get("r_realized") or 0) for r in stock_taken]
    return {
        "csv_rows": len(rows),
        "trade_count": len(fut_taken),
        "stock_trade_count": len(stock_taken),
        "skip_count": len(skipped),
        "win_count": len(wins),
        "win_pct": (100.0 * len(wins) / len(fut_taken)) if fut_taken else None,
        "avg_r": (sum(r_vals) / len(r_vals)) if r_vals else None,
        "sum_pnl_inr": sum(float(r.get("pnl_inr") or 0) for r in fut_taken),
        "stock_win_count": len(stock_wins),
        "stock_win_pct": (100.0 * len(stock_wins) / len(stock_taken)) if stock_taken else None,
        "stock_avg_r": (sum(r_stock) / len(r_stock)) if r_stock else None,
        "stock_sum_pnl_inr": sum(float(r.get("pnl_inr") or 0) for r in stock_taken),
        "skip_reasons": _count_field(skipped, "skip_reason"),
        "exit_reasons": _count_field(fut_taken, "exit_reason"),
        "stock_exit_reasons": _count_field(stock_taken, "exit_reason"),
    }


def _count_field(rows: List[Dict[str, Any]], key: str) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for r in rows:
        k = str(r.get(key) or "unknown")
        out[k] = out.get(k, 0) + 1
    return out


def run_trap_ce_backtest(
    csv_path: Path,
    *,
    upstox: Any,
    cache_dir: Optional[Path] = None,
    limit: Optional[int] = None,
    symbol_pause_sec: float = 0.12,
) -> Dict[str, Any]:
    signals = load_trap_ce_csv(csv_path)
    if limit is not None:
        signals = signals[: int(limit)]
    lots = LotSizeLookup()
    cache_dir = cache_dir or default_cache_dir()
    rows: List[Dict[str, Any]] = []
    bar_cache: Dict[tuple, List[Dict[str, Any]]] = {}

    for sig in signals:
        if sig.get("skip_reason") == SKIP_SHORT:
            rows.append({**{k: v for k, v in sig.items() if k != "trigger_dt"}, "taken": False})
            continue
        session_date: date = sig["session_date"]
        symbol = sig["symbol"]
        leg = resolve_leg(symbol, session_date, lots=lots)
        if not leg:
            rows.append(
                {
                    "symbol": symbol,
                    "session_date": session_date.isoformat(),
                    "trigger_time": sig["trigger_time"].strftime("%H:%M"),
                    "taken": False,
                    "skip_reason": SKIP_NO_FUT,
                }
            )
            continue
        lot = int(leg.lot_size or 0)
        if lot <= 0:
            rows.append(
                {
                    "symbol": symbol,
                    "instrument_key": leg.instrument_key,
                    "future_symbol": leg.trading_symbol if leg.kind == "fut" else "",
                    "bucket": "stock" if leg.kind == "eq" else "fut",
                    "session_date": session_date.isoformat(),
                    "trigger_time": sig["trigger_time"].strftime("%H:%M"),
                    "taken": False,
                    "skip_reason": SKIP_NO_LOT,
                }
            )
            continue
        key = (leg.instrument_key, session_date)
        if key not in bar_cache:
            try:
                bar_cache[key] = fetch_session_10m(
                    upstox,
                    leg.instrument_key,
                    session_date,
                    cache_dir=cache_dir,
                    symbol_pause_sec=symbol_pause_sec,
                )
            except Exception as e:
                logger.warning("trap_ce fetch failed %s %s: %s", symbol, session_date, e)
                bar_cache[key] = []
        trade = simulate_trap_ce_long(
            bar_cache[key],
            trigger_time=sig["trigger_time"],
            lot_size=lot,
            session_date=session_date,
            symbol=symbol,
            instrument_key=leg.instrument_key,
            future_symbol=leg.trading_symbol if leg.kind == "fut" else "",
        )
        trade["bucket"] = "stock" if leg.kind == "eq" else "fut"
        trade["instrument_kind"] = leg.kind
        trade["qty"] = lot
        rows.append(trade)

    summary = summarize(rows)
    return {"ok": True, "summary": summary, "rows": rows, "csv": str(csv_path)}


def write_artifact(result: Dict[str, Any], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    slim_rows = []
    for r in result.get("rows") or []:
        slim_rows.append({k: v for k, v in r.items() if k not in ("trigger_dt",)})
    payload = {**result, "rows": slim_rows}
    out_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")

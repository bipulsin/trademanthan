"""Main backtest orchestration — up to two rows per day (gainer + loser sides)."""
from __future__ import annotations

import logging
from datetime import date
from typing import Any, Dict, List, Optional, Set, Tuple

from backend.services.btst_backtest.config import get_config
from backend.services.btst_backtest.daily_screener import (
    load_fno_stock_universe,
    rank_both_sides,
)
from backend.services.btst_backtest.data_access import BtstDataAccess
from backend.services.btst_backtest.entry_evaluator import evaluate_entry
from backend.services.btst_backtest.exit_manager import compute_exits
from backend.services.btst_backtest.gates import select_daily_candidate
from backend.services.btst_backtest import progress as btst_progress
from backend.services.btst_backtest.repository import (
    create_run,
    fetch_earliest_trade_date,
    fetch_failed_row_keys,
    upsert_result,
)
from backend.services.btst_backtest.timing import (
    full_trading_calendar_span,
    recent_trading_days,
    trading_days_before,
)
from backend.services.market_holiday import refresh_holiday_dates_from_db

logger = logging.getLogger(__name__)

SIDES = ("gainer", "loser")


def _empty_row(trade_date: date, side: str, *, reason: str) -> Dict[str, Any]:
    return {
        "trade_date": trade_date,
        "side": side,
        "stock_symbol": None,
        "change_pct_at_1445": None,
        "rank_type": None,
        "scan_rank": None,
        "spot_price_1445": None,
        "cpr_pivot": None,
        "cpr_tc": None,
        "cpr_bc": None,
        "cpr_gate_pass": None,
        "rsi_14_5min": None,
        "rsi_gate_pass": None,
        "liquidity_gate_pass": None,
        "direction": "bullish" if side == "gainer" else "bearish",
        "atm_strike": None,
        "option_symbol": None,
        "data_mode": None,
        "premium_at_1500": None,
        "supertrend_pass": None,
        "hull_pass": None,
        "entry_time": None,
        "entry_premium": None,
        "lot_size": None,
        "buy_cost": None,
        "exit_a_time": None,
        "exit_a_premium": None,
        "exit_a_pnl": None,
        "exit_b_time": None,
        "exit_b_premium": None,
        "exit_b_pnl": None,
        "eligible_final": False,
        "no_eligible_reason": reason,
    }


def _best_failed_audit(audit: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not audit:
        return {}

    def _score(a: Dict[str, Any]) -> int:
        s = 0
        if a.get("cpr_gate_pass"):
            s += 1
        if a.get("rsi_gate_pass"):
            s += 2
        if a.get("liquidity_gate_pass"):
            s += 4
        return s

    return max(audit, key=_score)


def _resolve_trading_days(
    *,
    trading_days: int,
    end_date: Optional[date],
    mode: str,
) -> List[date]:
    n = int(trading_days)
    if mode == "earlier":
        earliest = fetch_earliest_trade_date()
        if earliest is None:
            return recent_trading_days(n, end=end_date)
        return trading_days_before(n, earliest)
    return recent_trading_days(n, end=end_date)


def _process_side(
    data: BtstDataAccess,
    universe: List[Dict[str, str]],
    trade_date: date,
    trading_calendar: List[date],
    side: str,
    ranked: List[Dict[str, Any]],
    gate_cfg: Dict[str, Any],
    cfg: Dict[str, Any],
    day_reason: Optional[str],
) -> Dict[str, Any]:
    if day_reason:
        return _empty_row(trade_date, side, reason=day_reason)
    if not ranked:
        return _empty_row(trade_date, side, reason="no candidate passed all gates")
    selected, audit = select_daily_candidate(ranked, gate_cfg)
    if selected is None:
        fail = _best_failed_audit(audit)
        return {
            **_empty_row(trade_date, side, reason="no candidate passed all gates"),
            "stock_symbol": fail.get("stock_symbol"),
            "change_pct_at_1445": fail.get("change_pct_at_1445"),
            "rank_type": fail.get("rank_type"),
            "scan_rank": fail.get("scan_rank"),
            "spot_price_1445": fail.get("spot_price_1445"),
            "cpr_pivot": fail.get("cpr_pivot"),
            "cpr_tc": fail.get("cpr_tc"),
            "cpr_bc": fail.get("cpr_bc"),
            "cpr_gate_pass": fail.get("cpr_gate_pass"),
            "rsi_14_5min": fail.get("rsi_14_5min"),
            "rsi_gate_pass": fail.get("rsi_gate_pass"),
            "liquidity_gate_pass": fail.get("liquidity_gate_pass"),
            "direction": fail.get("direction"),
        }
    selected["side"] = side
    entry = evaluate_entry(data, selected, cfg)
    row = {
        "trade_date": trade_date,
        "side": side,
        "stock_symbol": selected["stock_symbol"],
        "change_pct_at_1445": selected["change_pct_at_1445"],
        "rank_type": selected["rank_type"],
        "scan_rank": selected["scan_rank"],
        "spot_price_1445": selected["spot_price_1445"],
        "cpr_pivot": selected["cpr_pivot"],
        "cpr_tc": selected["cpr_tc"],
        "cpr_bc": selected["cpr_bc"],
        "cpr_gate_pass": selected["cpr_gate_pass"],
        "rsi_14_5min": selected["rsi_14_5min"],
        "rsi_gate_pass": selected["rsi_gate_pass"],
        "liquidity_gate_pass": selected["liquidity_gate_pass"],
        "direction": entry.get("direction"),
        "atm_strike": entry.get("atm_strike"),
        "option_symbol": entry.get("option_symbol"),
        "data_mode": entry.get("data_mode"),
        "premium_at_1500": entry.get("premium_at_1500"),
        "supertrend_pass": entry.get("supertrend_pass"),
        "hull_pass": entry.get("hull_pass"),
        "entry_time": entry.get("entry_time"),
        "entry_premium": entry.get("entry_premium"),
        "lot_size": entry.get("lot_size"),
        "buy_cost": entry.get("buy_cost"),
        "eligible_final": entry.get("eligible_final", False),
        "no_eligible_reason": entry.get("no_eligible_reason"),
    }
    if entry.get("eligible_final") and entry.get("option_instrument_key"):
        exits = compute_exits(
            data,
            trade_date,
            entry["option_instrument_key"],
            entry.get("premium_candles") or [],
            entry.get("entry_premium"),
            entry.get("lot_size"),
            cfg,
        )
        row.update(exits)
    return row


def _day_skip_reason(
    trade_date: date,
    holidays: Set[date],
    data: BtstDataAccess,
    universe: List[Dict[str, str]],
) -> Optional[str]:
    if trade_date in holidays:
        return "no_data_holiday_or_gap"
    status = data.market_session_data_status(universe, trade_date)
    if status == "api_fetch_failed":
        return "api_fetch_failed"
    if status == "no_session_bars":
        return "no_data_holiday_or_gap"
    return None


def run_btst_backtest(
    *,
    trading_days: Optional[int] = None,
    end_date: Optional[date] = None,
    notes: str = "",
    mode: str = "recent",
    retry_keys: Optional[List[Tuple[date, str]]] = None,
) -> Dict[str, Any]:
    cfg = get_config()
    n_days = int(trading_days or cfg["trading_days_default"])
    if retry_keys:
        days = sorted({d for d, _ in retry_keys}, reverse=True)
    else:
        days = _resolve_trading_days(trading_days=n_days, end_date=end_date, mode=mode)
    if not days:
        return {"error": "no trading days"}
    run_id = create_run(min(days), max(days), notes or mode)
    data = BtstDataAccess()
    if not data.ux.access_token:
        return {"error": "no Upstox access token", "run_id": run_id}
    universe = load_fno_stock_universe()
    holidays = refresh_holiday_dates_from_db()
    trading_calendar = full_trading_calendar_span(days)
    window_start = min(trading_calendar) if trading_calendar else min(days)
    window_end = max(days)
    btst_progress.set_run_created(
        run_id,
        days_total=len(days),
        prefetch_total=len(universe),
        window_start=window_start.isoformat(),
        window_end=window_end.isoformat(),
    )
    logger.info("BTST prefetch window %s .. %s (%s instruments)", window_start, window_end, len(universe))
    prefetch = data.prefetch_universe(universe, window_start, window_end)
    gate_cfg = {
        "snapshot_hhmm": cfg["snapshot_hhmm"],
        "rsi_bull_min": cfg["rsi_bull_min"],
        "rsi_bull_max": cfg["rsi_bull_max"],
        "rsi_bear_min": cfg["rsi_bear_min"],
        "rsi_bear_max": cfg["rsi_bear_max"],
        "liquidity_min_volume_1445": cfg["liquidity_min_volume_1445"],
    }
    row_ids: List[int] = []
    retry_set = set(retry_keys or [])
    for i, trade_date in enumerate(days):
        btst_progress.set_screening(i + 1, len(days), trade_date.isoformat())
        logger.info("BTST day %s/%s: %s", i + 1, len(days), trade_date)
        day_reason = _day_skip_reason(trade_date, holidays, data, universe)
        sides_to_run = list(SIDES)
        if retry_set:
            sides_to_run = [s for s in SIDES if (trade_date, s) in retry_set]
            if not sides_to_run:
                continue
        ranked_both = (
            {}
            if day_reason
            else rank_both_sides(
                universe,
                data,
                trade_date,
                trading_calendar,
                top_n=int(cfg["top_n_per_side"]),
                snapshot_hhmm=str(cfg["snapshot_hhmm"]),
            )
        )
        for side in sides_to_run:
            ranked = ranked_both.get(side, []) if not day_reason else []
            row = _process_side(
                data,
                universe,
                trade_date,
                trading_calendar,
                side,
                ranked,
                gate_cfg,
                cfg,
                day_reason,
            )
            row_ids.append(upsert_result(run_id, row))
    return {
        "run_id": run_id,
        "days": len(days),
        "result_ids": row_ids,
        "prefetch_failed_instruments": len(prefetch.failed_keys),
    }


def run_btst_retry_failed(*, notes: str = "retry_failed") -> Dict[str, Any]:
    keys = fetch_failed_row_keys()
    if not keys:
        return {"error": "no failed rows to retry", "result_ids": []}
    return run_btst_backtest(notes=notes, mode="retry", retry_keys=keys, trading_days=0)

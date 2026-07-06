"""Main backtest orchestration loop — one row per trading day."""
from __future__ import annotations

import logging
from datetime import date
from typing import Any, Dict, List, Optional

from backend.services.btst_backtest.config import get_config
from backend.services.btst_backtest.daily_screener import (
    load_fno_stock_universe,
    nifty_scan_side,
    rank_candidates_for_side,
)
from backend.services.btst_backtest.data_access import BtstDataAccess
from backend.services.btst_backtest.entry_evaluator import evaluate_entry
from backend.services.btst_backtest.exit_manager import compute_exits
from backend.services.btst_backtest.gates import select_daily_candidate
from backend.services.btst_backtest.repository import create_run, insert_result
from backend.services.btst_backtest.timing import last_n_trading_days

logger = logging.getLogger(__name__)


def _empty_row(
    trade_date: date,
    *,
    nifty_change_pct=None,
    scan_side=None,
    reason: str,
) -> Dict[str, Any]:
    return {
        "trade_date": trade_date,
        "nifty_change_pct": nifty_change_pct,
        "scan_side": scan_side,
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
        "direction": None,
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


def run_btst_backtest(
    *,
    trading_days: Optional[int] = None,
    end_date: Optional[date] = None,
    notes: str = "",
) -> Dict[str, Any]:
    cfg = get_config()
    n_days = int(trading_days or cfg["trading_days_default"])
    days = last_n_trading_days(n_days, end=end_date)
    if not days:
        return {"error": "no trading days"}
    run_id = create_run(days[0], days[-1], notes)
    data = BtstDataAccess()
    if not data.ux.access_token:
        return {"error": "no Upstox access token", "run_id": run_id}
    universe = load_fno_stock_universe()
    nifty_key = str(cfg["nifty_instrument_key"])
    gate_cfg = {
        "snapshot_hhmm": cfg["snapshot_hhmm"],
        "rsi_bull_min": cfg["rsi_bull_min"],
        "rsi_bull_max": cfg["rsi_bull_max"],
        "rsi_bear_min": cfg["rsi_bear_min"],
        "rsi_bear_max": cfg["rsi_bear_max"],
        "liquidity_min_volume_1445": cfg["liquidity_min_volume_1445"],
    }
    row_ids: List[int] = []
    for i, trade_date in enumerate(days):
        logger.info("BTST backtest day %s/%s: %s", i + 1, len(days), trade_date)
        if not data.session_has_equity_bars(nifty_key, trade_date):
            row = _empty_row(trade_date, reason="no_data_holiday_or_gap")
            row_ids.append(insert_result(run_id, row))
            continue
        scan_side, nifty_chg = nifty_scan_side(
            data,
            trade_date,
            days,
            nifty_key,
            flat_epsilon_pct=float(cfg["nifty_flat_epsilon_pct"]),
        )
        ranked = rank_candidates_for_side(
            universe,
            data,
            trade_date,
            days,
            scan_side,
            top_n=int(cfg["top_n_per_side"]),
            snapshot_hhmm=str(cfg["snapshot_hhmm"]),
        )
        if not ranked:
            row = _empty_row(
                trade_date,
                nifty_change_pct=nifty_chg,
                scan_side=scan_side,
                reason="no_data_holiday_or_gap",
            )
            row_ids.append(insert_result(run_id, row))
            continue
        selected, audit = select_daily_candidate(ranked, gate_cfg)
        if selected is None:
            fail = _best_failed_audit(audit)
            row = {
                **_empty_row(trade_date, nifty_change_pct=nifty_chg, scan_side=scan_side, reason="no candidate passed all gates"),
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
            row_ids.append(insert_result(run_id, row))
            continue
        entry = evaluate_entry(data, selected, cfg)
        row = {
            "trade_date": trade_date,
            "nifty_change_pct": nifty_chg,
            "scan_side": scan_side,
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
        row_ids.append(insert_result(run_id, row))
    return {"run_id": run_id, "days": len(days), "result_ids": row_ids}

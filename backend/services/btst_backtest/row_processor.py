"""Process one ChartInk CSV row — option resolve, premiums, gates, PnL."""
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, Dict, Optional, Tuple

import pytz

from backend.services.btst_backtest.atm import resolve_atm_option
from backend.services.btst_backtest.config import get_config
from backend.services.btst_backtest.data_access import BtstDataAccess, FetchOutcome
from backend.services.btst_backtest.gates import check_hull_gate, check_supertrend_gate
from backend.services.btst_backtest.timing import close_at_or_before, next_trading_day
from backend.services.symbol_isin_mapping import get_instrument_key

IST = pytz.timezone("Asia/Kolkata")


def trading_sessions_since(trade_date: date, *, end: Optional[date] = None) -> int:
    """Trading sessions strictly after trade_date through end (default today)."""
    end = end or datetime.now(IST).date()
    if trade_date >= end:
        return 0
    n = 0
    d = trade_date + timedelta(days=1)
    while d <= end:
        if d.weekday() < 5:
            n += 1
        d += timedelta(days=1)
    return n


def is_within_premium_history_window(trade_date: date, *, window_days: int = 24) -> bool:
    return trading_sessions_since(trade_date) <= int(window_days)


def compute_change_pct(prev_close: float, price_1445: float) -> float:
    if not prev_close:
        return 0.0
    return (float(price_1445) - float(prev_close)) / float(prev_close) * 100.0


def direction_from_change_pct(change_pct: float) -> str:
    return "CE" if float(change_pct) >= 0 else "PE"


def resolve_option_from_csv_row(
    stock_symbol: str,
    trade_date: date,
    spot_1500: float,
    direction: str,
) -> Tuple[Optional[float], Optional[str], Optional[str], Optional[int]]:
    """ATM strike, option symbol, numeric instrument key, lot size."""
    return resolve_atm_option(stock_symbol, spot_1500, trade_date, direction)


def fetch_premium_at_times(
    premium_candles: list,
    trade_date: date,
    cfg: Dict[str, Any],
) -> Dict[str, Optional[float]]:
    nd = next_trading_day(trade_date)
    return {
        "entry_premium": close_at_or_before(premium_candles, trade_date, cfg["entry_hhmm"]),
        "exit_a_premium": close_at_or_before(premium_candles, trade_date, cfg["exit_a_hhmm"]),
        "exit_b_premium": close_at_or_before(premium_candles, nd, cfg["exit_b_hhmm"]),
    }


def compute_pnl(
    entry_premium: Optional[float],
    exit_a_premium: Optional[float],
    exit_b_premium: Optional[float],
    lot_size: Optional[int],
) -> Dict[str, Optional[float]]:
    if entry_premium is None or not lot_size:
        return {"buy_cost": None, "exit_a_pnl": None, "exit_b_pnl": None}
    lot = int(lot_size)
    buy_cost = float(entry_premium) * lot
    exit_a_pnl = (
        (float(exit_a_premium) - float(entry_premium)) * lot
        if exit_a_premium is not None
        else None
    )
    exit_b_pnl = (
        (float(exit_b_premium) - float(entry_premium)) * lot
        if exit_b_premium is not None
        else None
    )
    return {"buy_cost": buy_cost, "exit_a_pnl": exit_a_pnl, "exit_b_pnl": exit_b_pnl}


def process_csv_row(
    data: BtstDataAccess,
    csv_row: Dict[str, Any],
    cfg: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Full pipeline for one CSV row → DB-ready dict."""
    cfg = cfg or get_config()
    trade_date = csv_row["trade_date"]
    sym = csv_row["stock_symbol"]
    sector = csv_row.get("sector")

    base: Dict[str, Any] = {
        "trade_date": trade_date,
        "stock_symbol": sym,
        "sector": sector,
        "change_pct": None,
        "reference_price": None,
        "atm_strike": None,
        "direction": None,
        "option_symbol": None,
        "numeric_instrument_key": None,
        "data_mode": None,
        "supertrend_pass": None,
        "hull_pass": None,
        "eligible_final": False,
        "entry_premium": None,
        "exit_a_premium": None,
        "exit_b_premium": None,
        "lot_size": None,
        "buy_cost": None,
        "exit_a_pnl": None,
        "exit_b_pnl": None,
        "no_data_reason": None,
    }

    if not data.ux.access_token:
        base["no_data_reason"] = "no_upstox_token"
        return base

    eq_key = get_instrument_key(sym)
    prev_close = data.previous_close(eq_key, trade_date)
    price_1445 = data.spot_at(eq_key, trade_date, cfg["snapshot_hhmm"])
    if prev_close is None or price_1445 is None:
        base["no_data_reason"] = "equity_price_fetch_failed"
        return base

    change_pct = compute_change_pct(prev_close, price_1445)
    direction = direction_from_change_pct(change_pct)
    base["change_pct"] = change_pct
    base["reference_price"] = price_1445
    base["direction"] = direction

    spot_1500 = data.spot_at(eq_key, trade_date, cfg["atm_hhmm"])
    if spot_1500 is None:
        base["no_data_reason"] = "spot_1500_fetch_failed"
        return base

    atm_strike, option_symbol, option_key, lot_size = resolve_option_from_csv_row(
        sym, trade_date, spot_1500, direction
    )
    base["atm_strike"] = atm_strike
    base["option_symbol"] = option_symbol
    base["numeric_instrument_key"] = option_key
    base["lot_size"] = lot_size

    if not option_key or lot_size is None:
        base["data_mode"] = "manual_fill"
        base["no_data_reason"] = "option_unresolved"
        return base

    window_days = int(cfg.get("premium_history_trading_days", 24))
    if not is_within_premium_history_window(trade_date, window_days=window_days):
        base["data_mode"] = "manual_fill"
        base["no_data_reason"] = "premium_history_outside_window"
        return base

    outcome, premium_candles = data.option_premium_candles(option_key, trade_date)
    if outcome == FetchOutcome.FAILED:
        base["no_data_reason"] = "option_premium_fetch_failed"
        return base
    if outcome == FetchOutcome.EMPTY:
        base["data_mode"] = "manual_fill"
        base["no_data_reason"] = "premium_history_unavailable"
        return base

    premiums = fetch_premium_at_times(premium_candles, trade_date, cfg)
    base.update(premiums)
    base["data_mode"] = "full"

    st_pass, _ = check_supertrend_gate(
        premium_candles,
        trade_date,
        cfg["premium_gate_hhmm"],
        period=int(cfg["supertrend_period"]),
        multiplier=float(cfg["supertrend_multiplier"]),
    )
    hull_pass, _, _ = check_hull_gate(
        premium_candles,
        trade_date,
        cfg["premium_gate_hhmm"],
        length=int(cfg["hull_length"]),
    )
    base["supertrend_pass"] = st_pass
    base["hull_pass"] = hull_pass
    base["eligible_final"] = bool(st_pass and hull_pass)
    if not base["eligible_final"]:
        base["no_data_reason"] = "premium_indicators_failed"

    base.update(
        compute_pnl(
            base.get("entry_premium"),
            base.get("exit_a_premium"),
            base.get("exit_b_premium"),
            lot_size,
        )
    )
    return base

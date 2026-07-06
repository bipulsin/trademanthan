"""Pure gate functions — no data fetching, reusable for live trading later."""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from backend.services.btst_backtest.indicators import (
    compute_cpr,
    hma_last_two,
    rsi_at_session_close,
    supertrend_series,
)
from backend.services.btst_backtest.timing import cumulative_volume_through


def check_cpr_gate(
    direction: str,
    spot_price: float,
    prev_day_ohlc: Dict[str, float],
) -> Tuple[bool, float, float, float]:
    pivot, tc, bc = compute_cpr(
        float(prev_day_ohlc["high"]),
        float(prev_day_ohlc["low"]),
        float(prev_day_ohlc["close"]),
    )
    if direction == "bullish":
        passed = spot_price > pivot
    else:
        passed = spot_price < pivot
    return passed, pivot, tc, bc


def check_rsi_gate(
    direction: str,
    candles_5min: List[dict],
    trade_date,
    snapshot_hhmm: str,
    *,
    bull_min: float = 55,
    bull_max: float = 70,
    bear_min: float = 25,
    bear_max: float = 40,
) -> Tuple[bool, Optional[float]]:
    rsi = rsi_at_session_close(candles_5min, trade_date, snapshot_hhmm)
    if rsi is None:
        return False, None
    if direction == "bullish":
        passed = bull_min <= rsi <= bull_max
    else:
        passed = bear_min <= rsi <= bear_max
    return passed, rsi


def check_liquidity_gate(
    equity_volume_by_1445: float,
    min_volume_threshold: float,
) -> bool:
    return float(equity_volume_by_1445) >= float(min_volume_threshold)


def check_supertrend_gate(
    premium_candles: List[dict],
    trade_date,
    gate_hhmm: str,
    *,
    period: int = 10,
    multiplier: float = 3.0,
) -> Tuple[bool, Optional[float]]:
    from backend.services.btst_backtest.timing import bar_minutes, bars_on_session, parse_hhmm

    h, m = parse_hhmm(gate_hhmm)
    target = h * 60 + m
    session = bars_on_session(premium_candles, trade_date)
    subset = []
    for c in session:
        tm = bar_minutes(c.get("timestamp"))
        if tm is not None and tm <= target:
            subset.append(c)
    if len(subset) < max(20, period + 3):
        return False, None
    highs = [float(c.get("high") or 0) for c in subset]
    lows = [float(c.get("low") or 0) for c in subset]
    closes = [float(c.get("close") or 0) for c in subset]
    st_vals, _dirs = supertrend_series(highs, lows, closes, period, multiplier)
    if not st_vals:
        return False, None
    st_line = st_vals[-1]
    close = closes[-1]
    passed = close > st_line
    return passed, st_line


def check_hull_gate(
    premium_candles: List[dict],
    trade_date,
    gate_hhmm: str,
    *,
    length: int = 32,
) -> Tuple[bool, Optional[float], bool]:
    from backend.services.btst_backtest.timing import bar_minutes, bars_on_session, parse_hhmm

    h, m = parse_hhmm(gate_hhmm)
    target = h * 60 + m
    session = bars_on_session(premium_candles, trade_date)
    subset = []
    for c in session:
        tm = bar_minutes(c.get("timestamp"))
        if tm is not None and tm <= target:
            subset.append(c)
    if len(subset) < length + 2:
        return False, None, False
    closes = [float(c.get("close") or 0) for c in subset]
    hma_cur, hma_prev = hma_last_two(closes, length)
    if hma_cur is None or hma_prev is None:
        return False, hma_cur, False
    close = closes[-1]
    rising = hma_cur > hma_prev
    passed = close > hma_cur and rising
    return passed, hma_cur, rising


def select_daily_candidate(
    ranked_candidates: List[Dict[str, Any]],
    gate_fns: Dict[str, Any],
) -> Tuple[Optional[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Walk ranked list; return (first passing all gates, audit trail for all walked).
    """
    audit: List[Dict[str, Any]] = []
    for rank_idx, cand in enumerate(ranked_candidates, start=1):
        direction = cand["direction"]
        cpr_pass, pivot, tc, bc = check_cpr_gate(
            direction, cand["spot_price_1445"], cand["prev_day_ohlc"]
        )
        rsi_pass, rsi_val = check_rsi_gate(
            direction,
            cand["candles_5min"],
            cand["trade_date"],
            gate_fns["snapshot_hhmm"],
            bull_min=gate_fns["rsi_bull_min"],
            bull_max=gate_fns["rsi_bull_max"],
            bear_min=gate_fns["rsi_bear_min"],
            bear_max=gate_fns["rsi_bear_max"],
        )
        vol = cumulative_volume_through(
            cand["candles_5min"], cand["trade_date"], gate_fns["snapshot_hhmm"]
        )
        liq_pass = check_liquidity_gate(vol, gate_fns["liquidity_min_volume_1445"])
        cand_out = {
            **cand,
            "scan_rank": rank_idx,
            "cpr_pivot": pivot,
            "cpr_tc": tc,
            "cpr_bc": bc,
            "cpr_gate_pass": cpr_pass,
            "rsi_14_5min": rsi_val,
            "rsi_gate_pass": rsi_pass,
            "liquidity_gate_pass": liq_pass,
            "equity_volume_1445": vol,
        }
        audit.append(cand_out)
        if cpr_pass and rsi_pass and liq_pass:
            return cand_out, audit
    return None, audit

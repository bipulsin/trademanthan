"""Daily Expected-Value allocator with vol-buffered stops and ₹8k risk."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from rocket.engine.backtester import compute_structural_stop_target
from rocket.ml.meta_filter import RocketMetaFilter

logger = logging.getLogger(__name__)

MIN_PROB = 0.40  # sigmoid floor for EV selection
DEFAULT_KELLY_FACTOR = 0.35
DEFAULT_MIN_TRADES_PER_DAY = 2
DEFAULT_MAX_TRADES_PER_DAY = 3
MAX_RISK_RUPEES = 8000.0  # 0.08% of ₹1Cr
EMA5_MAX_DIST_ATR = 0.70
EMA20_MAX_DIST_ATR = 1.80
RSI_OVERSOLD = 25.0
RSI_OVERBOUGHT = 75.0

# Back-compat aliases
ANOMALY_FLOOR = MIN_PROB
HARD_FLOOR = MIN_PROB
TIER1_PROB = 0.62
TIER2_PROB = MIN_PROB
Z_SCORE_MIN = 0.0  # unused; kept for older imports


def fractional_kelly(p_win: float, reward_risk: float, *, kelly_factor: float = DEFAULT_KELLY_FACTOR) -> float:
    """f* = clip( (P·R − (1−P)) / R , 0, 1 ) × kelly_factor"""
    p = float(np.clip(p_win, 0.0, 1.0))
    r = float(reward_risk)
    if r <= 0:
        return 0.0
    raw = (p * r - (1.0 - p)) / r
    return float(np.clip(raw, 0.0, 1.0) * float(kelly_factor))


def expected_value(p_win: float, reward_risk: float) -> float:
    """EV in R-units: (P × R) − (1 − P)."""
    p = float(np.clip(p_win, 0.0, 1.0))
    r = float(reward_risk)
    return (p * r) - (1.0 - p)


def _atr_from_row(row: Dict[str, Any], entry: float) -> float:
    for key in ("safe_atr", "atr", "atr_14"):
        val = row.get(key)
        if val is not None and np.isfinite(float(val)) and float(val) > 0:
            return float(val)
    atr_pct = row.get("atr_pct")
    if atr_pct is not None and np.isfinite(float(atr_pct)) and float(atr_pct) > 0:
        return abs(entry) * float(atr_pct) / 100.0
    return abs(entry) * 0.005


def _raw_rsi(row: Dict[str, Any]) -> Optional[float]:
    val = row.get("raw_rsi_14")
    if val is not None and np.isfinite(float(val)):
        return float(val)
    return None


def _dist_atr(row: Dict[str, Any], key: str, entry: float, atr: float, level_key: str) -> float:
    val = row.get(key)
    if val is not None and np.isfinite(float(val)):
        return float(val)
    level = row.get(level_key)
    if level is not None and atr > 0 and np.isfinite(float(level)):
        return abs(entry - float(level)) / atr
    return 0.0


def entry_gate_reject_reason(row: Dict[str, Any], *, side: str, entry: float, atr: float) -> Optional[str]:
    """Return rejection code or None if gates pass."""
    if _dist_atr(row, "ema5_dist_atr", entry, atr, "ema_5") > EMA5_MAX_DIST_ATR:
        return "REJECT_MID_AIR_CHASE"
    if _dist_atr(row, "ema20_dist_atr", entry, atr, "ema_20") > EMA20_MAX_DIST_ATR:
        return "REJECT_EMA20_EXTENSION"

    raw_rsi = _raw_rsi(row)
    if raw_rsi is not None:
        if side in ("SELL", "SHORT") and raw_rsi < RSI_OVERSOLD:
            return "REJECT_RSI_EXHAUSTION_SHORT"
        if side in ("BUY", "LONG") and raw_rsi > RSI_OVERBOUGHT:
            return "REJECT_RSI_EXHAUSTION_LONG"
    return None


def apply_tiered_sizing(
    row: Dict[str, Any],
    *,
    kelly_factor: float = DEFAULT_KELLY_FACTOR,
    anomaly_floor: float = MIN_PROB,
    max_risk_rupees: float = MAX_RISK_RUPEES,
    is_top_rank: bool = False,
    tier1_prob: float = TIER1_PROB,
    soft_floor: float = MIN_PROB,
    hard_floor: float = MIN_PROB,
    require_positive_ev: bool = True,
) -> Optional[Dict[str, Any]]:
    """
    Enrich a scored signal with vol-buffered SL/TP, EV, and ₹8k risk-capped lots.

    Inclusion is decided by daily EV ranking; this enforces P≥0.40, EV>0 (optional),
    proximity gates, and monetary risk. 2 lots only when ``is_top_rank`` and
    2×risk ≤ ₹8,000.
    """
    _ = (tier1_prob, soft_floor, hard_floor)
    p = float(row.get("win_probability") or 0.0)
    if p < float(anomaly_floor):
        return None

    side = str(row.get("side") or row.get("bias") or "BUY").upper()
    if side in ("LONG",):
        side = "BUY"
    if side in ("SHORT",):
        side = "SELL"

    entry = float(row.get("entry_price") or row.get("close") or 0.0)
    if entry <= 0:
        return None
    atr = _atr_from_row(row, entry)

    reject = entry_gate_reject_reason(row, side=side, entry=entry, atr=atr)
    if reject:
        logger.debug("%s %s @ %.2f — %s", side, row.get("symbol"), entry, reject)
        return None

    levels = compute_structural_stop_target(
        side=side,
        entry_price=entry,
        ema_20=row.get("ema_20"),
        ema_10=row.get("ema_10"),
        vwap=row.get("vwap"),
        safe_atr=atr,
    )
    stop_loss = float(levels["stop_loss"])
    take_profit = float(levels["take_profit"])
    stop_dist = float(levels["stop_distance"])
    target_dist = float(levels["target_distance"])
    rr = (target_dist / stop_dist) if stop_dist > 0 else 1.8
    ev = expected_value(p, rr)
    if require_positive_ev and ev <= 0.0:
        return None
    f_star = fractional_kelly(p, rr, kelly_factor=kelly_factor)

    lot_size = int(row.get("lot_size") or 0)
    if lot_size <= 0:
        lot_size = 1
    per_share_risk = abs(entry - stop_loss)
    risk_per_lot = per_share_risk * lot_size
    if risk_per_lot > float(max_risk_rupees):
        logger.debug(
            "REJECT_MAX_RISK_EXCEEDED %s risk/lot=₹%.0f > ₹%.0f",
            row.get("symbol"),
            risk_per_lot,
            max_risk_rupees,
        )
        return None

    if is_top_rank and (risk_per_lot * 2) <= float(max_risk_rupees) and f_star > 0:
        tier = 1
        lots = 2
    else:
        tier = 2
        lots = 1

    out = dict(row)
    out.update(
        {
            "side": side,
            "tier": tier,
            "is_top_rank": bool(is_top_rank),
            "kelly_fraction": round(f_star, 6),
            "reward_risk": round(rr, 4),
            "expected_value": round(ev, 6),
            "lots": int(lots),
            "lot_size": lot_size,
            "quantity": int(lots) * lot_size,
            "atr": atr,
            "safe_atr": atr,
            "stop_loss": float(stop_loss),
            "take_profit": float(take_profit),
            "target_price": float(take_profit),
            "stop_distance": round(stop_dist, 6),
            "target_distance": round(target_dist, 6),
            "risk_per_lot": round(risk_per_lot, 2),
            "total_risk": round(risk_per_lot * lots, 2),
            "stop_kind": "vol_buffered" if levels.get("stop_kind", 0) >= 1.0 else "atr_floor",
            "ema5_dist_atr": round(_dist_atr(row, "ema5_dist_atr", entry, atr, "ema_5"), 6),
            "ema20_dist_atr": round(_dist_atr(row, "ema20_dist_atr", entry, atr, "ema_20"), 6),
        }
    )
    return out


class DailyTradeRanker:
    """
    Daily Expected-Value top-K (2–3/day).

    Candidates must pass proximity/RSI gates, ``P ≥ 0.40``, and ``EV > 0``,
    then the top scores by ``expected_value`` are kept (₹8k risk budget).
    """

    def __init__(
        self,
        meta_filter: Optional[RocketMetaFilter] = None,
        *,
        min_probability_threshold: float = MIN_PROB,
        hard_floor: float = MIN_PROB,
        min_trades_per_day: int = DEFAULT_MIN_TRADES_PER_DAY,
        max_trades_per_day: int = DEFAULT_MAX_TRADES_PER_DAY,
        kelly_factor: float = DEFAULT_KELLY_FACTOR,
        high_conviction_prob: float = TIER1_PROB,
        max_risk_rupees: float = MAX_RISK_RUPEES,
        anomaly_floor: Optional[float] = None,
        z_score_min: float = Z_SCORE_MIN,
    ):
        self.meta_filter = meta_filter
        _ = z_score_min
        # EV gate is P≥0.40; remap legacy 0.20/0.30 floors upward.
        raw = float(anomaly_floor if anomaly_floor is not None else min_probability_threshold)
        self.min_prob = MIN_PROB if raw < MIN_PROB else float(raw)
        self.anomaly_floor = self.min_prob
        self.hard_floor = self.min_prob
        self.min_trades = int(min_trades_per_day)
        self.max_trades = int(max_trades_per_day)
        self.kelly_factor = float(kelly_factor)
        self.high_conviction_prob = float(high_conviction_prob)
        self.max_risk_rupees = float(max_risk_rupees)

    def rank_and_select(
        self,
        scored_signals: List[Dict[str, Any]] | pd.DataFrame,
    ) -> List[Dict[str, Any]]:
        if scored_signals is None:
            return []
        df = (
            scored_signals
            if isinstance(scored_signals, pd.DataFrame)
            else pd.DataFrame(list(scored_signals))
        )
        if df.empty:
            return []

        df = df.copy()
        if "trade_date" not in df.columns:
            df["trade_date"] = pd.to_datetime(df["timestamp"]).dt.date.astype(str)
        else:
            df["trade_date"] = df["trade_date"].astype(str)

        selected: List[Dict[str, Any]] = []
        for _date, group in df.groupby("trade_date", sort=True):
            g = group.copy()
            # One position per symbol per day (keep highest raw score first)
            g = g.sort_values(
                ["win_probability", "strategy_confidence"],
                ascending=[False, False],
                na_position="last",
            )
            g = g.drop_duplicates(subset=["symbol"], keep="first")
            if g.empty:
                continue

            pool: List[Dict[str, Any]] = []
            for _, row in g.iterrows():
                sized = apply_tiered_sizing(
                    row.to_dict(),
                    kelly_factor=self.kelly_factor,
                    anomaly_floor=self.min_prob,
                    max_risk_rupees=self.max_risk_rupees,
                    is_top_rank=False,
                    require_positive_ev=True,
                )
                if sized is not None:
                    pool.append(sized)
            if not pool:
                continue

            pool.sort(
                key=lambda r: (
                    float(r.get("expected_value") or -1e9),
                    float(r.get("win_probability") or 0.0),
                    float(r.get("strategy_confidence") or 0.0),
                ),
                reverse=True,
            )

            day_picks: List[Dict[str, Any]] = []
            for i, cand in enumerate(pool):
                if len(day_picks) >= self.max_trades:
                    break
                if i == 0:
                    # Re-size top EV name for optional 2-lot allocation
                    top = apply_tiered_sizing(
                        {k: v for k, v in cand.items() if k not in ("lots", "quantity", "tier", "total_risk")},
                        kelly_factor=self.kelly_factor,
                        anomaly_floor=self.min_prob,
                        max_risk_rupees=self.max_risk_rupees,
                        is_top_rank=True,
                        require_positive_ev=True,
                    )
                    day_picks.append(top if top is not None else cand)
                else:
                    day_picks.append(cand)
            selected.extend(day_picks)

        return selected

    def selected_keys(self, selected: List[Dict[str, Any]]) -> set[Tuple[str, str, str]]:
        keys: set[Tuple[str, str, str]] = set()
        for row in selected:
            ts = row.get("timestamp")
            ts_s = ts.isoformat() if hasattr(ts, "isoformat") else str(ts)
            side = str(row.get("side") or row.get("bias") or "").upper()
            keys.add((str(row["symbol"]), ts_s, side))
        return keys

    def selected_by_key(self, selected: List[Dict[str, Any]]) -> Dict[Tuple[str, str, str], Dict[str, Any]]:
        out: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
        for row in selected:
            ts = row.get("timestamp")
            ts_s = ts.isoformat() if hasattr(ts, "isoformat") else str(ts)
            side = str(row.get("side") or row.get("bias") or "").upper()
            out[(str(row["symbol"]), ts_s, side)] = row
        return out

"""Daily top-K allocator: P≥0.50 floor, EMA5/RSI gates, structural SL, ₹3k risk cap."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from rocket.engine.backtester import compute_structural_stop_target
from rocket.ml.meta_filter import RocketMetaFilter

logger = logging.getLogger(__name__)

TIER1_PROB = 0.62  # high conviction
TIER2_PROB = 0.50  # absolute soft-fill floor / standard
HARD_FLOOR = 0.50  # never select below this
DEFAULT_KELLY_FACTOR = 0.35
DEFAULT_MAX_TRADES_PER_DAY = 3
MAX_RISK_RUPEES = 3000.0
EMA5_MAX_DIST_ATR = 0.35
RSI_OVERSOLD = 30.0
RSI_OVERBOUGHT = 70.0


def fractional_kelly(p_win: float, reward_risk: float, *, kelly_factor: float = DEFAULT_KELLY_FACTOR) -> float:
    """
    Fractional Kelly fraction.

    f* = clip( (P·R − (1−P)) / R , 0, 1 ) × kelly_factor
    """
    p = float(np.clip(p_win, 0.0, 1.0))
    r = float(reward_risk)
    if r <= 0:
        return 0.0
    raw = (p * r - (1.0 - p)) / r
    return float(np.clip(raw, 0.0, 1.0) * float(kelly_factor))


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
    for key in ("raw_rsi_14",):
        val = row.get(key)
        if val is not None and np.isfinite(float(val)):
            return float(val)
    # Side-flipped rsi_14 is not usable for exhaustion gates
    return None


def _ema5_dist(row: Dict[str, Any], entry: float, atr: float) -> float:
    val = row.get("ema5_dist_atr")
    if val is not None and np.isfinite(float(val)):
        return float(val)
    ema5 = row.get("ema_5")
    if ema5 is not None and atr > 0 and np.isfinite(float(ema5)):
        return abs(entry - float(ema5)) / atr
    return 0.0


def entry_gate_reject_reason(row: Dict[str, Any], *, side: str, entry: float, atr: float) -> Optional[str]:
    """Return rejection code or None if gates pass."""
    dist = _ema5_dist(row, entry, atr)
    if dist > EMA5_MAX_DIST_ATR:
        return "REJECT_MID_AIR_CHASE"

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
    tier1_prob: float = TIER1_PROB,
    soft_floor: float = TIER2_PROB,
    hard_floor: float = HARD_FLOOR,
    max_risk_rupees: float = MAX_RISK_RUPEES,
) -> Optional[Dict[str, Any]]:
    """
    Enrich a scored signal with tier, structural SL/TP, and ₹risk-capped lots.

    Tier 1 (P ≥ 0.62): up to 2 lots if 2×risk ≤ ₹3,000
    Tier 2 (0.50 ≤ P < 0.62): 1 lot if risk ≤ ₹3,000
    P < 0.50 / mid-air / RSI exhaustion / risk > ₹3,000 → discard
    """
    p = float(row.get("win_probability") or 0.0)
    floor = max(float(hard_floor), float(soft_floor), HARD_FLOOR)
    if p < floor:
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
        ema_10=row.get("ema_10"),
        vwap=row.get("vwap"),
        safe_atr=atr,
    )
    stop_loss = float(levels["stop_loss"])
    take_profit = float(levels["take_profit"])
    stop_dist = float(levels["stop_distance"])
    target_dist = float(levels["target_distance"])
    rr = (target_dist / stop_dist) if stop_dist > 0 else 2.5
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

    if p >= tier1_prob:
        tier = 1
        if (risk_per_lot * 2) <= float(max_risk_rupees) and f_star > 0:
            lots = 2
        else:
            lots = 1
    else:
        tier = 2
        lots = 1

    out = dict(row)
    out.update(
        {
            "side": side,
            "tier": tier,
            "kelly_fraction": round(f_star, 6),
            "reward_risk": round(rr, 4),
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
            "stop_kind": "structural" if levels.get("stop_kind", 0) >= 1.0 else "atr_fallback",
            "ema5_dist_atr": round(_ema5_dist(row, entry, atr), 6),
        }
    )
    return out


class DailyTradeRanker:
    """Daily top-K (≤3) with absolute P≥0.50 floor (no soft-fill below 0.50)."""

    def __init__(
        self,
        meta_filter: Optional[RocketMetaFilter] = None,
        *,
        min_probability_threshold: float = TIER2_PROB,
        hard_floor: float = HARD_FLOOR,
        min_trades_per_day: int = 2,
        max_trades_per_day: int = DEFAULT_MAX_TRADES_PER_DAY,
        kelly_factor: float = DEFAULT_KELLY_FACTOR,
        high_conviction_prob: float = TIER1_PROB,
        max_risk_rupees: float = MAX_RISK_RUPEES,
    ):
        self.meta_filter = meta_filter
        # Absolute floor: never below 0.50 even if caller passes a lower value
        self.min_prob = max(float(min_probability_threshold), HARD_FLOOR)
        self.hard_floor = max(float(hard_floor), HARD_FLOOR)
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
            g = group.sort_values(
                ["win_probability", "strategy_confidence"],
                ascending=[False, False],
                na_position="last",
            )
            # One position per symbol per day
            g = g.drop_duplicates(subset=["symbol"], keep="first")

            # Absolute floor P≥0.50 — soft-fill cannot go below hard_floor
            floor = max(self.min_prob, self.hard_floor, HARD_FLOOR)
            pool = g[g["win_probability"] >= floor]
            if pool.empty:
                continue

            # Walk the full ranked pool so gate/risk rejects don't starve the day
            day_picks: List[Dict[str, Any]] = []
            for _, row in pool.iterrows():
                if len(day_picks) >= self.max_trades:
                    break
                sized = apply_tiered_sizing(
                    row.to_dict(),
                    kelly_factor=self.kelly_factor,
                    tier1_prob=self.high_conviction_prob,
                    soft_floor=self.min_prob,
                    hard_floor=self.hard_floor,
                    max_risk_rupees=self.max_risk_rupees,
                )
                if sized is not None:
                    day_picks.append(sized)
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

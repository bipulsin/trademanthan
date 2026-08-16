"""Daily top-K allocator with calibrated soft-floor and fixed 1.8×ATR stops."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from rocket.ml.meta_filter import RocketMetaFilter

# Fixed stop/target for all tiers (no 1.2× tightening)
STOP_ATR = 1.8
TARGET_ATR = 3.2

TIER1_PROB = 0.62  # high conviction
TIER2_PROB = 0.55  # preferred soft floor / standard
# Absolute discard when soft-filling toward min_trades/day (isotonic OOF is bimodal)
HARD_FLOOR = 0.20
DEFAULT_KELLY_FACTOR = 0.35
DEFAULT_MAX_TRADES_PER_DAY = 3


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
    for key in ("atr", "safe_atr", "atr_14"):
        val = row.get(key)
        if val is not None and np.isfinite(float(val)) and float(val) > 0:
            return float(val)
    atr_pct = row.get("atr_pct")
    if atr_pct is not None and np.isfinite(float(atr_pct)) and float(atr_pct) > 0:
        return abs(entry) * float(atr_pct) / 100.0
    return abs(entry) * 0.005


def apply_tiered_sizing(
    row: Dict[str, Any],
    *,
    kelly_factor: float = DEFAULT_KELLY_FACTOR,
    tier1_prob: float = TIER1_PROB,
    soft_floor: float = TIER2_PROB,
    hard_floor: float = HARD_FLOOR,
) -> Optional[Dict[str, Any]]:
    """
    Enrich a scored signal with tier, Kelly fraction, lots, and fixed 1.8/3.2 ATR levels.

    Tier 1 (P ≥ 0.62): 2 lots (Kelly-capped)
    Tier 2 (hard_floor ≤ P < 0.62): 1 lot  — includes soft-fill below preferred 0.55
    Below hard_floor: discard
    """
    p = float(row.get("win_probability") or 0.0)
    if p < hard_floor:
        return None

    side = str(row.get("side") or row.get("bias") or "BUY").upper()
    if side in ("LONG",):
        side = "BUY"
    if side in ("SHORT",):
        side = "SELL"
    direction = 1 if side in ("BUY", "LONG") else -1

    entry = float(row.get("entry_price") or row.get("close") or 0.0)
    if entry <= 0:
        return None
    atr = _atr_from_row(row, entry)

    stop_mult, target_mult = STOP_ATR, TARGET_ATR
    rr = target_mult / stop_mult  # ≈ 1.778
    f_star = fractional_kelly(p, rr, kelly_factor=kelly_factor)

    if p >= tier1_prob:
        tier = 1
        lots = 2
        if f_star <= 0:
            lots = 1
    else:
        # Preferred band is soft_floor→tier1; soft-filled marginals stay 1 lot
        tier = 2
        lots = 1
        _ = soft_floor  # documented preferred floor; sizing uses hard_floor gate

    stop_dist = stop_mult * atr
    target_dist = target_mult * atr
    if direction > 0:
        stop_loss = entry - stop_dist
        take_profit = entry + target_dist
    else:
        stop_loss = entry + stop_dist
        take_profit = entry - target_dist

    out = dict(row)
    out.update(
        {
            "side": side,
            "tier": tier,
            "kelly_fraction": round(f_star, 6),
            "reward_risk": round(rr, 4),
            "lots": int(lots),
            "atr": atr,
            "stop_loss": float(stop_loss),
            "take_profit": float(take_profit),
            "target_price": float(take_profit),
            "stop_atr_mult": stop_mult,
            "target_atr_mult": target_mult,
        }
    )
    return out


class DailyTradeRanker:
    """
    Daily top-K (≤3) with calibrated soft-floor.

    Prefer P ≥ 0.55; if a day is short of min_trades, soft-fill from next-best
    scores down to HARD_FLOOR so the window stays near 20–35 trades.
    """

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
    ):
        self.meta_filter = meta_filter
        self.min_prob = float(min_probability_threshold)
        self.hard_floor = float(hard_floor)
        self.min_trades = int(min_trades_per_day)
        self.max_trades = int(max_trades_per_day)
        self.kelly_factor = float(kelly_factor)
        self.high_conviction_prob = float(high_conviction_prob)

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

            preferred = g[g["win_probability"] >= self.min_prob]
            if len(preferred) >= self.min_trades:
                pick = preferred.head(self.max_trades)
            else:
                # Dynamic soft-fill: keep preferred, then next-best ≥ hard_floor
                pool = g[g["win_probability"] >= self.hard_floor]
                if pool.empty:
                    continue
                # Prefer filling to min_trades when possible; never exceed max
                n = min(self.max_trades, max(self.min_trades, len(preferred)), len(pool))
                n = min(self.max_trades, max(n, min(self.min_trades, len(pool))))
                pick = pool.head(n)

            for _, row in pick.iterrows():
                sized = apply_tiered_sizing(
                    row.to_dict(),
                    kelly_factor=self.kelly_factor,
                    tier1_prob=self.high_conviction_prob,
                    soft_floor=self.min_prob,
                    hard_floor=self.hard_floor,
                )
                if sized is not None:
                    selected.append(sized)

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

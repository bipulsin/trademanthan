"""Daily top-K allocator with volatility-adjusted fractional Kelly sizing."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from rocket.ml.meta_filter import RocketMetaFilter

# Tier ATR multiples (structural invalidation + reward)
TIER1_STOP_ATR = 1.2
TIER1_TARGET_ATR = 3.2
TIER2_STOP_ATR = 1.8
TIER2_TARGET_ATR = 3.2

TIER1_PROB = 0.75
TIER2_PROB = 0.65
DEFAULT_KELLY_FACTOR = 0.35


def fractional_kelly(p_win: float, reward_risk: float, *, kelly_factor: float = DEFAULT_KELLY_FACTOR) -> float:
    """
    Half/fractional Kelly fraction.

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
        # atr_pct in feature extractor is percent of price
        return abs(entry) * float(atr_pct) / 100.0
    return abs(entry) * 0.005


def apply_tiered_sizing(
    row: Dict[str, Any],
    *,
    kelly_factor: float = DEFAULT_KELLY_FACTOR,
    max_lots_tier1: int = 3,
    min_lots_tier1: int = 2,
) -> Optional[Dict[str, Any]]:
    """
    Enrich a scored signal with tier, Kelly fraction, lots, and recalibrated SL/TP.

    Returns None when P < Tier-2 threshold (discard).
    """
    p = float(row.get("win_probability") or 0.0)
    if p < TIER2_PROB:
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

    if p >= TIER1_PROB:
        tier = 1
        stop_mult, target_mult = TIER1_STOP_ATR, TIER1_TARGET_ATR
        rr = target_mult / stop_mult  # ≈ 2.667
        f_star = fractional_kelly(p, rr, kelly_factor=kelly_factor)
        # Map f* into 2–3 lots (higher conviction / Kelly → 3)
        lots = max_lots_tier1 if f_star >= (kelly_factor * 0.35) else min_lots_tier1
        lots = int(np.clip(lots, min_lots_tier1, max_lots_tier1))
    else:
        tier = 2
        stop_mult, target_mult = TIER2_STOP_ATR, TIER2_TARGET_ATR
        rr = target_mult / stop_mult  # ≈ 1.778
        f_star = fractional_kelly(p, rr, kelly_factor=kelly_factor)
        lots = 1

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
    """Keep 2–4 highest-conviction signals per session day with Kelly sizing."""

    def __init__(
        self,
        meta_filter: Optional[RocketMetaFilter] = None,
        *,
        min_probability_threshold: float = TIER2_PROB,
        min_trades_per_day: int = 2,
        max_trades_per_day: int = 4,
        kelly_factor: float = DEFAULT_KELLY_FACTOR,
        high_conviction_prob: float = TIER1_PROB,
    ):
        self.meta_filter = meta_filter
        self.min_prob = float(min_probability_threshold)
        self.min_trades = int(min_trades_per_day)
        self.max_trades = int(max_trades_per_day)
        self.kelly_factor = float(kelly_factor)
        self.high_conviction_prob = float(high_conviction_prob)

    def rank_and_select(
        self,
        scored_signals: List[Dict[str, Any]] | pd.DataFrame,
    ) -> List[Dict[str, Any]]:
        """
        Parameters
        ----------
        scored_signals
            Rows must include ``win_probability``, ``timestamp``, ``symbol``,
            and price/ATR context for sizing.
        """
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
            # Tier-3 discard: hard floor at min_prob (default 0.65)
            g = group[group["win_probability"] >= self.min_prob].copy()
            if g.empty:
                continue
            g = g.sort_values(
                ["win_probability", "strategy_confidence"],
                ascending=[False, False],
                na_position="last",
            )
            g = g.drop_duplicates(subset=["symbol"], keep="first")

            n = min(self.max_trades, len(g))
            if n < self.min_trades and len(g) >= self.min_trades:
                n = self.min_trades
            n = min(n, len(g), self.max_trades)
            alloc = g.head(n)

            for _, row in alloc.iterrows():
                sized = apply_tiered_sizing(row.to_dict(), kelly_factor=self.kelly_factor)
                if sized is not None:
                    selected.append(sized)

        return selected

    def selected_keys(self, selected: List[Dict[str, Any]]) -> set[Tuple[str, str, str]]:
        """Stable identity for backtester gate: (symbol, signal_ts_iso, side)."""
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

"""Daily top-K allocator for Rocket meta-filtered signals."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import pandas as pd

from rocket.ml.meta_filter import RocketMetaFilter


class DailyTradeRanker:
    """Keep 2–4 highest-conviction signals per session day."""

    def __init__(
        self,
        meta_filter: Optional[RocketMetaFilter] = None,
        *,
        min_probability_threshold: float = 0.65,
        min_trades_per_day: int = 2,
        max_trades_per_day: int = 4,
    ):
        self.meta_filter = meta_filter
        self.min_prob = float(min_probability_threshold)
        self.min_trades = int(min_trades_per_day)
        self.max_trades = int(max_trades_per_day)

    def rank_and_select(
        self,
        scored_signals: List[Dict[str, Any]] | pd.DataFrame,
    ) -> List[Dict[str, Any]]:
        """
        Parameters
        ----------
        scored_signals
            Rows must include ``win_probability``, ``timestamp``, ``symbol``,
            and preferably ``side`` / ``bias``.
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
            g = group.sort_values("win_probability", ascending=False)
            qualified = g[g["win_probability"] >= self.min_prob].copy()

            if qualified.empty:
                # Soft fallback: take up to min_trades if within 0.08 of threshold
                soft = g[g["win_probability"] >= (self.min_prob - 0.08)].head(self.min_trades)
                if soft.empty or float(soft.iloc[0]["win_probability"]) < (self.min_prob - 0.08):
                    continue
                pick = soft
            else:
                pick = qualified

            # One entry per symbol per day
            pick = pick.drop_duplicates(subset=["symbol"], keep="first")
            alloc = pick.head(self.max_trades)
            selected.extend(alloc.to_dict("records"))

        return selected

    def selected_keys(self, selected: List[Dict[str, Any]]) -> set[tuple]:
        """Stable identity for backtester gate: (symbol, signal_ts_iso, side)."""
        keys: set[tuple] = set()
        for row in selected:
            ts = row.get("timestamp")
            ts_s = ts.isoformat() if hasattr(ts, "isoformat") else str(ts)
            side = str(row.get("side") or row.get("bias") or "").upper()
            keys.add((str(row["symbol"]), ts_s, side))
        return keys

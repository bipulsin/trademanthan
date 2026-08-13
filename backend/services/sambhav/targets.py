"""Target generation — future close at T+30m (3 × 10m bars). No arbitrary threshold."""

from __future__ import annotations

from typing import Any, Dict, Sequence

import numpy as np
import pandas as pd

from backend.services.sambhav.candles import to_ist
from backend.services.sambhav.config import HORIZON_BARS
from backend.services.sambhav.features import bars_to_dataframe


def attach_targets(
    bars: Sequence[Dict[str, Any]] | pd.DataFrame,
    *,
    horizon_bars: int = HORIZON_BARS,
) -> pd.DataFrame:
    """
    For each bar at index i, future_close = close[i + horizon_bars].
    target_direction = UP if future_close > close else DOWN (ties → DOWN).
    future_return = (future_close / close) - 1.
    Last horizon_bars rows have NaN targets.
    """
    if isinstance(bars, pd.DataFrame):
        df = bars.sort_values("candle_start").reset_index(drop=True).copy()
        if "candle_start" in df.columns:
            df["candle_start"] = df["candle_start"].map(to_ist)
    else:
        df = bars_to_dataframe(bars)

    df["future_close"] = df["close"].shift(-horizon_bars)
    df["future_return"] = (df["future_close"] / df["close"]) - 1.0
    # NumPy 2: cannot promote float NaN with str in a single np.where — set then mask.
    known = df["future_close"].notna()
    df["target_direction"] = pd.Series(pd.NA, index=df.index, dtype="object")
    df.loc[known, "target_direction"] = np.where(
        df.loc[known, "future_close"].to_numpy() > df.loc[known, "close"].to_numpy(),
        "UP",
        "DOWN",
    )
    df["target_up"] = np.where(
        df["future_close"].isna(),
        np.nan,
        (df["future_close"] > df["close"]).astype(float),
    )
    return df

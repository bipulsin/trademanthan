"""Target generation — same-session +30m only (3 × 10m bars). No overnight targets."""

from __future__ import annotations

from datetime import time
from typing import Any, Dict, List, Optional, Sequence, Set

import numpy as np
import pandas as pd

from backend.services.sambhav.candles import to_ist
from backend.services.sambhav.config import HORIZON_BARS, TF_MINUTES
from backend.services.sambhav.features import bars_to_dataframe

# Final three 10m opens of a regular session — no complete +30m same-session horizon.
TARGET_EXCLUDE_TIMES: frozenset[str] = frozenset({"15:05", "15:15", "15:25"})


def attach_targets(
    bars: Sequence[Dict[str, Any]] | pd.DataFrame,
    *,
    horizon_bars: int = HORIZON_BARS,
) -> pd.DataFrame:
    """
    Legacy helper (may cross session boundary via shift). Prefer
    ``attach_same_session_targets`` for Sambhav V1 / Phase 2A.
    """
    if isinstance(bars, pd.DataFrame):
        df = bars.sort_values("candle_start").reset_index(drop=True).copy()
        if "candle_start" in df.columns:
            df["candle_start"] = df["candle_start"].map(to_ist)
    else:
        df = bars_to_dataframe(bars)

    df["future_close"] = df["close"].shift(-horizon_bars)
    df["future_return"] = (df["future_close"] / df["close"]) - 1.0
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


def attach_same_session_targets(
    bars: Sequence[Dict[str, Any]] | pd.DataFrame,
    *,
    horizon_bars: int = HORIZON_BARS,
    exclude_times: Optional[Set[str]] = None,
) -> pd.DataFrame:
    """
    Same-session +30m target only.

    - Prediction timestamp = close of candle T (candle_start + TF_MINUTES)
    - future_close = close[T + horizon_bars] within the same session_date
    - Bars at 15:05 / 15:15 / 15:25 are excluded (no complete same-session horizon)
    - Never uses the next trading day
    """
    excl = exclude_times if exclude_times is not None else set(TARGET_EXCLUDE_TIMES)
    if isinstance(bars, pd.DataFrame):
        df = bars.sort_values("candle_start").reset_index(drop=True).copy()
        if "candle_start" in df.columns:
            df["candle_start"] = df["candle_start"].map(to_ist)
    else:
        df = bars_to_dataframe(bars)

    if df.empty:
        for col in (
            "session_date",
            "candle_hm",
            "predict_at",
            "target_timestamp",
            "future_close",
            "future_return",
            "target_direction",
            "target_resolvable",
        ):
            df[col] = []
        return df

    df["session_date"] = df["candle_start"].map(lambda ts: ts.date() if ts is not None else None)
    df["candle_hm"] = df["candle_start"].map(lambda ts: ts.strftime("%H:%M") if ts is not None else "")
    df["predict_at"] = df["candle_start"].map(
        lambda ts: ts + pd.Timedelta(minutes=TF_MINUTES) if ts is not None else None
    )

    future_close = np.full(len(df), np.nan, dtype=float)
    target_ts: List[Any] = [None] * len(df)
    resolvable = np.zeros(len(df), dtype=bool)

    for _, idx in df.groupby("session_date", sort=False).groups.items():
        ix = list(idx)
        closes = df.loc[ix, "close"].to_numpy(dtype=float)
        starts = df.loc[ix, "candle_start"].tolist()
        hms = df.loc[ix, "candle_hm"].tolist()
        for j, row_i in enumerate(ix):
            if hms[j] in excl:
                continue
            k = j + horizon_bars
            if k >= len(ix):
                continue
            # Same-session enforced by groupby; still assert date match.
            if starts[k] is None or starts[j] is None:
                continue
            if starts[k].date() != starts[j].date():
                continue
            future_close[row_i] = closes[k]
            target_ts[row_i] = starts[k] + pd.Timedelta(minutes=TF_MINUTES)
            resolvable[row_i] = True

    df["future_close"] = future_close
    df["target_timestamp"] = target_ts
    df["future_return"] = (df["future_close"] / df["close"]) - 1.0
    df["target_resolvable"] = resolvable
    df["target_direction"] = pd.Series(pd.NA, index=df.index, dtype="object")
    known = df["target_resolvable"]
    df.loc[known, "target_direction"] = np.where(
        df.loc[known, "future_close"].to_numpy() > df.loc[known, "close"].to_numpy(),
        "UP",
        "DOWN",
    )
    return df


def ternary_label(future_return: float, threshold: float) -> str:
    if future_return > threshold:
        return "UP"
    if future_return < -threshold:
        return "DOWN"
    return "NEUTRAL"

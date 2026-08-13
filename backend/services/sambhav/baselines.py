"""Simple chronological baselines — no ML, for comparison only."""

from __future__ import annotations

from typing import Any, Dict, Sequence

import numpy as np
import pandas as pd

from backend.services.sambhav.config import HORIZON_BARS
from backend.services.sambhav.features import bars_to_dataframe, _ema
from backend.services.sambhav.targets import attach_targets


def baseline_ema21_direction(bars: Sequence[Dict[str, Any]] | pd.DataFrame) -> pd.DataFrame:
    """UP if close > EMA21 else DOWN. Probabilities are hard 1/0 (uncalibrated)."""
    if isinstance(bars, pd.DataFrame):
        df = bars.sort_values("candle_start").reset_index(drop=True).copy()
    else:
        df = bars_to_dataframe(bars)
    ema21 = _ema(df["close"], 21)
    pred_up = (df["close"] > ema21).astype(float)
    out = df[["candle_start", "close"]].copy()
    out["baseline"] = "ema21"
    out["pred_direction"] = np.where(pred_up == 1.0, "UP", "DOWN")
    out["p_up"] = pred_up
    out["p_down"] = 1.0 - pred_up
    return out


def baseline_prior_30m_direction(
    bars: Sequence[Dict[str, Any]] | pd.DataFrame,
    *,
    horizon_bars: int = HORIZON_BARS,
) -> pd.DataFrame:
    """Direction of prior 30m return (close vs close[t-3])."""
    if isinstance(bars, pd.DataFrame):
        df = bars.sort_values("candle_start").reset_index(drop=True).copy()
    else:
        df = bars_to_dataframe(bars)
    prior = df["close"].pct_change(horizon_bars)
    pred_up = (prior > 0).astype(float)
    out = df[["candle_start", "close"]].copy()
    out["baseline"] = "prior_30m"
    out["pred_direction"] = np.where(pred_up == 1.0, "UP", "DOWN")
    out["p_up"] = pred_up
    out["p_down"] = 1.0 - pred_up
    # First horizon bars undefined
    out.loc[prior.isna(), ["p_up", "p_down", "pred_direction"]] = np.nan
    return out


def baseline_majority_direction(
    bars: Sequence[Dict[str, Any]] | pd.DataFrame,
    *,
    train_mask: np.ndarray | None = None,
) -> pd.DataFrame:
    """
    Predict the majority historical target direction (from labeled rows).
    If train_mask provided, majority is computed only on that subset (walk-forward safe).
    """
    labeled = attach_targets(bars)
    y = labeled["target_up"]
    if train_mask is not None:
        subset = y[train_mask]
    else:
        subset = y.dropna()
    if len(subset) == 0 or subset.isna().all():
        maj = 1.0
    else:
        maj = float((subset.dropna() >= 0.5).mean() >= 0.5)
    out = labeled[["candle_start", "close"]].copy()
    out["baseline"] = "majority"
    out["p_up"] = maj
    out["p_down"] = 1.0 - maj
    out["pred_direction"] = "UP" if maj >= 0.5 else "DOWN"
    return out


def evaluate_baseline_accuracy(
    pred: pd.DataFrame,
    labeled: pd.DataFrame,
) -> Dict[str, Any]:
    merged = pred.merge(
        labeled[["candle_start", "target_direction", "target_up"]],
        on="candle_start",
        how="inner",
    )
    merged = merged.dropna(subset=["pred_direction", "target_direction"])
    n = len(merged)
    if n < 30:
        return {
            "n": n,
            "accuracy": None,
            "status": "INSUFFICIENT DATA",
        }
    acc = float((merged["pred_direction"] == merged["target_direction"]).mean())
    return {"n": n, "accuracy": round(acc, 4), "status": "OK"}

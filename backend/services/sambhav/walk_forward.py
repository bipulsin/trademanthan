"""Walk-forward chronological validation — never random split."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence

import numpy as np
import pandas as pd

from backend.services.sambhav.baselines import (
    baseline_ema21_direction,
    baseline_majority_direction,
    baseline_prior_30m_direction,
    evaluate_baseline_accuracy,
)
from backend.services.sambhav.calibration import ProbabilityCalibrator
from backend.services.sambhav.features import compute_features, feature_matrix, bars_to_dataframe
from backend.services.sambhav.metrics import full_eval_report, regression_metrics
from backend.services.sambhav.models import DirectionClassifier, make_xgb_classifier, make_xgb_regressor
from backend.services.sambhav.targets import attach_targets


@dataclass
class WalkForwardConfig:
    train_bars: int = 1500
    test_bars: int = 300
    step_bars: int = 300
    min_train: int = 400
    calibration: str = "isotonic"  # platt | isotonic | none
    model: str = "xgboost"


def _build_dataset(bars: Sequence[Dict[str, Any]] | pd.DataFrame) -> pd.DataFrame:
    if isinstance(bars, pd.DataFrame):
        price = bars.sort_values("candle_start").reset_index(drop=True).copy()
    else:
        price = bars_to_dataframe(bars)
    labeled = attach_targets(price)
    feats = compute_features(price)
    merged = labeled.merge(feats, on="candle_start", how="inner")
    return merged.dropna(subset=["target_up"]).reset_index(drop=True)


def walk_forward_splits(
    n: int,
    *,
    train_bars: int,
    test_bars: int,
    step_bars: int,
    min_train: int,
) -> List[tuple[np.ndarray, np.ndarray]]:
    """Expanding or sliding: use last train_bars before each test window."""
    splits = []
    start_test = max(min_train, train_bars)
    while start_test + test_bars <= n:
        train_start = max(0, start_test - train_bars)
        train_idx = np.arange(train_start, start_test)
        test_idx = np.arange(start_test, start_test + test_bars)
        if len(train_idx) >= min_train and len(test_idx) > 0:
            splits.append((train_idx, test_idx))
        start_test += step_bars
    return splits


def run_walk_forward(
    bars: Sequence[Dict[str, Any]] | pd.DataFrame,
    cfg: Optional[WalkForwardConfig] = None,
) -> Dict[str, Any]:
    cfg = cfg or WalkForwardConfig()
    data = _build_dataset(bars)
    from backend.services.sambhav.config import FEATURE_NAMES

    cols = list(FEATURE_NAMES)
    data = data.dropna(subset=cols + ["target_up"]).reset_index(drop=True)
    n = len(data)
    if n < cfg.min_train + 50:
        return {
            "status": "INSUFFICIENT DATA",
            "n": n,
            "message": f"need >= {cfg.min_train + 50} labeled feature rows",
        }

    X = data[cols].to_numpy(dtype=float)
    y = data["target_up"].to_numpy(dtype=float)
    y_ret = data["future_return"].to_numpy(dtype=float)

    splits = walk_forward_splits(
        n,
        train_bars=cfg.train_bars,
        test_bars=cfg.test_bars,
        step_bars=cfg.step_bars,
        min_train=cfg.min_train,
    )
    if not splits:
        return {"status": "INSUFFICIENT DATA", "n": n, "message": "no walk-forward windows"}

    all_y: List[float] = []
    all_praw: List[float] = []
    all_pcal: List[float] = []
    all_ret_true: List[float] = []
    all_ret_pred: List[float] = []
    fold_summaries: List[Dict[str, Any]] = []

    for fi, (tr, te) in enumerate(splits):
        if cfg.model == "logistic":
            from backend.services.sambhav.models import make_logistic_classifier

            clf = make_logistic_classifier()
        else:
            clf = make_xgb_classifier()
        clf.fit(X[tr], y[tr])
        p_raw = clf.predict_proba_up(X[te])

        cal = ProbabilityCalibrator(method=cfg.calibration)  # type: ignore[arg-type]
        # Calibrate on end of train (last 20%) to avoid using test
        cal_n = max(50, len(tr) // 5)
        cal_idx = tr[-cal_n:]
        p_cal_train = clf.predict_proba_up(X[cal_idx])
        cal.fit(p_cal_train, y[cal_idx])
        p_cal = cal.transform(p_raw)

        reg = make_xgb_regressor()
        reg.fit(X[tr], y_ret[tr])
        ret_hat = reg.predict(X[te])

        fold_report = full_eval_report(y[te], p_raw, p_cal, label=f"fold_{fi}")
        fold_summaries.append(
            {
                "fold": fi,
                "train_n": int(len(tr)),
                "test_n": int(len(te)),
                "train_start": str(data.loc[tr[0], "candle_start"]),
                "test_start": str(data.loc[te[0], "candle_start"]),
                "test_end": str(data.loc[te[-1], "candle_start"]),
                "report": fold_report,
                "regression": regression_metrics(y_ret[te], ret_hat, label=f"fold_{fi}_reg"),
            }
        )
        all_y.extend(y[te].tolist())
        all_praw.extend(p_raw.tolist())
        all_pcal.extend(p_cal.tolist())
        all_ret_true.extend(y_ret[te].tolist())
        all_ret_pred.extend(ret_hat.tolist())

    overall = full_eval_report(
        np.asarray(all_y),
        np.asarray(all_praw),
        np.asarray(all_pcal),
        label="walk_forward",
    )
    overall_reg = regression_metrics(
        np.asarray(all_ret_true), np.asarray(all_ret_pred), label="walk_forward_reg"
    )

    # Baselines on full labeled set (for reference; not walk-forward purged)
    price_df = data[["candle_start", "open", "high", "low", "close", "volume"]].copy()
    labeled = attach_targets(price_df)
    b1 = baseline_ema21_direction(price_df)
    b2 = baseline_prior_30m_direction(price_df)
    b3 = baseline_majority_direction(price_df)
    baselines = {
        "ema21": evaluate_baseline_accuracy(b1, labeled),
        "prior_30m": evaluate_baseline_accuracy(b2, labeled),
        "majority": evaluate_baseline_accuracy(b3, labeled),
    }

    return {
        "status": overall.get("verdict", "MODEL NOT VALIDATED"),
        "n_rows": n,
        "n_folds": len(splits),
        "config": {
            "train_bars": cfg.train_bars,
            "test_bars": cfg.test_bars,
            "step_bars": cfg.step_bars,
            "calibration": cfg.calibration,
            "model": cfg.model,
        },
        "overall": overall,
        "overall_regression": overall_reg,
        "folds": fold_summaries,
        "baselines": baselines,
        "model_lifecycle": "RESEARCH",
        "disclaimer": (
            "Walk-forward metrics are research estimates only. "
            "Model status remains RESEARCH until explicit admin validation. "
            "Raw probabilities are not claimed accurate; use calibrated + buckets."
        ),
    }

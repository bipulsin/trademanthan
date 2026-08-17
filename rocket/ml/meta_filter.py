"""Walk-forward ML meta-model for Rocket trade conviction scoring."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, List, Optional, Sequence

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import log_loss, precision_score, roc_auc_score

from rocket.ml.feature_extractor import FEATURE_COLUMNS

logger = logging.getLogger(__name__)


@dataclass
class MetaModelConfig:
    max_iter: int = 150
    learning_rate: float = 0.05
    max_depth: int = 4
    min_samples_leaf: int = 20
    l2_regularization: float = 2.0
    scoring_threshold: float = 0.12
    min_train_samples: int = 40
    model_path: str = ".cache/rocket_meta_filter.joblib"
    # Continuous-bulk band used by the selector (documented here for OOF audits)
    continuous_prob_min: float = 0.12
    continuous_prob_max: float = 0.85


def in_continuous_prob_bulk(p: float, *, lo: float = 0.12, hi: float = 0.85) -> bool:
    """Exclude bottom noise and the ≥0.85 / 0.95 calibration artifact spike."""
    x = float(p)
    return lo <= x <= hi


class RocketMetaFilter:
    """Binary win-probability model with expanding-window (walk-forward) scoring."""

    def __init__(self, config: Optional[MetaModelConfig] = None):
        self.config = config or MetaModelConfig()
        self.model: Any = None
        self.feature_columns: List[str] = list(FEATURE_COLUMNS)
        self.train_metrics: Dict[str, Any] = {}

    def build_labels(
        self,
        trades_df: pd.DataFrame,
        *,
        min_reward_risk: float = 1.5,
    ) -> pd.DataFrame:
        """
        Label raw path outcomes.

        Expected columns: ``pnl_r`` (R-multiples) and/or ``hit_target_first`` /
        ``entry_price``, ``stop_distance``, ``pnl``.
        """
        trades = trades_df.copy()
        if "pnl_r" in trades.columns:
            trades["target_met"] = (trades["pnl_r"] >= float(min_reward_risk)).astype(int)
        elif "hit_target_first" in trades.columns:
            trades["target_met"] = trades["hit_target_first"].astype(int)
        else:
            # Fallback: approx 1.5R using 0.2% of entry as 1R proxy
            r_proxy = trades["entry_price"].astype(float) * 0.002 * float(min_reward_risk)
            trades["target_met"] = (trades["pnl"].astype(float) >= r_proxy).astype(int)
        return trades

    def _fit_estimator(self, X: pd.DataFrame, y: pd.Series) -> Any:
        base = HistGradientBoostingClassifier(
            max_iter=self.config.max_iter,
            learning_rate=self.config.learning_rate,
            max_depth=self.config.max_depth,
            min_samples_leaf=self.config.min_samples_leaf,
            l2_regularization=self.config.l2_regularization,
            random_state=42,
        )
        # Uncalibrated leaf frequencies. Sigmoid Platt collapses expansion-only
        # labels (~10% pos) to ~base-rate scores, so P≥0.36 never fires.
        base.fit(X, y)
        return base

    def train(self, X: pd.DataFrame, y: pd.Series) -> Dict[str, Any]:
        if X.empty or y.empty:
            raise ValueError("Cannot train meta-filter on empty dataset")
        Xf = X[self.feature_columns].astype(float).fillna(0.0)
        y = y.astype(int)
        if y.nunique() < 2:
            # Degenerate: constant heuristic
            self.model = _ConstantProba(float(y.iloc[0]))
            metrics = {
                "roc_auc": 0.5,
                "log_loss": float("nan"),
                "precision_at_threshold": float(y.mean()),
                "samples_filtered": int(len(y)),
                "total_samples": int(len(y)),
                "note": "single_class",
            }
            self.train_metrics = metrics
            return metrics

        self.model = self._fit_estimator(Xf, y)
        probs = self.predict_probability(Xf.to_dict("records"))
        preds = (probs >= self.config.scoring_threshold).astype(int)
        metrics = {
            "roc_auc": float(roc_auc_score(y, probs)) if y.nunique() > 1 else 0.5,
            "log_loss": float(log_loss(y, np.clip(probs, 1e-6, 1 - 1e-6))),
            "precision_at_threshold": float(precision_score(y, preds, zero_division=0)),
            "samples_filtered": int((probs >= self.config.scoring_threshold).sum()),
            "total_samples": int(len(y)),
        }
        self.train_metrics = metrics
        os.makedirs(os.path.dirname(self.config.model_path) or ".", exist_ok=True)
        joblib.dump({"model": self.model, "features": self.feature_columns}, self.config.model_path)
        return metrics

    def load(self, path: Optional[str] = None) -> None:
        path = path or self.config.model_path
        blob = joblib.load(path)
        if isinstance(blob, dict):
            self.model = blob["model"]
            self.feature_columns = list(blob.get("features") or FEATURE_COLUMNS)
        else:
            self.model = blob

    def predict_probability(self, feature_dicts: Sequence[Dict[str, float]]) -> np.ndarray:
        if self.model is None:
            if os.path.exists(self.config.model_path):
                self.load()
            else:
                raise FileNotFoundError("Trained meta-model file not found.")
        if not feature_dicts:
            return np.asarray([], dtype=float)
        df = pd.DataFrame(list(feature_dicts))
        for col in self.feature_columns:
            if col not in df.columns:
                df[col] = 0.0
        X = df[self.feature_columns].astype(float).fillna(0.0)
        if hasattr(self.model, "predict_proba"):
            return np.asarray(self.model.predict_proba(X)[:, 1], dtype=float)
        # Constant / custom
        return np.asarray(self.model.predict_proba(X)[:, 1], dtype=float)

    def score_walk_forward(
        self,
        signals_df: pd.DataFrame,
        *,
        date_col: str = "trade_date",
        label_col: str = "target_met",
    ) -> pd.DataFrame:
        """
        Expanding-window OOF scoring by calendar day.

        For day D, train on rows with ``trade_date < D`` (and a resolved label),
        then score day D. Days with insufficient history fall back to
        ``strategy_confidence`` (clipped) so selection still works.
        """
        if signals_df.empty:
            return signals_df.copy()

        df = signals_df.copy()
        df[date_col] = pd.to_datetime(df[date_col]).dt.date
        df = df.sort_values([date_col, "timestamp"]).reset_index(drop=True)
        probs = np.full(len(df), np.nan, dtype=float)
        fold_stats: List[Dict[str, Any]] = []

        dates = sorted(df[date_col].unique())
        for d in dates:
            test_mask = df[date_col] == d
            train_mask = (df[date_col] < d) & df[label_col].notna()
            train = df.loc[train_mask]
            test_idx = df.index[test_mask]

            if len(train) < self.config.min_train_samples or train[label_col].nunique() < 2:
                # Cold start: use strategy confidence as proxy probability
                conf = df.loc[test_idx, "strategy_confidence"].astype(float).clip(0.0, 1.0)
                probs[test_idx] = conf.values
                fold_stats.append(
                    {
                        "date": str(d),
                        "train_n": int(len(train)),
                        "test_n": int(test_mask.sum()),
                        "mode": "confidence_fallback",
                    }
                )
                continue

            X_train = train[self.feature_columns]
            y_train = train[label_col].astype(int)
            model = self._fit_estimator(X_train.astype(float).fillna(0.0), y_train)
            X_test = df.loc[test_idx, self.feature_columns].astype(float).fillna(0.0)
            if hasattr(model, "predict_proba"):
                probs[test_idx] = model.predict_proba(X_test)[:, 1]
            else:
                probs[test_idx] = 0.5
            fold_stats.append(
                {
                    "date": str(d),
                    "train_n": int(len(train)),
                    "test_n": int(test_mask.sum()),
                    "mode": "walk_forward",
                    "train_pos_rate": float(y_train.mean()),
                }
            )
            logger.info(
                "meta WF %s train=%s test=%s pos_rate=%.2f",
                d,
                len(train),
                int(test_mask.sum()),
                float(y_train.mean()),
            )

        out = df.copy()
        out["win_probability"] = probs
        # Audit flags — selection still happens in DailyTradeRanker
        p = out["win_probability"].astype(float)
        if len(p):
            logger.info(
                "OOF P p50=%.3f p90=%.3f p99=%.3f frac>=0.12=%.4f frac>=0.36=%.4f",
                float(p.median()),
                float(p.quantile(0.90)),
                float(p.quantile(0.99)),
                float((p >= 0.12).mean()),
                float((p >= 0.36).mean()),
            )
        lo = float(self.config.continuous_prob_min)
        hi = float(self.config.continuous_prob_max)
        out["in_continuous_bulk"] = (p >= lo) & (p <= hi)
        out["is_artifact_spike"] = p > hi
        self.train_metrics = {
            "walk_forward_folds": fold_stats,
            "prob_bulk_frac": float(out["in_continuous_bulk"].mean()) if len(out) else 0.0,
            "prob_artifact_frac": float(out["is_artifact_spike"].mean()) if len(out) else 0.0,
            "continuous_prob_min": lo,
            "continuous_prob_max": hi,
        }
        # Fit final model on all labeled rows for persistence / later days
        labeled = out[out[label_col].notna()]
        if len(labeled) >= self.config.min_train_samples and labeled[label_col].nunique() > 1:
            try:
                self.train(labeled[self.feature_columns], labeled[label_col].astype(int))
            except Exception as exc:
                logger.warning("final meta-fit skipped: %s", exc)
        return out


class _ConstantProba:
    def __init__(self, p: float):
        self.p = float(np.clip(p, 0.0, 1.0))

    def predict_proba(self, X: Any) -> np.ndarray:
        n = len(X)
        return np.column_stack([np.full(n, 1.0 - self.p), np.full(n, self.p)])

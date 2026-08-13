"""Model abstractions for Sambhav classifiers / regressors."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

import numpy as np


class DirectionClassifier(ABC):
    """Outputs P(UP). P(DOWN) = 1 - P(UP)."""

    name: str = "base"

    @abstractmethod
    def fit(self, X: np.ndarray, y: np.ndarray) -> "DirectionClassifier":
        ...

    @abstractmethod
    def predict_proba_up(self, X: np.ndarray) -> np.ndarray:
        ...

    def predict_direction(self, X: np.ndarray) -> np.ndarray:
        p = self.predict_proba_up(X)
        return np.where(p >= 0.5, "UP", "DOWN")

    def get_params(self) -> Dict[str, Any]:
        return {"name": self.name}


class ReturnRegressor(ABC):
    name: str = "base_reg"

    @abstractmethod
    def fit(self, X: np.ndarray, y: np.ndarray) -> "ReturnRegressor":
        ...

    @abstractmethod
    def predict(self, X: np.ndarray) -> np.ndarray:
        ...


class SklearnClassifierAdapter(DirectionClassifier):
    """Wrap any sklearn-like classifier with predict_proba."""

    def __init__(self, estimator: Any, name: str = "sklearn"):
        self.estimator = estimator
        self.name = name

    def fit(self, X: np.ndarray, y: np.ndarray) -> "SklearnClassifierAdapter":
        self.estimator.fit(X, y.astype(int))
        return self

    def predict_proba_up(self, X: np.ndarray) -> np.ndarray:
        proba = self.estimator.predict_proba(X)
        # Assume classes_ includes 1 for UP
        classes = list(getattr(self.estimator, "classes_", [0, 1]))
        if 1 in classes:
            idx = classes.index(1)
            return proba[:, idx]
        return proba[:, -1]


class SklearnRegressorAdapter(ReturnRegressor):
    def __init__(self, estimator: Any, name: str = "sklearn_reg"):
        self.estimator = estimator
        self.name = name

    def fit(self, X: np.ndarray, y: np.ndarray) -> "SklearnRegressorAdapter":
        self.estimator.fit(X, y)
        return self

    def predict(self, X: np.ndarray) -> np.ndarray:
        return np.asarray(self.estimator.predict(X), dtype=float)


def make_xgb_classifier(**kwargs: Any) -> SklearnClassifierAdapter:
    from xgboost import XGBClassifier

    defaults = dict(
        n_estimators=120,
        max_depth=4,
        learning_rate=0.05,
        subsample=0.9,
        colsample_bytree=0.9,
        objective="binary:logistic",
        eval_metric="logloss",
        n_jobs=2,
        random_state=42,
    )
    defaults.update(kwargs)
    return SklearnClassifierAdapter(XGBClassifier(**defaults), name="xgboost_clf")


def make_xgb_regressor(**kwargs: Any) -> SklearnRegressorAdapter:
    from xgboost import XGBRegressor

    defaults = dict(
        n_estimators=120,
        max_depth=4,
        learning_rate=0.05,
        subsample=0.9,
        colsample_bytree=0.9,
        objective="reg:squarederror",
        n_jobs=2,
        random_state=42,
    )
    defaults.update(kwargs)
    return SklearnRegressorAdapter(XGBRegressor(**defaults), name="xgboost_reg")


def make_logistic_classifier(**kwargs: Any) -> SklearnClassifierAdapter:
    from sklearn.linear_model import LogisticRegression
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import StandardScaler

    defaults = dict(max_iter=500, random_state=42)
    defaults.update(kwargs)
    pipe = Pipeline(
        [
            ("scaler", StandardScaler()),
            ("clf", LogisticRegression(**defaults)),
        ]
    )
    return SklearnClassifierAdapter(pipe, name="logistic")

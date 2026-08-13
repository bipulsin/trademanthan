"""Probability calibration — keep raw + calibrated. Never treat raw as accurate."""

from __future__ import annotations

from typing import Any, Dict, Literal, Optional, Tuple

import numpy as np

CalibrationMethod = Literal["platt", "isotonic", "none"]


class ProbabilityCalibrator:
    def __init__(self, method: CalibrationMethod = "isotonic"):
        self.method = method
        self._impl: Any = None
        self.fitted = False

    def fit(self, p_raw: np.ndarray, y: np.ndarray) -> "ProbabilityCalibrator":
        p = np.asarray(p_raw, dtype=float).reshape(-1)
        yy = np.asarray(y, dtype=int).reshape(-1)
        mask = np.isfinite(p) & np.isfinite(yy)
        p, yy = p[mask], yy[mask]
        if len(p) < 50 or self.method == "none":
            self.fitted = False
            self._impl = None
            return self
        try:
            from sklearn.calibration import CalibratedClassifierCV
            from sklearn.isotonic import IsotonicRegression
            from sklearn.linear_model import LogisticRegression
        except ImportError as exc:
            raise RuntimeError("scikit-learn required for calibration") from exc

        if self.method == "isotonic":
            iso = IsotonicRegression(out_of_bounds="clip")
            iso.fit(p, yy)
            self._impl = ("isotonic", iso)
        else:
            # Platt: logistic on logit of raw prob
            eps = 1e-6
            p_clip = np.clip(p, eps, 1 - eps)
            logit = np.log(p_clip / (1 - p_clip)).reshape(-1, 1)
            lr = LogisticRegression(max_iter=500)
            lr.fit(logit, yy)
            self._impl = ("platt", lr)
        self.fitted = True
        return self

    def transform(self, p_raw: np.ndarray) -> np.ndarray:
        p = np.asarray(p_raw, dtype=float).reshape(-1)
        if not self.fitted or self._impl is None:
            return p.copy()
        kind, model = self._impl
        if kind == "isotonic":
            return np.asarray(model.predict(p), dtype=float)
        eps = 1e-6
        p_clip = np.clip(p, eps, 1 - eps)
        logit = np.log(p_clip / (1 - p_clip)).reshape(-1, 1)
        return np.asarray(model.predict_proba(logit)[:, 1], dtype=float)


def calibration_buckets(
    p: np.ndarray,
    y: np.ndarray,
    *,
    n_bins: int = 10,
) -> Dict[str, Any]:
    """Reliability-style buckets: predicted prob vs empirical frequency."""
    p = np.asarray(p, dtype=float).reshape(-1)
    y = np.asarray(y, dtype=float).reshape(-1)
    mask = np.isfinite(p) & np.isfinite(y)
    p, y = p[mask], y[mask]
    if len(p) < 30:
        return {"status": "INSUFFICIENT DATA", "n": int(len(p)), "buckets": []}

    edges = np.linspace(0.0, 1.0, n_bins + 1)
    buckets = []
    ece = 0.0
    for i in range(n_bins):
        lo, hi = edges[i], edges[i + 1]
        if i == n_bins - 1:
            m = (p >= lo) & (p <= hi)
        else:
            m = (p >= lo) & (p < hi)
        n = int(m.sum())
        if n == 0:
            buckets.append(
                {
                    "bin": i,
                    "lo": float(lo),
                    "hi": float(hi),
                    "n": 0,
                    "mean_pred": None,
                    "frac_up": None,
                }
            )
            continue
        mean_pred = float(p[m].mean())
        frac_up = float(y[m].mean())
        ece += (n / len(p)) * abs(mean_pred - frac_up)
        buckets.append(
            {
                "bin": i,
                "lo": float(lo),
                "hi": float(hi),
                "n": n,
                "mean_pred": round(mean_pred, 4),
                "frac_up": round(frac_up, 4),
            }
        )
    status = "OK"
    if ece > 0.15:
        status = "CALIBRATION POOR"
    return {
        "status": status,
        "n": int(len(p)),
        "ece": round(float(ece), 4),
        "buckets": buckets,
    }

"""Evaluation metrics — never fabricate; return INSUFFICIENT DATA when n too small."""

from __future__ import annotations

from typing import Any, Dict, Optional

import numpy as np

from backend.services.sambhav.calibration import calibration_buckets


MIN_SAMPLES = 50


def _safe_auc(y: np.ndarray, p: np.ndarray) -> Optional[float]:
    try:
        from sklearn.metrics import roc_auc_score

        if len(np.unique(y)) < 2:
            return None
        return float(roc_auc_score(y, p))
    except Exception:
        return None


def _brier(y: np.ndarray, p: np.ndarray) -> float:
    return float(np.mean((p - y) ** 2))


def _logloss(y: np.ndarray, p: np.ndarray) -> float:
    eps = 1e-6
    p = np.clip(p, eps, 1 - eps)
    return float(-np.mean(y * np.log(p) + (1 - y) * np.log(1 - p)))


def classification_metrics(
    y_true: np.ndarray,
    p_up: np.ndarray,
    *,
    label: str = "eval",
) -> Dict[str, Any]:
    y = np.asarray(y_true, dtype=float).reshape(-1)
    p = np.asarray(p_up, dtype=float).reshape(-1)
    mask = np.isfinite(y) & np.isfinite(p)
    y, p = y[mask], p[mask]
    n = int(len(y))
    if n < MIN_SAMPLES:
        return {
            "label": label,
            "n": n,
            "status": "INSUFFICIENT DATA",
            "accuracy": None,
            "auc": None,
            "brier": None,
            "logloss": None,
        }
    pred = (p >= 0.5).astype(float)
    acc = float((pred == y).mean())
    auc = _safe_auc(y, p)
    return {
        "label": label,
        "n": n,
        "status": "OK",
        "accuracy": round(acc, 4),
        "auc": round(auc, 4) if auc is not None else None,
        "brier": round(_brier(y, p), 4),
        "logloss": round(_logloss(y, p), 4),
        "base_rate_up": round(float(y.mean()), 4),
    }


def regression_metrics(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    *,
    label: str = "eval",
) -> Dict[str, Any]:
    y = np.asarray(y_true, dtype=float).reshape(-1)
    yp = np.asarray(y_pred, dtype=float).reshape(-1)
    mask = np.isfinite(y) & np.isfinite(yp)
    y, yp = y[mask], yp[mask]
    n = int(len(y))
    if n < MIN_SAMPLES:
        return {"label": label, "n": n, "status": "INSUFFICIENT DATA", "mae": None, "rmse": None}
    err = yp - y
    mae = float(np.mean(np.abs(err)))
    rmse = float(np.sqrt(np.mean(err ** 2)))
    # Directional agreement of sign
    dir_acc = float((np.sign(yp) == np.sign(y)).mean()) if n else None
    return {
        "label": label,
        "n": n,
        "status": "OK",
        "mae": round(mae, 6),
        "rmse": round(rmse, 6),
        "sign_accuracy": round(dir_acc, 4) if dir_acc is not None else None,
    }


def full_eval_report(
    y_true: np.ndarray,
    p_raw: np.ndarray,
    p_cal: Optional[np.ndarray] = None,
    *,
    label: str = "eval",
) -> Dict[str, Any]:
    raw = classification_metrics(y_true, p_raw, label=f"{label}_raw")
    buckets_raw = calibration_buckets(p_raw, y_true)
    out: Dict[str, Any] = {
        "raw": raw,
        "calibration_raw": buckets_raw,
    }
    if p_cal is not None:
        out["calibrated"] = classification_metrics(y_true, p_cal, label=f"{label}_cal")
        out["calibration_calibrated"] = calibration_buckets(p_cal, y_true)
    # Overall honesty flag
    if raw.get("status") == "INSUFFICIENT DATA":
        out["verdict"] = "INSUFFICIENT DATA"
    elif buckets_raw.get("status") == "CALIBRATION POOR" and p_cal is None:
        out["verdict"] = "CALIBRATION POOR"
    elif p_cal is not None and out["calibration_calibrated"].get("status") == "CALIBRATION POOR":
        out["verdict"] = "CALIBRATION POOR"
    else:
        out["verdict"] = "MODEL NOT VALIDATED"  # never auto-claim validated
    return out

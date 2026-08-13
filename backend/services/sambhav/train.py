"""Train + persist Sambhav model artifacts (RESEARCH status by default)."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Sequence

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session

from backend.services.sambhav.calibration import ProbabilityCalibrator
from backend.services.sambhav.candles import load_10m_df_rows
from backend.services.sambhav.config import (
    ARTIFACTS_DIR,
    FEATURE_NAMES,
    INSTRUMENT_KEY,
    IST,
    STATUS_RESEARCH,
)
from backend.services.sambhav.features import bars_to_dataframe, compute_features
from backend.services.sambhav.models import make_logistic_classifier, make_xgb_classifier, make_xgb_regressor
from backend.services.sambhav.tables import ensure_sambhav_tables
from backend.services.sambhav.targets import attach_targets
from backend.services.sambhav.walk_forward import WalkForwardConfig, run_walk_forward

logger = logging.getLogger(__name__)


def _joblib():
    import joblib

    return joblib


def _dataset_from_bars(bars: Sequence[Dict[str, Any]]) -> pd.DataFrame:
    price = bars_to_dataframe(bars)
    labeled = attach_targets(price)
    feats = compute_features(price)
    merged = labeled.merge(feats, on="candle_start", how="inner")
    cols = list(FEATURE_NAMES)
    return merged.dropna(subset=cols + ["target_up", "future_return"]).reset_index(drop=True)


def train_and_save(
    db: Session,
    *,
    instrument_key: str = INSTRUMENT_KEY,
    model_name: str = "sambhav_xgb_v1",
    model_kind: str = "xgboost",
    calibration: str = "isotonic",
    run_validation: bool = True,
) -> Dict[str, Any]:
    ensure_sambhav_tables()
    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)

    bars = load_10m_df_rows(db, instrument_key=instrument_key, complete_only=True)
    if len(bars) < 500:
        return {
            "ok": False,
            "status": "INSUFFICIENT DATA",
            "n_bars": len(bars),
            "message": "Need >= 500 complete 10m bars before training",
        }

    data = _dataset_from_bars(bars)
    if len(data) < 400:
        return {
            "ok": False,
            "status": "INSUFFICIENT DATA",
            "n_rows": len(data),
            "message": "Need >= 400 labeled feature rows",
        }

    cols = list(FEATURE_NAMES)
    X = data[cols].to_numpy(dtype=float)
    y = data["target_up"].to_numpy(dtype=float)
    y_ret = data["future_return"].to_numpy(dtype=float)

    # Hold out last 15% chronologically for calibration fit only after classifier train on earlier
    n = len(data)
    split = int(n * 0.85)
    split = max(split, n - max(100, n // 10))
    X_tr, y_tr = X[:split], y[:split]
    X_cal, y_cal = X[split:], y[split:]

    if model_kind == "logistic":
        clf = make_logistic_classifier()
    else:
        clf = make_xgb_classifier()
    clf.fit(X_tr, y_tr)
    p_cal_in = clf.predict_proba_up(X_cal)
    calibrator = ProbabilityCalibrator(method=calibration)  # type: ignore[arg-type]
    calibrator.fit(p_cal_in, y_cal)

    reg = make_xgb_regressor()
    reg.fit(X_tr, y_ret[:split])

    stamp = datetime.now(IST).strftime("%Y%m%d_%H%M%S")
    artifact_path = ARTIFACTS_DIR / f"{model_name}_{stamp}.joblib"
    payload = {
        "classifier": clf,
        "regressor": reg,
        "calibrator": calibrator,
        "feature_names": cols,
        "model_kind": model_kind,
        "calibration": calibration,
        "trained_at": stamp,
        "train_rows": int(split),
        "cal_rows": int(n - split),
    }
    _joblib().dump(payload, artifact_path)

    wf: Optional[Dict[str, Any]] = None
    if run_validation:
        wf = run_walk_forward(
            bars,
            WalkForwardConfig(calibration=calibration, model=model_kind),  # type: ignore[arg-type]
        )

    row = db.execute(
        text(
            """
            INSERT INTO sambhav_models (
                name, model_type, status, artifact_path, feature_list_json,
                train_start, train_end, metrics_json, calibration_method, notes
            ) VALUES (
                :name, :mtype, :status, :path, CAST(:feats AS jsonb),
                :t0, :t1, CAST(:metrics AS jsonb), :cal, :notes
            )
            RETURNING id
            """
        ),
        {
            "name": model_name,
            "mtype": model_kind,
            "status": STATUS_RESEARCH,
            "path": str(artifact_path),
            "feats": json.dumps(cols),
            "t0": data.loc[0, "candle_start"],
            "t1": data.loc[n - 1, "candle_start"],
            "metrics": json.dumps(wf or {"status": "NOT_RUN"}),
            "cal": calibration,
            "notes": "Auto-created RESEARCH model; never auto-VALIDATED",
        },
    ).fetchone()
    db.commit()
    model_id = int(row[0]) if row else None

    if wf and model_id is not None:
        db.execute(
            text(
                """
                INSERT INTO sambhav_metrics (
                    model_id, eval_type, window_start, window_end, n_samples,
                    metrics_json, calibration_buckets_json
                ) VALUES (
                    :mid, 'walk_forward', :w0, :w1, :n,
                    CAST(:m AS jsonb), CAST(:b AS jsonb)
                )
                """
            ),
            {
                "mid": model_id,
                "w0": data.loc[0, "candle_start"],
                "w1": data.loc[n - 1, "candle_start"],
                "n": wf.get("n_rows"),
                "m": json.dumps(wf.get("overall") or {}),
                "b": json.dumps(
                    (wf.get("overall") or {}).get("calibration_calibrated")
                    or (wf.get("overall") or {}).get("calibration_raw")
                    or {}
                ),
            },
        )
        db.commit()

    return {
        "ok": True,
        "model_id": model_id,
        "status": STATUS_RESEARCH,
        "artifact_path": str(artifact_path),
        "n_bars": len(bars),
        "n_train_rows": int(split),
        "walk_forward": wf,
        "disclaimer": "MODEL NOT VALIDATED — research artifact only",
    }


def load_artifact(path: str | Path) -> Dict[str, Any]:
    return _joblib().load(path)


def get_active_model_row(db: Session, prefer_status: str = "LIVE") -> Optional[Dict[str, Any]]:
    """Prefer LIVE, else latest RESEARCH/VALIDATED. Never invent metrics."""
    ensure_sambhav_tables()
    for st in (prefer_status, "VALIDATED", "RESEARCH"):
        row = db.execute(
            text(
                """
                SELECT id, name, model_type, status, artifact_path, feature_list_json,
                       metrics_json, calibration_method, created_at
                FROM sambhav_models
                WHERE status = :st AND artifact_path IS NOT NULL
                ORDER BY id DESC LIMIT 1
                """
            ),
            {"st": st},
        ).fetchone()
        if row:
            return {
                "id": row.id,
                "name": row.name,
                "model_type": row.model_type,
                "status": row.status,
                "artifact_path": row.artifact_path,
                "feature_list_json": row.feature_list_json,
                "metrics_json": row.metrics_json,
                "calibration_method": row.calibration_method,
                "created_at": row.created_at.isoformat() if row.created_at else None,
            }
    return None

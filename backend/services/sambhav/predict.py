"""Live / batch prediction + resolution (prediction-only; no trades)."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from backend.services.sambhav.candles import (
    build_10m_candles,
    load_10m_df_rows,
    to_ist,
)
from backend.services.sambhav.config import (
    FEATURE_NAMES,
    HORIZON_MINUTES,
    INSTRUMENT_KEY,
    IST,
    PRED_PENDING,
    PRED_RESOLVED,
    TF_MINUTES,
)
from backend.services.sambhav.features import compute_features, bars_to_dataframe
from backend.services.sambhav.importer import _fetch_chunk, _get_upstox, upsert_raw_candles
from backend.services.sambhav.tables import ensure_sambhav_tables
from backend.services.sambhav.train import get_active_model_row, load_artifact

logger = logging.getLogger(__name__)


def _latest_feature_row(bars: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if len(bars) < 40:
        return None
    price = bars_to_dataframe(bars)
    feats = compute_features(price)
    cols = list(FEATURE_NAMES)
    last = feats.dropna(subset=cols).tail(1)
    if last.empty:
        return None
    row = last.iloc[0]
    return {
        "candle_start": row["candle_start"],
        "features": {c: float(row[c]) for c in cols},
        "close": float(price.loc[price["candle_start"] == row["candle_start"], "close"].iloc[-1]),
    }


def predict_latest(
    db: Session,
    *,
    instrument_key: str = INSTRUMENT_KEY,
    source: str = "live",
    model_id: Optional[int] = None,
) -> Dict[str, Any]:
    ensure_sambhav_tables()
    model = None
    if model_id is not None:
        row = db.execute(
            text(
                """
                SELECT id, name, model_type, status, artifact_path, calibration_method, created_at
                FROM sambhav_models WHERE id = :id
                """
            ),
            {"id": model_id},
        ).fetchone()
        if row:
            model = {
                "id": row.id,
                "name": row.name,
                "status": row.status,
                "artifact_path": row.artifact_path,
                "calibration_method": row.calibration_method,
            }
    else:
        model = get_active_model_row(db)

    if not model or not model.get("artifact_path"):
        return {
            "ok": False,
            "status": "MODEL NOT VALIDATED",
            "message": "No trained model artifact available",
        }

    path = Path(model["artifact_path"])
    if not path.exists():
        return {"ok": False, "status": "MODEL NOT VALIDATED", "message": f"artifact missing: {path}"}

    bars = load_10m_df_rows(db, instrument_key=instrument_key, complete_only=True)
    feat_row = _latest_feature_row(bars)
    if not feat_row:
        return {"ok": False, "status": "INSUFFICIENT DATA", "message": "Not enough 10m bars for features"}

    art = load_artifact(path)
    clf = art["classifier"]
    calibrator = art.get("calibrator")
    cols = art.get("feature_names") or list(FEATURE_NAMES)
    import numpy as np

    X = np.asarray([[feat_row["features"][c] for c in cols]], dtype=float)
    p_up_raw = float(clf.predict_proba_up(X)[0])
    if calibrator is not None and getattr(calibrator, "fitted", False):
        p_up_cal = float(calibrator.transform(np.asarray([p_up_raw]))[0])
    else:
        p_up_cal = p_up_raw

    p_down_raw = 1.0 - p_up_raw
    p_down_cal = 1.0 - p_up_cal
    direction = "UP" if p_up_cal >= 0.5 else "DOWN"
    now = datetime.now(IST)
    candle_start = feat_row["candle_start"]

    db.execute(
        text(
            """
            INSERT INTO sambhav_predictions (
                instrument_key, candle_start, predict_at, horizon_minutes, model_id,
                p_up_raw, p_down_raw, p_up_calibrated, p_down_calibrated,
                predicted_direction, features_json, status, source
            ) VALUES (
                :ik, :cs, :pa, :hz, :mid,
                :pur, :pdr, :puc, :pdc,
                :dir, CAST(:feats AS jsonb), :st, :src
            )
            ON CONFLICT (instrument_key, candle_start, model_id, source) DO UPDATE SET
                predict_at = EXCLUDED.predict_at,
                p_up_raw = EXCLUDED.p_up_raw,
                p_down_raw = EXCLUDED.p_down_raw,
                p_up_calibrated = EXCLUDED.p_up_calibrated,
                p_down_calibrated = EXCLUDED.p_down_calibrated,
                predicted_direction = EXCLUDED.predicted_direction,
                features_json = EXCLUDED.features_json
            RETURNING id
            """
        ),
        {
            "ik": instrument_key,
            "cs": candle_start,
            "pa": now,
            "hz": HORIZON_MINUTES,
            "mid": model["id"],
            "pur": p_up_raw,
            "pdr": p_down_raw,
            "puc": p_up_cal,
            "pdc": p_down_cal,
            "dir": direction,
            "feats": json.dumps(feat_row["features"]),
            "st": PRED_PENDING,
            "src": source,
        },
    )
    db.commit()

    return {
        "ok": True,
        "model_id": model["id"],
        "model_status": model.get("status") or "RESEARCH",
        "candle_start": candle_start.isoformat() if hasattr(candle_start, "isoformat") else str(candle_start),
        "close": feat_row["close"],
        "p_up_raw": round(p_up_raw, 4),
        "p_down_raw": round(p_down_raw, 4),
        "p_up_calibrated": round(p_up_cal, 4),
        "p_down_calibrated": round(p_down_cal, 4),
        "predicted_direction": direction,
        "disclaimer": (
            "Raw model probability is NOT claimed accurate. "
            "Prefer calibrated probabilities and calibration buckets. "
            "MODEL NOT VALIDATED for trading."
        ),
        "status": PRED_PENDING,
    }


def resolve_pending_predictions(
    db: Session,
    *,
    instrument_key: str = INSTRUMENT_KEY,
) -> Dict[str, Any]:
    """Resolve PENDING preds when future close (T+30m) is available."""
    ensure_sambhav_tables()
    pending = db.execute(
        text(
            """
            SELECT id, candle_start, horizon_minutes
            FROM sambhav_predictions
            WHERE instrument_key = :ik AND status = :st
            ORDER BY candle_start ASC
            LIMIT 500
            """
        ),
        {"ik": instrument_key, "st": PRED_PENDING},
    ).fetchall()

    resolved = 0
    for row in pending:
        cs = to_ist(row.candle_start)
        if cs is None:
            continue
        # Target close is close of bar starting at cs + 30m (3 bars ahead),
        # i.e. the 10m bar whose start == cs + horizon.
        target_start = cs + timedelta(minutes=int(row.horizon_minutes or HORIZON_MINUTES))
        fut = db.execute(
            text(
                """
                SELECT close FROM sambhav_10m_candles
                WHERE instrument_key = :ik AND candle_start = :cs AND is_complete = TRUE
                """
            ),
            {"ik": instrument_key, "cs": target_start},
        ).fetchone()
        entry = db.execute(
            text(
                """
                SELECT close FROM sambhav_10m_candles
                WHERE instrument_key = :ik AND candle_start = :cs
                """
            ),
            {"ik": instrument_key, "cs": cs},
        ).fetchone()
        if not fut or not entry:
            continue
        future_close = float(fut.close)
        entry_close = float(entry.close)
        fut_ret = (future_close / entry_close) - 1.0 if entry_close else None
        actual = "UP" if future_close > entry_close else "DOWN"
        db.execute(
            text(
                """
                UPDATE sambhav_predictions SET
                    status = :st,
                    future_close = :fc,
                    future_return = :fr,
                    actual_direction = :ad,
                    resolved_at = :ra
                WHERE id = :id
                """
            ),
            {
                "st": PRED_RESOLVED,
                "fc": future_close,
                "fr": fut_ret,
                "ad": actual,
                "ra": datetime.now(IST),
                "id": row.id,
            },
        )
        resolved += 1
    if resolved:
        db.commit()
    return {"resolved": resolved, "checked": len(pending)}


def refresh_recent_1m_and_10m(db: Session, *, days_back: int = 3) -> Dict[str, Any]:
    """Pull recent 1m from Upstox and rebuild 10m (for live scheduler)."""
    ensure_sambhav_tables()
    upstox = _get_upstox()
    to_d = datetime.now(IST).date()
    from_d = to_d - timedelta(days=days_back)
    candles = _fetch_chunk(upstox, INSTRUMENT_KEY, from_d, to_d)
    # Also try intraday for today
    try:
        intra = upstox._fetch_intraday_candles_v3(INSTRUMENT_KEY, "minutes/1")
        if intra:
            candles = (candles or []) + list(intra)
    except Exception:
        logger.debug("sambhav intraday merge skipped", exc_info=True)
    written = upsert_raw_candles(db, candles or [], instrument_key=INSTRUMENT_KEY)
    agg = build_10m_candles(db, instrument_key=INSTRUMENT_KEY, require_complete=True)
    return {"upserted_1m": written, "agg": agg}

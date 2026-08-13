"""TWCTO Sambhav API — NIFTY 10m → 30m ML probability (research)."""

from __future__ import annotations

import logging
import threading
import uuid
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.orm import Session

from backend.database import SessionLocal, get_db
from backend.models.user import User
from backend.routers.auth import get_user_from_token, oauth2_scheme
from backend.services.sambhav.config import INSTRUMENT_KEY, IST, STATUS_RESEARCH, STATUS_VALIDATED, STATUS_LIVE
from backend.services.sambhav.tables import ensure_sambhav_tables

logger = logging.getLogger(__name__)
router = APIRouter(tags=["sambhav"])

_jobs_lock = threading.Lock()
_jobs: Dict[str, Dict[str, Any]] = {}


def _require_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)) -> User:
    return get_user_from_token(token, db)


def _require_admin(user: User = Depends(_require_user)) -> User:
    if (getattr(user, "is_admin", None) or "").strip() != "Yes":
        raise HTTPException(status_code=403, detail="Administrator only")
    return user


class ImportBody(BaseModel):
    from_date: date
    to_date: Optional[date] = None
    rebuild_10m: bool = True


class TrainBody(BaseModel):
    model_name: str = "sambhav_xgb_v1"
    model_kind: str = Field("xgboost", description="xgboost|logistic")
    calibration: str = Field("isotonic", description="isotonic|platt|none")
    run_validation: bool = True


class BacktestBody(BaseModel):
    train_bars: int = Field(1500, ge=200, le=20000)
    test_bars: int = Field(300, ge=50, le=5000)
    step_bars: int = Field(300, ge=50, le=5000)
    calibration: str = "isotonic"
    model: str = "xgboost"


class ModelStatusBody(BaseModel):
    status: str = Field(..., description="RESEARCH|VALIDATED|LIVE")
    notes: Optional[str] = None


def _job_set(job_id: str, **kwargs: Any) -> None:
    with _jobs_lock:
        cur = _jobs.get(job_id, {})
        cur.update(kwargs)
        _jobs[job_id] = cur


@router.get("/status")
def sambhav_status(user: User = Depends(_require_user), db: Session = Depends(get_db)):
    ensure_sambhav_tables()
    from backend.services.sambhav.data_status import compute_data_status
    from backend.services.sambhav.dataset import get_active_dataset_version
    from backend.services.sambhav.importer import get_import_state
    from backend.services.sambhav.train import get_active_model_row

    raw_n = db.execute(
        text("SELECT COUNT(*) FROM sambhav_raw_candles WHERE instrument_key = :ik"),
        {"ik": INSTRUMENT_KEY},
    ).scalar()
    c10 = db.execute(
        text("SELECT COUNT(*) FROM sambhav_10m_candles WHERE instrument_key = :ik"),
        {"ik": INSTRUMENT_KEY},
    ).scalar()
    pred_n = db.execute(
        text("SELECT COUNT(*) FROM sambhav_predictions WHERE instrument_key = :ik"),
        {"ik": INSTRUMENT_KEY},
    ).scalar()
    models = db.execute(text("SELECT COUNT(*) FROM sambhav_models")).scalar()
    dataset = compute_data_status(db, refresh_sessions=False)
    return {
        "module": "TWCTO Sambhav",
        "subtitle": "NIFTY 10-Minute → 30-Minute ML Probability Engine",
        "instrument_key": INSTRUMENT_KEY,
        "raw_1m_count": int(raw_n or 0),
        "complete_10m_count": int(c10 or 0),
        "predictions_count": int(pred_n or 0),
        "models_count": int(models or 0),
        "import_state": get_import_state(db),
        "active_model": get_active_model_row(db),
        "dataset": dataset,
        "active_dataset_version": get_active_dataset_version(db),
        "model_status": "MODEL NOT VALIDATED",
        "disclaimer": (
            "Research probability engine. Not trading advice. "
            "Raw probabilities are not claimed accurate. Never auto-VALIDATED."
        ),
        "now_ist": datetime.now(IST).isoformat(),
        "user": getattr(user, "email", None),
    }


@router.get("/data-status")
def sambhav_data_status(
    refresh: bool = Query(False),
    user: User = Depends(_require_user),
    db: Session = Depends(get_db),
):
    """Sambhav V1 dataset quality — REGULAR sessions only."""
    from backend.services.sambhav.data_status import compute_data_status

    return compute_data_status(db, refresh_sessions=refresh)


@router.get("/phase2a")
def sambhav_phase2a_status(user: User = Depends(_require_user), db: Session = Depends(get_db)):
    """Phase 2A research status — features/targets only; no model training."""
    from backend.services.sambhav.phase2a import get_phase2a_status

    return get_phase2a_status(db)


@router.get("/current")
def sambhav_current(user: User = Depends(_require_user), db: Session = Depends(get_db)):
    ensure_sambhav_tables()
    row = db.execute(
        text(
            """
            SELECT id, candle_start, predict_at, p_up_raw, p_down_raw,
                   p_up_calibrated, p_down_calibrated, predicted_direction,
                   status, model_id, source, actual_direction, future_return
            FROM sambhav_predictions
            WHERE instrument_key = :ik
            ORDER BY predict_at DESC NULLS LAST, id DESC
            LIMIT 1
            """
        ),
        {"ik": INSTRUMENT_KEY},
    ).fetchone()
    if not row:
        return {
            "ok": False,
            "status": "INSUFFICIENT DATA",
            "message": "No predictions yet",
            "disclaimer": "MODEL NOT VALIDATED",
        }
    return {
        "ok": True,
        "id": row.id,
        "candle_start": row.candle_start.isoformat() if row.candle_start else None,
        "predict_at": row.predict_at.isoformat() if row.predict_at else None,
        "p_up_raw": row.p_up_raw,
        "p_down_raw": row.p_down_raw,
        "p_up_calibrated": row.p_up_calibrated,
        "p_down_calibrated": row.p_down_calibrated,
        "predicted_direction": row.predicted_direction,
        "status": row.status,
        "model_id": row.model_id,
        "source": row.source,
        "actual_direction": row.actual_direction,
        "future_return": row.future_return,
        "warning": "Calibrated P(UP)/P(DOWN) are research estimates — not trade signals.",
    }


@router.get("/history")
def sambhav_history(
    limit: int = Query(50, ge=1, le=500),
    user: User = Depends(_require_user),
    db: Session = Depends(get_db),
):
    ensure_sambhav_tables()
    rows = db.execute(
        text(
            """
            SELECT id, candle_start, predict_at, p_up_calibrated, p_down_calibrated,
                   predicted_direction, status, actual_direction, future_return, source, model_id
            FROM sambhav_predictions
            WHERE instrument_key = :ik
            ORDER BY candle_start DESC
            LIMIT :lim
            """
        ),
        {"ik": INSTRUMENT_KEY, "lim": limit},
    ).fetchall()
    return {
        "n": len(rows),
        "items": [
            {
                "id": r.id,
                "candle_start": r.candle_start.isoformat() if r.candle_start else None,
                "predict_at": r.predict_at.isoformat() if r.predict_at else None,
                "p_up_calibrated": r.p_up_calibrated,
                "p_down_calibrated": r.p_down_calibrated,
                "predicted_direction": r.predicted_direction,
                "status": r.status,
                "actual_direction": r.actual_direction,
                "future_return": r.future_return,
                "source": r.source,
                "model_id": r.model_id,
            }
            for r in rows
        ],
    }


@router.get("/performance")
def sambhav_performance(user: User = Depends(_require_user), db: Session = Depends(get_db)):
    ensure_sambhav_tables()

    def _perf(source: str) -> Dict[str, Any]:
        rows = db.execute(
            text(
                """
                SELECT predicted_direction, actual_direction, p_up_calibrated
                FROM sambhav_predictions
                WHERE instrument_key = :ik AND status = 'RESOLVED' AND source = :src
                  AND actual_direction IS NOT NULL
                """
            ),
            {"ik": INSTRUMENT_KEY, "src": source},
        ).fetchall()
        n = len(rows)
        if n < 30:
            return {"source": source, "n": n, "status": "INSUFFICIENT DATA", "accuracy": None}
        correct = sum(1 for r in rows if r.predicted_direction == r.actual_direction)
        return {
            "source": source,
            "n": n,
            "status": "OK",
            "accuracy": round(correct / n, 4),
            "note": "Live vs backtest kept separate; not a validation stamp.",
        }

    latest_metric = db.execute(
        text(
            """
            SELECT model_id, eval_type, n_samples, metrics_json, created_at
            FROM sambhav_metrics ORDER BY id DESC LIMIT 1
            """
        )
    ).fetchone()
    return {
        "backtest": _perf("backtest"),
        "live": _perf("live"),
        "latest_walk_forward": {
            "model_id": latest_metric.model_id if latest_metric else None,
            "eval_type": latest_metric.eval_type if latest_metric else None,
            "n_samples": latest_metric.n_samples if latest_metric else None,
            "metrics_json": latest_metric.metrics_json if latest_metric else None,
            "created_at": latest_metric.created_at.isoformat() if latest_metric and latest_metric.created_at else None,
        }
        if latest_metric
        else {"status": "INSUFFICIENT DATA"},
        "verdict": "MODEL NOT VALIDATED",
    }


@router.get("/calibration")
def sambhav_calibration(user: User = Depends(_require_user), db: Session = Depends(get_db)):
    ensure_sambhav_tables()
    from backend.services.sambhav.data_status import calibration_status_payload

    row = db.execute(
        text(
            """
            SELECT calibration_buckets_json, metrics_json, model_id, created_at
            FROM sambhav_metrics
            WHERE calibration_buckets_json IS NOT NULL
            ORDER BY id DESC LIMIT 1
            """
        )
    ).fetchone()
    if not row:
        return calibration_status_payload(buckets={"n": 0, "ece": None, "buckets": []})
    return calibration_status_payload(
        buckets=row.calibration_buckets_json,
        metrics=row.metrics_json,
        model_id=row.model_id,
        created_at=row.created_at.isoformat() if row.created_at else None,
    )


@router.get("/model")
def sambhav_model(user: User = Depends(_require_user), db: Session = Depends(get_db)):
    ensure_sambhav_tables()
    from backend.services.sambhav.train import get_active_model_row

    active = get_active_model_row(db)
    rows = db.execute(
        text(
            """
            SELECT id, name, model_type, status, calibration_method, created_at, artifact_path
            FROM sambhav_models ORDER BY id DESC LIMIT 20
            """
        )
    ).fetchall()
    return {
        "active": active,
        "lifecycle_note": "VALIDATED/LIVE only via explicit admin POST /model/status — never automatic.",
        "models": [
            {
                "id": r.id,
                "name": r.name,
                "model_type": r.model_type,
                "status": r.status,
                "calibration_method": r.calibration_method,
                "created_at": r.created_at.isoformat() if r.created_at else None,
                "artifact_path": r.artifact_path,
            }
            for r in rows
        ],
    }


@router.post("/model/status")
def set_model_status(
    body: ModelStatusBody,
    admin: User = Depends(_require_admin),
    db: Session = Depends(get_db),
    model_id: int = Query(...),
):
    st = body.status.strip().upper()
    if st not in (STATUS_RESEARCH, STATUS_VALIDATED, STATUS_LIVE):
        raise HTTPException(400, detail="status must be RESEARCH|VALIDATED|LIVE")
    ensure_sambhav_tables()
    res = db.execute(
        text(
            """
            UPDATE sambhav_models SET status = :st, notes = COALESCE(:notes, notes),
                updated_at = CURRENT_TIMESTAMP
            WHERE id = :id
            RETURNING id, status
            """
        ),
        {"st": st, "notes": body.notes, "id": model_id},
    ).fetchone()
    if not res:
        raise HTTPException(404, detail="model not found")
    db.commit()
    return {"ok": True, "model_id": res.id, "status": res.status, "by": getattr(admin, "email", None)}


@router.post("/predict")
def sambhav_predict_now(admin: User = Depends(_require_admin), db: Session = Depends(get_db)):
    from backend.services.sambhav.predict import predict_latest, resolve_pending_predictions

    resolve_pending_predictions(db)
    return predict_latest(db, source="live")


@router.get("/jobs/{job_id}")
def sambhav_job(job_id: str, user: User = Depends(_require_user)):
    with _jobs_lock:
        job = _jobs.get(job_id)
    if not job:
        raise HTTPException(404, detail="job not found")
    return job


def _run_import_job(job_id: str, from_date: date, to_date: Optional[date], rebuild_10m: bool) -> None:
    db = SessionLocal()
    try:
        from backend.services.sambhav.importer import import_historical_10m, import_incremental_10m
        from backend.services.sambhav.sessions import classify_and_persist_sessions

        # rebuild_10m retained for API compat; V1 always uses native 10m (never 1m).
        _ = rebuild_10m
        if from_date is None:
            result = import_incremental_10m(db, to_date=to_date)
        else:
            result = import_historical_10m(
                db, from_date=from_date, to_date=to_date, resume=True
            )
            if result.get("ok"):
                classify_and_persist_sessions(db)
        _job_set(
            job_id,
            status="done" if result.get("ok") else "error",
            result=result,
            finished_at=datetime.now(IST).isoformat(),
        )
    except Exception as exc:
        logger.exception("sambhav import job failed")
        _job_set(job_id, status="error", error=str(exc), finished_at=datetime.now(IST).isoformat())
    finally:
        db.close()


@router.post("/import")
def sambhav_import(body: ImportBody, admin: User = Depends(_require_admin)):
    job_id = str(uuid.uuid4())
    _job_set(
        job_id,
        status="running",
        started_at=datetime.now(IST).isoformat(),
        parameters=body.model_dump(mode="json"),
    )
    t = threading.Thread(
        target=_run_import_job,
        args=(job_id, body.from_date, body.to_date, body.rebuild_10m),
        daemon=True,
    )
    t.start()
    return {"job_id": job_id, "status": "running"}


def _run_train_job(job_id: str, body: TrainBody) -> None:
    db = SessionLocal()
    try:
        from backend.services.sambhav.train import train_and_save

        result = train_and_save(
            db,
            model_name=body.model_name,
            model_kind=body.model_kind,
            calibration=body.calibration,
            run_validation=body.run_validation,
        )
        _job_set(job_id, status="done" if result.get("ok") else "error", result=result, finished_at=datetime.now(IST).isoformat())
    except Exception as exc:
        logger.exception("sambhav train job failed")
        _job_set(job_id, status="error", error=str(exc), finished_at=datetime.now(IST).isoformat())
    finally:
        db.close()


@router.post("/train")
def sambhav_train(body: TrainBody, admin: User = Depends(_require_admin)):
    job_id = str(uuid.uuid4())
    _job_set(job_id, status="running", started_at=datetime.now(IST).isoformat(), parameters=body.model_dump())
    threading.Thread(target=_run_train_job, args=(job_id, body), daemon=True).start()
    return {"job_id": job_id, "status": "running"}


def _run_backtest_job(job_id: str, body: BacktestBody) -> None:
    db = SessionLocal()
    try:
        from backend.services.sambhav.candles import load_10m_df_rows
        from backend.services.sambhav.walk_forward import WalkForwardConfig, run_walk_forward

        bars = load_10m_df_rows(db, complete_only=True)
        result = run_walk_forward(
            bars,
            WalkForwardConfig(
                train_bars=body.train_bars,
                test_bars=body.test_bars,
                step_bars=body.step_bars,
                calibration=body.calibration,  # type: ignore[arg-type]
                model=body.model,
            ),
        )
        _job_set(job_id, status="done", result=result, finished_at=datetime.now(IST).isoformat())
    except Exception as exc:
        logger.exception("sambhav backtest job failed")
        _job_set(job_id, status="error", error=str(exc), finished_at=datetime.now(IST).isoformat())
    finally:
        db.close()


@router.post("/backtest")
def sambhav_backtest(body: BacktestBody, admin: User = Depends(_require_admin)):
    job_id = str(uuid.uuid4())
    _job_set(job_id, status="running", started_at=datetime.now(IST).isoformat(), parameters=body.model_dump())
    threading.Thread(target=_run_backtest_job, args=(job_id, body), daemon=True).start()
    return {"job_id": job_id, "status": "running"}


@router.get("/tradingview-stub")
def tradingview_stub(user: User = Depends(_require_user)):
    """Phase 14 stub — light TV integration deferred."""
    return {
        "status": "DEFERRED",
        "message": (
            "TradingView embedding deferred. Use Sambhav history table + external TV "
            "NIFTY chart (NSE:NIFTY) on 10m for visual context."
        ),
        "suggested_symbol": "NSE:NIFTY",
        "interval": "10",
    }

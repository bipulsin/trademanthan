"""Phase 2A — leakage-free ML dataset build + target research (no model training)."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session

from backend.services.sambhav.candles import to_ist
from backend.services.sambhav.config import (
    DATASET_VERSION_V1,
    FEATURES_VERSION_V1,
    HORIZON_BARS,
    INSTRUMENT_KEY,
    IST,
    SESSION_TYPE_REGULAR,
    TF_MINUTES,
)
from backend.services.sambhav.features_v1 import (
    FEATURE_NAMES_V1,
    FEATURE_WARMUP_BARS_V1,
    assert_no_lookahead_features_v1,
    assess_volume_availability,
    compute_features_v1,
    feature_completeness_mask,
)
from backend.services.sambhav.tables import ensure_sambhav_tables
from backend.services.sambhav.targets import (
    TARGET_EXCLUDE_TIMES,
    attach_same_session_targets,
    ternary_label,
)

logger = logging.getLogger(__name__)

MEANINGFUL_MOVE_THRESHOLDS = (0.0010, 0.0015, 0.0020, 0.0025, 0.0030)  # ±0.10% … ±0.30%
ZERO_EPS = 1e-10


def load_regular_v1_bars(
    db: Session,
    *,
    instrument_key: str = INSTRUMENT_KEY,
    dataset_version: str = DATASET_VERSION_V1,
) -> List[Dict[str, Any]]:
    """Load source OHLC for REGULAR / included_in_sambhav_v1 sessions only. Read-only."""
    ensure_sambhav_tables()
    active = db.execute(
        text(
            """
            SELECT dataset_version FROM sambhav_dataset_versions
            WHERE is_active = TRUE ORDER BY created_at DESC LIMIT 1
            """
        )
    ).fetchone()
    if active and str(active.dataset_version) != dataset_version:
        logger.warning(
            "active dataset_version=%s expected=%s — proceeding with regular sessions filter",
            active.dataset_version,
            dataset_version,
        )

    rows = db.execute(
        text(
            """
            SELECT c.candle_start, c.candle_end, c.open, c.high, c.low, c.close, c.volume
            FROM sambhav_10m_candles c
            INNER JOIN sambhav_sessions s
              ON s.instrument_key = c.instrument_key
             AND s.session_date = (c.candle_start AT TIME ZONE 'Asia/Kolkata')::date
            WHERE c.instrument_key = :ik
              AND s.session_type = :st
              AND s.included_in_sambhav_v1 = TRUE
            ORDER BY c.candle_start ASC
            """
        ),
        {"ik": instrument_key, "st": SESSION_TYPE_REGULAR},
    ).mappings().all()

    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "candle_start": r["candle_start"],
                "candle_end": r["candle_end"],
                "open": float(r["open"]),
                "high": float(r["high"]),
                "low": float(r["low"]),
                "close": float(r["close"]),
                "volume": float(r["volume"] or 0),
            }
        )
    return out


def _return_distribution(returns: np.ndarray) -> Dict[str, Any]:
    r = returns[np.isfinite(returns)]
    if len(r) == 0:
        return {"count": 0}
    pcts = [1, 5, 10, 25, 50, 75, 90, 95, 99]
    q = np.percentile(r, pcts)
    approx_zero = float(np.mean(np.abs(r) <= ZERO_EPS))
    return {
        "count": int(len(r)),
        "mean": float(np.mean(r)),
        "median": float(np.median(r)),
        "std": float(np.std(r, ddof=1)) if len(r) > 1 else 0.0,
        "min": float(np.min(r)),
        "max": float(np.max(r)),
        "percentiles": {f"p{p}": float(v) for p, v in zip(pcts, q)},
        "pct_positive": float(np.mean(r > 0)),
        "pct_negative": float(np.mean(r < 0)),
        "pct_approx_zero": approx_zero,
    }


def _binary_balance(returns: np.ndarray) -> Dict[str, Any]:
    r = returns[np.isfinite(returns)]
    up = int(np.sum(r > 0))
    down = int(np.sum(r <= 0))
    n = up + down
    return {
        "UP": up,
        "DOWN": down,
        "n": n,
        "pct_UP": (up / n) if n else None,
        "pct_DOWN": (down / n) if n else None,
        "definition": "UP if future_return > 0 else DOWN (ties → DOWN)",
    }


def _ternary_balance(returns: np.ndarray, threshold: float) -> Dict[str, Any]:
    r = returns[np.isfinite(returns)]
    labels = [ternary_label(float(x), threshold) for x in r]
    n = len(labels)
    counts = {k: labels.count(k) for k in ("UP", "NEUTRAL", "DOWN")}
    return {
        "threshold": threshold,
        "threshold_pct": threshold * 100.0,
        "n": n,
        "counts": counts,
        "pct": {k: (counts[k] / n if n else None) for k in counts},
    }


def assert_same_session_target_no_overnight(df: pd.DataFrame) -> None:
    """Leakage/overnight test: every resolvable target stays on the same calendar date."""
    sub = df[df["target_resolvable"]].copy()
    if sub.empty:
        raise AssertionError("no resolvable targets to validate")
    if (sub["candle_hm"].isin(TARGET_EXCLUDE_TIMES)).any():
        raise AssertionError("excluded session-end times unexpectedly resolvable")
    sess = sub["candle_start"].map(lambda t: to_ist(t).date() if to_ist(t) else None)
    targ = sub["target_timestamp"].map(lambda t: to_ist(t).date() if to_ist(t) else None)
    if (sess != targ).any():
        raise AssertionError("overnight target detected in resolvable set")
    pred = sub["predict_at"].map(to_ist)
    fut = sub["target_timestamp"].map(to_ist)
    secs = [(f - p).total_seconds() for p, f in zip(pred, fut) if p is not None and f is not None]
    if any(abs(s - HORIZON_BARS * TF_MINUTES * 60) > 1 for s in secs):
        raise AssertionError("target horizon is not 30 minutes for all resolvable rows")


def build_phase2a_frame(bars: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    """In-memory Phase 2A dataset + research stats. Does not touch DB source tables."""
    from backend.services.sambhav.features_v1 import bars_to_dataframe

    raw = bars_to_dataframe(bars)
    n_source = len(raw)
    n_sessions = int(raw["candle_start"].map(lambda t: t.date()).nunique()) if n_source else 0

    vol_info = assess_volume_availability(raw)
    feat = compute_features_v1(raw)
    labeled = attach_same_session_targets(raw)

    feat_only = feat.drop(columns=["close"], errors="ignore")
    merged = labeled.merge(feat_only, on="candle_start", how="left")

    # Target-resolvable before feature drop
    resolvable = merged["target_resolvable"].fillna(False).astype(bool)
    n_resolvable = int(resolvable.sum())
    n_excluded_no_horizon = int((~resolvable).sum())

    # Warm-up: feature-incomplete among resolvable
    complete = feature_completeness_mask(merged)
    n_warmup_lost = int((resolvable & ~complete).sum())
    ml_ready = resolvable & complete
    n_ml_ready = int(ml_ready.sum())

    returns = merged.loc[resolvable, "future_return"].to_numpy(dtype=float)
    dist = _return_distribution(returns)
    binary = _binary_balance(returns)
    ternary = [_ternary_balance(returns, thr) for thr in MEANINGFUL_MOVE_THRESHOLDS]

    # Leakage tests
    leakage = {"features_v1": "PASS", "same_session_targets": "PASS"}
    try:
        assert_no_lookahead_features_v1(bars[: min(len(bars), 200)] if len(bars) >= 60 else bars)
    except Exception as exc:
        leakage["features_v1"] = f"FAIL: {exc}"
    try:
        assert_same_session_target_no_overnight(merged)
    except Exception as exc:
        leakage["same_session_targets"] = f"FAIL: {exc}"

    research = {
        "phase": "2A",
        "dataset_version": DATASET_VERSION_V1,
        "feature_version": FEATURES_VERSION_V1,
        "source_candles": n_source,
        "regular_sessions": n_sessions,
        "usable_target_observations": n_resolvable,
        "excluded_no_30m_horizon": n_excluded_no_horizon,
        "feature_warmup_exclusions": n_warmup_lost,
        "final_ml_ready_rows": n_ml_ready,
        "feature_count": len(FEATURE_NAMES_V1),
        "feature_list": list(FEATURE_NAMES_V1),
        "feature_warmup_bars_design": FEATURE_WARMUP_BARS_V1,
        "volume": vol_info,
        "vwap": {
            "available": False,
            "reason": "excluded_because_volume_unavailable",
        },
        "future_return_distribution": dist,
        "target_a_binary": binary,
        "target_b_meaningful_move": ternary,
        "target_c_regression": {
            "name": "future_return_30m",
            "definition": "(close[T+3]/close[T]) - 1, same session only",
            "n": int(dist.get("count") or 0),
        },
        "target_status": "UNDER RESEARCH",
        "model_status": "NOT TRAINED",
        "validation_status": "NOT STARTED",
        "lookahead_tests": leakage,
        "exclude_times": sorted(TARGET_EXCLUDE_TIMES),
        "horizon_bars": HORIZON_BARS,
    }
    return {"frame": merged, "ml_ready_mask": ml_ready, "research": research}


def persist_phase2a_features(
    db: Session,
    frame: pd.DataFrame,
    *,
    persist_mask: Optional[pd.Series] = None,
    instrument_key: str = INSTRUMENT_KEY,
    dataset_version: str = DATASET_VERSION_V1,
    feature_version: str = FEATURES_VERSION_V1,
    volume_available: bool = False,
    batch_size: int = 500,
) -> int:
    """Upsert feature rows. Never writes to sambhav_10m_candles."""
    ensure_sambhav_tables()
    # Expand schema for Phase 2A columns
    db.execute(
        text(
            """
            ALTER TABLE sambhav_features ADD COLUMN IF NOT EXISTS candle_close TIMESTAMPTZ;
            ALTER TABLE sambhav_features ADD COLUMN IF NOT EXISTS current_price DOUBLE PRECISION;
            ALTER TABLE sambhav_features ADD COLUMN IF NOT EXISTS target_future_close DOUBLE PRECISION;
            ALTER TABLE sambhav_features ADD COLUMN IF NOT EXISTS target_future_return DOUBLE PRECISION;
            ALTER TABLE sambhav_features ADD COLUMN IF NOT EXISTS target_direction TEXT;
            ALTER TABLE sambhav_features ADD COLUMN IF NOT EXISTS target_timestamp TIMESTAMPTZ;
            ALTER TABLE sambhav_features ADD COLUMN IF NOT EXISTS session_date DATE;
            ALTER TABLE sambhav_features ADD COLUMN IF NOT EXISTS volume_available BOOLEAN;
            ALTER TABLE sambhav_features ADD COLUMN IF NOT EXISTS features_complete BOOLEAN;
            """
        )
    )
    db.commit()

    mask = persist_mask if persist_mask is not None else frame["target_resolvable"].fillna(False)
    subset = frame.loc[mask].copy()
    written = 0
    cols = list(FEATURE_NAMES_V1)

    def _jsonable(row: pd.Series) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        for c in cols:
            v = row.get(c)
            if v is None or (isinstance(v, float) and (np.isnan(v) or np.isinf(v))):
                out[c] = None
            elif isinstance(v, (np.floating, np.integer)):
                out[c] = float(v)
            else:
                out[c] = v
        return out

    records = []
    for _, row in subset.iterrows():
        cs = to_ist(row["candle_start"])
        if cs is None:
            continue
        candle_close = cs + timedelta(minutes=TF_MINUTES)
        tt = to_ist(row["target_timestamp"]) if row.get("target_timestamp") is not None else None
        complete = all(pd.notna(row.get(c)) for c in cols)
        feat_json = _jsonable(row)
        records.append(
            {
                "ik": instrument_key,
                "cs": cs,
                "cc": candle_close,
                "fv": feature_version,
                "dv": dataset_version,
                "price": float(row["close"]),
                "fj": json.dumps(feat_json),
                "tfc": float(row["future_close"]) if pd.notna(row["future_close"]) else None,
                "tfr": float(row["future_return"]) if pd.notna(row["future_return"]) else None,
                "td": row["target_direction"] if pd.notna(row.get("target_direction")) else None,
                "tts": tt,
                "sd": cs.date(),
                "vol": bool(volume_available),
                "fc": complete,
            }
        )

    # Fast path: executemany via psycopg2
    try:
        raw_conn = db.connection().connection
        from psycopg2.extras import Json, execute_values

        sql = """
            INSERT INTO sambhav_features (
                instrument_key, candle_start, candle_close, feature_version, dataset_version,
                current_price, features_json,
                target_future_close, target_future_return, target_direction, target_timestamp,
                session_date, volume_available, features_complete, created_at
            ) VALUES %s
            ON CONFLICT (instrument_key, candle_start, feature_version) DO UPDATE SET
                candle_close = EXCLUDED.candle_close,
                dataset_version = EXCLUDED.dataset_version,
                current_price = EXCLUDED.current_price,
                features_json = EXCLUDED.features_json,
                target_future_close = EXCLUDED.target_future_close,
                target_future_return = EXCLUDED.target_future_return,
                target_direction = EXCLUDED.target_direction,
                target_timestamp = EXCLUDED.target_timestamp,
                session_date = EXCLUDED.session_date,
                volume_available = EXCLUDED.volume_available,
                features_complete = EXCLUDED.features_complete
        """
        tuples = [
            (
                r["ik"],
                r["cs"],
                r["cc"],
                r["fv"],
                r["dv"],
                r["price"],
                Json(json.loads(r["fj"])),
                r["tfc"],
                r["tfr"],
                r["td"],
                r["tts"],
                r["sd"],
                r["vol"],
                r["fc"],
                datetime.now(IST),
            )
            for r in records
        ]
        with raw_conn.cursor() as cur:
            for i in range(0, len(tuples), batch_size):
                execute_values(cur, sql, tuples[i : i + batch_size], page_size=batch_size)
        db.commit()
        return len(records)
    except Exception:
        logger.exception("sambhav phase2a fast persist failed; falling back to ORM loop")
        db.rollback()

    for i in range(0, len(records), batch_size):
        chunk = records[i : i + batch_size]
        for rec in chunk:
            db.execute(
                text(
                    """
                    INSERT INTO sambhav_features (
                        instrument_key, candle_start, candle_close, feature_version, dataset_version,
                        current_price, features_json,
                        target_future_close, target_future_return, target_direction, target_timestamp,
                        session_date, volume_available, features_complete, created_at
                    ) VALUES (
                        :ik, :cs, :cc, :fv, :dv,
                        :price, CAST(:fj AS jsonb),
                        :tfc, :tfr, :td, :tts,
                        :sd, :vol, :fc, CURRENT_TIMESTAMP
                    )
                    ON CONFLICT (instrument_key, candle_start, feature_version) DO UPDATE SET
                        candle_close = EXCLUDED.candle_close,
                        dataset_version = EXCLUDED.dataset_version,
                        current_price = EXCLUDED.current_price,
                        features_json = EXCLUDED.features_json,
                        target_future_close = EXCLUDED.target_future_close,
                        target_future_return = EXCLUDED.target_future_return,
                        target_direction = EXCLUDED.target_direction,
                        target_timestamp = EXCLUDED.target_timestamp,
                        session_date = EXCLUDED.session_date,
                        volume_available = EXCLUDED.volume_available,
                        features_complete = EXCLUDED.features_complete
                    """
                ),
                rec,
            )
        db.commit()
        written += len(chunk)
    return written


def save_research_status(db: Session, research: Dict[str, Any]) -> None:
    ensure_sambhav_tables()
    db.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS sambhav_research_status (
                phase TEXT PRIMARY KEY,
                status_json JSONB NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
    )
    db.execute(
        text(
            """
            INSERT INTO sambhav_research_status (phase, status_json, updated_at)
            VALUES ('2A', CAST(:js AS jsonb), CURRENT_TIMESTAMP)
            ON CONFLICT (phase) DO UPDATE SET
                status_json = EXCLUDED.status_json,
                updated_at = CURRENT_TIMESTAMP
            """
        ),
        {"js": json.dumps(research, default=str)},
    )
    db.commit()


def get_phase2a_status(db: Session) -> Dict[str, Any]:
    ensure_sambhav_tables()
    try:
        row = db.execute(
            text("SELECT status_json, updated_at FROM sambhav_research_status WHERE phase = '2A'")
        ).fetchone()
    except Exception:
        return {"phase": "2A", "status": "NOT_BUILT"}
    if not row:
        return {"phase": "2A", "status": "NOT_BUILT"}
    payload = row.status_json if isinstance(row.status_json, dict) else json.loads(row.status_json)
    payload["updated_at"] = row.updated_at.isoformat() if row.updated_at else None
    return payload


def run_phase2a(
    db: Session,
    *,
    persist: bool = True,
    instrument_key: str = INSTRUMENT_KEY,
) -> Dict[str, Any]:
    """Build Phase 2A dataset from existing regular candles. No download. No training."""
    bars = load_regular_v1_bars(db, instrument_key=instrument_key)
    built = build_phase2a_frame(bars)
    research = built["research"]
    written = 0
    if persist:
        # Persist all resolvable-target rows (features may still be warming up)
        written = persist_phase2a_features(
            db,
            built["frame"],
            persist_mask=built["frame"]["target_resolvable"].fillna(False),
            instrument_key=instrument_key,
            volume_available=bool(research["volume"]["volume_available"]),
        )
        research["rows_persisted"] = written
        save_research_status(db, research)
    research["persisted"] = bool(persist)
    research["status"] = (
        "PASS"
        if research["lookahead_tests"].get("features_v1") == "PASS"
        and research["lookahead_tests"].get("same_session_targets") == "PASS"
        else "FAIL"
    )
    return research

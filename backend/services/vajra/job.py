"""
Batch Vajra rating job: arbitrage_master current-month futures, every 15 minutes (IST).
"""
from __future__ import annotations

import json
import logging
import os
import time
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

import pytz
from sqlalchemy import text

from backend.config import settings
from backend.database import SessionLocal
from backend.services.smart_futures_session_date import effective_session_date_ist_for_trend
from backend.services.upstox_service import UpstoxService
from backend.services.vajra.engine import compute_ecs_rating, compute_vajra_rating, sort_vajra_rows
from backend.services.vajra.pipeline import run_transition_pipeline
from backend.services.vajra.ranking import sort_vajra_rows_for_display
from backend.services.vajra.staleness import is_vajra_db_snapshot_stale, is_vajra_ratings_stale
from backend.services.vajra.tables import ensure_vajra_futures_rating_table
from backend.services.vajra.timeframes import (
    DEFAULT_HTF,
    DEFAULT_SCAN_TF,
    MIN_SCAN_BARS,
    fetch_config,
    validate_tf_pair,
)

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

_LIVE_CACHE: Dict[str, Tuple[float, List[Dict[str, Any]]]] = {}
_LIVE_CACHE_TTL_SEC = 300


def clear_vajra_live_cache(session_date: Optional[date] = None) -> None:
    """Drop in-process rating cache after a successful DB persist."""
    if session_date is None:
        session_date = effective_session_date_ist_for_trend()
    for k in list(_LIVE_CACHE.keys()):
        if k.startswith(f"{session_date.isoformat()}:"):
            _LIVE_CACHE.pop(k, None)


def _sort_candles(raw: Optional[List[dict]]) -> List[dict]:
    if not raw:
        return []
    out = list(raw)

    def _ts(c: dict) -> str:
        return str(c.get("timestamp") or "")

    out.sort(key=_ts)
    return out


def load_arbitrage_curr_mth_universe() -> List[Dict[str, str]]:
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT stock, currmth_future_symbol, currmth_future_instrument_key
                FROM arbitrage_master
                WHERE currmth_future_instrument_key IS NOT NULL
                  AND TRIM(currmth_future_instrument_key) <> ''
                ORDER BY stock
                """
            )
        ).fetchall()
        return [
            {
                "stock": str(r[0] or "").strip(),
                "future_symbol": str(r[1] or "").strip(),
                "instrument_key": str(r[2] or "").strip(),
            }
            for r in rows
        ]
    finally:
        db.close()


def fetch_vajra_ratings_for_session(session_date: Optional[date] = None) -> List[Dict[str, Any]]:
    sd = session_date or effective_session_date_ist_for_trend()
    db = SessionLocal()
    try:
        ensure_vajra_futures_rating_table(db)
        rows = db.execute(
            text(
                """
                SELECT stock, future_symbol, instrument_key, trade_type, confidence,
                       bull_score, bear_score,
                       structure_pass, momentum_pass, trend_pass, volume_pass,
                       obv_label, market_phase, reversal_risk, computed_at,
                       tps_score, ecs_score, transition_state,
                       vwap_reclaim_status, ema_reclaim_status, rsi_transition_status,
                       pullback_quality_score, extension_risk_score,
                       execution_validated, execution_step, pipeline_stage, alertable,
                       ees_score, entry_state, enter_action, enter_enabled, ees_alerts,
                       trade_quality_score, discovery_score, conviction_score,
                       risk_efficiency_score, primary_blocker, qualification_stage,
                       execution_score, evs_score, breakout_phase
                FROM vajra_futures_rating
                WHERE session_date = :sd
                ORDER BY trade_type, confidence DESC, stock
                """
            ),
            {"sd": sd},
        ).fetchall()
        out: List[Dict[str, Any]] = []
        for r in rows:
            raw_alerts = r[31]
            if isinstance(raw_alerts, str):
                try:
                    ees_alerts = json.loads(raw_alerts) if raw_alerts else []
                except json.JSONDecodeError:
                    ees_alerts = []
            elif isinstance(raw_alerts, list):
                ees_alerts = raw_alerts
            else:
                ees_alerts = []
            out.append(
                {
                    "security": r[1] or r[0],
                    "stock": r[0],
                    "instrument_key": r[2],
                    "trade_type": r[3],
                    "confidence": float(r[4]) if r[4] is not None else 0.0,
                    "bull_score": float(r[5]) if r[5] is not None else None,
                    "bear_score": float(r[6]) if r[6] is not None else None,
                    "structure": "✔ PASS" if r[7] else "✘ FAIL",
                    "momentum": "✔ PASS" if r[8] else "✘ FAIL",
                    "trend": "✔ PASS" if r[9] else "✘ FAIL",
                    "volume": "✔ PASS" if r[10] else "✘ FAIL",
                    "obv": r[11],
                    "market_phase": r[12],
                    "reversal_risk": r[13],
                    "computed_at": r[14].isoformat() if r[14] else None,
                    "tps_score": float(r[15]) if r[15] is not None else None,
                    "ecs_score": float(r[16]) if r[16] is not None else None,
                    "transition_state": r[17],
                    "vwap_reclaim_status": r[18],
                    "ema_reclaim_status": r[19],
                    "rsi_transition_status": r[20],
                    "pullback_quality_score": float(r[21]) if r[21] is not None else None,
                    "extension_risk_score": float(r[22]) if r[22] is not None else None,
                    "execution_validated": bool(r[23]) if r[23] is not None else False,
                    "execution_step": r[24],
                    "pipeline_stage": r[25],
                    "alertable": bool(r[26]) if r[26] is not None else False,
                    "ees_score": float(r[27]) if r[27] is not None else None,
                    "entry_state": r[28],
                    "enter_action": r[29],
                    "enter_enabled": bool(r[30]) if r[30] is not None else False,
                    "ees_alerts": ees_alerts,
                    "trade_quality_score": float(r[32]) if len(r) > 32 and r[32] is not None else None,
                    "discovery_score": float(r[33]) if len(r) > 33 and r[33] is not None else None,
                    "conviction_score": float(r[34]) if len(r) > 34 and r[34] is not None else None,
                    "risk_efficiency_score": float(r[35]) if len(r) > 35 and r[35] is not None else None,
                    "primary_blocker": r[36] if len(r) > 36 else None,
                    "qualification_stage": r[37] if len(r) > 37 else None,
                    "execution_score": float(r[38]) if len(r) > 38 and r[38] is not None else None,
                    "evs_score": float(r[39]) if len(r) > 39 and r[39] is not None else None,
                    "breakout_phase": r[40] if len(r) > 40 else None,
                    "qualification_state": (r[37] or r[28] or "").upper() if len(r) > 37 else None,
                }
            )
        from backend.services.vajra.ui_mapping import finalize_screener_rows

        return finalize_screener_rows(sort_vajra_rows_for_display(out))
    finally:
        db.close()


def fetch_vajra_ratings_updated_at(session_date: Optional[date] = None) -> Optional[datetime]:
    """Latest computed_at for the session (set when the 5m rating job finishes)."""
    sd = session_date or effective_session_date_ist_for_trend()
    db = SessionLocal()
    try:
        ensure_vajra_futures_rating_table(db)
        row = db.execute(
            text(
                """
                SELECT MAX(computed_at)
                FROM vajra_futures_rating
                WHERE session_date = :sd
                """
            ),
            {"sd": sd},
        ).fetchone()
        if not row or row[0] is None:
            return None
        return row[0]
    finally:
        db.close()


def _fetch_candles_for_tf(upstox: UpstoxService, instrument_key: str, tf_id: str) -> List[dict]:
    cfg = fetch_config(tf_id)
    raw = upstox.get_historical_candles_by_instrument_key(
        instrument_key,
        interval=str(cfg["interval"]),
        days_back=int(cfg["days_back"]),
    )
    return _sort_candles(raw)


def _rating_to_api_row(
    rating,
    *,
    stock: str,
    fut_sym: str,
    computed_at: datetime,
) -> Dict[str, Any]:
    d = rating.to_row_dict()
    return {
        "security": fut_sym or stock,
        "stock": stock,
        "trade_type": d["trade_type"],
        "confidence": d["confidence"],
        "structure": d["structure"],
        "momentum": d["momentum"],
        "trend": d["trend"],
        "volume": d["volume"],
        "obv": d["obv"],
        "market_phase": d["market_phase"],
        "reversal_risk": d["reversal_risk"],
        "computed_at": computed_at.isoformat(),
    }


def _run_transition_pipeline_live(session_date: Optional[date] = None) -> List[Dict[str, Any]]:
    sd = session_date or effective_session_date_ist_for_trend()
    universe = load_arbitrage_curr_mth_universe()
    if not universe:
        return []
    upstox = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)

    def _fetch(key: str, tf: str) -> List[dict]:
        return _fetch_candles_for_tf(upstox, key, tf)

    return sort_vajra_rows_for_display(
        run_transition_pipeline(universe, _fetch, computed_at=datetime.now(IST))
    )


def resolve_vajra_ratings_for_api(
    session_date: Optional[date] = None,
    *,
    use_cache: bool = True,
) -> Tuple[List[Dict[str, Any]], str, Optional[str]]:
    """
    Load ratings for API: DB when fresh, else live pipeline recompute.
    Returns (rows, source, stale_reason).
    """
    sd = session_date or effective_session_date_ist_for_trend()
    cache_key = f"{sd.isoformat()}:transition"
    now = time.time()
    if use_cache and cache_key in _LIVE_CACHE:
        ts, rows = _LIVE_CACHE[cache_key]
        if now - ts < _LIVE_CACHE_TTL_SEC:
            return rows, "cache", None

    db_rows = fetch_vajra_ratings_for_session(sd)
    updated_at = fetch_vajra_ratings_updated_at(sd)
    stale, reason = is_vajra_ratings_stale(db_rows, updated_at)
    if db_rows and not stale:
        if use_cache:
            _LIVE_CACHE[cache_key] = (now, db_rows)
        return db_rows, "db", None

    if stale and db_rows:
        logger.info("vajra ratings stale (%s) — live recompute for API", reason)
    elif not db_rows:
        logger.info("vajra ratings empty — live recompute for API")

    rows = _run_transition_pipeline_live(sd)
    source = "live_recompute" if db_rows else "live"
    if use_cache and rows:
        _LIVE_CACHE[cache_key] = (now, rows)
    return rows, source, reason


def maybe_refresh_vajra_after_deploy() -> Optional[Dict[str, Any]]:
    """Background persist after deploy/startup when DB snapshot is stale."""
    sd = effective_session_date_ist_for_trend()
    updated_at = fetch_vajra_ratings_updated_at(sd)
    db = SessionLocal()
    try:
        ensure_vajra_futures_rating_table(db)
        count = int(
            db.execute(
                text("SELECT COUNT(*) FROM vajra_futures_rating WHERE session_date = :sd"),
                {"sd": sd},
            ).scalar()
            or 0
        )
        missing_evs = int(
            db.execute(
                text(
                    """
                    SELECT COUNT(*) FROM vajra_futures_rating
                    WHERE session_date = :sd AND evs_score IS NULL
                    """
                ),
                {"sd": sd},
            ).scalar()
            or 0
        )
    finally:
        db.close()

    need, reason = is_vajra_db_snapshot_stale(
        row_count=count,
        missing_evs_count=missing_evs,
        updated_at=updated_at,
    )
    if not need:
        logger.info("vajra post_deploy: ratings fresh — skip background refresh")
        return None

    logger.info("vajra post_deploy: background refresh (%s)", reason)
    return run_vajra_futures_rating_job(scan_trigger=f"post_deploy:{reason}")


def compute_vajra_ratings_live(
    scan_tf: str = DEFAULT_SCAN_TF,
    htf: str = DEFAULT_HTF,
    session_date: Optional[date] = None,
    *,
    use_cache: bool = True,
    mode: str = "transition",
) -> List[Dict[str, Any]]:
    """Compute Vajra ratings. Default mode: 30m TPS discovery + 5m shortlist validation."""
    sd = session_date or effective_session_date_ist_for_trend()
    mode_norm = (mode or "transition").strip().lower()

    if mode_norm == "transition":
        rows, _, _ = resolve_vajra_ratings_for_api(sd, use_cache=use_cache)
        return rows

    scan_id, htf_id = validate_tf_pair(scan_tf, htf)
    cache_key = f"{sd.isoformat()}:{scan_id}:{htf_id}:legacy"
    now = time.time()
    if use_cache and cache_key in _LIVE_CACHE:
        ts, rows = _LIVE_CACHE[cache_key]
        if now - ts < _LIVE_CACHE_TTL_SEC:
            return rows

    universe = load_arbitrage_curr_mth_universe()
    if not universe:
        return []

    upstox = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    computed_at = datetime.now(IST)
    rows: List[Dict[str, Any]] = []

    for item in universe:
        stock = item["stock"]
        fut_sym = item["future_symbol"]
        fut_key = item["instrument_key"]
        try:
            c_scan = _fetch_candles_for_tf(upstox, fut_key, scan_id)
            if len(c_scan) < MIN_SCAN_BARS:
                continue
            c_htf = _fetch_candles_for_tf(upstox, fut_key, htf_id)
            rating = compute_ecs_rating(c_scan, c_htf)
            if rating is None:
                continue
            row = _rating_to_api_row(
                rating,
                stock=stock,
                fut_sym=fut_sym,
                computed_at=computed_at,
            )
            row["ecs_score"] = row["confidence"]
            rows.append(row)
        except Exception as e:
            logger.debug("vajra_live: skip %s (%s/%s): %s", stock, scan_id, htf_id, e)

    rows = sort_vajra_rows(rows, discovery_first=False)
    if use_cache:
        _LIVE_CACHE[cache_key] = (now, rows)
    return rows


def run_vajra_futures_rating_job(scan_trigger: str = "manual") -> Dict[str, Any]:
    """
    Rate all current-month futures from arbitrage_master and persist for today's session.
    """
    if os.getenv("VAJRA_RATING_FORCE_WEEKEND", "").strip() not in ("1", "true", "yes"):
        from backend.services.market_holiday import should_skip_scheduled_market_jobs_ist

        if should_skip_scheduled_market_jobs_ist():
            logger.info("vajra_rating: skip non-trading day (%s)", scan_trigger)
            return {"skipped": "non_trading_day", "scan_trigger": scan_trigger}

    session_date = effective_session_date_ist_for_trend()
    universe = load_arbitrage_curr_mth_universe()
    if not universe:
        logger.info("vajra_rating: empty arbitrage_master universe")
        return {"skipped": "no_universe", "scan_trigger": scan_trigger}

    try:
        upstox = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    except Exception as e:
        logger.error("vajra_rating: Upstox init failed: %s", e)
        return {"error": str(e), "scan_trigger": scan_trigger}

    rated = 0
    skipped = 0
    errors = 0
    persist_rows: List[Dict[str, Any]] = []

    def _fetch(key: str, tf: str) -> List[dict]:
        return _fetch_candles_for_tf(upstox, key, tf)

    pipeline_rows = run_transition_pipeline(universe, _fetch)
    computed_at = datetime.now(IST)
    key_by_stock = {u["stock"]: u for u in universe}

    for prow in pipeline_rows:
        stock = prow.get("stock") or ""
        item = key_by_stock.get(stock)
        if not item:
            continue
        fut_key = item["instrument_key"]
        fut_sym = item["future_symbol"]
        row = dict(prow)
        row.update(
            {
                "session_date": session_date,
                "stock": stock,
                "future_symbol": fut_sym,
                "instrument_key": fut_key,
                "security": fut_sym or stock,
                "computed_at": computed_at,
                "structure_pass": "PASS" in str(prow.get("structure") or ""),
                "momentum_pass": "PASS" in str(prow.get("momentum") or ""),
                "trend_pass": "PASS" in str(prow.get("trend") or ""),
                "volume_pass": "PASS" in str(prow.get("volume") or ""),
                "obv_label": prow.get("obv"),
                "ecs_score": prow.get("ecs_score") or prow.get("confidence"),
            }
        )
        persist_rows.append(row)
        rated += 1

    db = SessionLocal()
    try:
        ensure_vajra_futures_rating_table(db)
        db.execute(
            text("DELETE FROM vajra_futures_rating WHERE session_date = :sd"),
            {"sd": session_date},
        )
        for row in persist_rows:
            db.execute(
                text(
                    """
                    INSERT INTO vajra_futures_rating (
                        session_date, stock, future_symbol, instrument_key,
                        trade_type, confidence, bull_score, bear_score,
                        structure_pass, momentum_pass, trend_pass, volume_pass,
                        obv_label, market_phase, reversal_risk, computed_at,
                        tps_score, ecs_score, transition_state,
                        vwap_reclaim_status, ema_reclaim_status, rsi_transition_status,
                        pullback_quality_score, extension_risk_score,
                        execution_validated, execution_step, pipeline_stage, alertable,
                        ees_score, entry_state, enter_action, enter_enabled, ees_alerts,
                        trade_quality_score, discovery_score, conviction_score,
                        risk_efficiency_score, primary_blocker, qualification_stage, execution_score,
                        evs_score, breakout_phase
                    ) VALUES (
                        :session_date, :stock, :future_symbol, :instrument_key,
                        :trade_type, :confidence, :bull_score, :bear_score,
                        :structure_pass, :momentum_pass, :trend_pass, :volume_pass,
                        :obv_label, :market_phase, :reversal_risk, :computed_at,
                        :tps_score, :ecs_score, :transition_state,
                        :vwap_reclaim_status, :ema_reclaim_status, :rsi_transition_status,
                        :pullback_quality_score, :extension_risk_score,
                        :execution_validated, :execution_step, :pipeline_stage, :alertable,
                        :ees_score, :entry_state, :enter_action, :enter_enabled, :ees_alerts,
                        :trade_quality_score, :discovery_score, :conviction_score,
                        :risk_efficiency_score, :primary_blocker, :qualification_stage, :execution_score,
                        :evs_score, :breakout_phase
                    )
                    ON CONFLICT (session_date, instrument_key) DO UPDATE SET
                        stock = EXCLUDED.stock,
                        future_symbol = EXCLUDED.future_symbol,
                        trade_type = EXCLUDED.trade_type,
                        confidence = EXCLUDED.confidence,
                        bull_score = EXCLUDED.bull_score,
                        bear_score = EXCLUDED.bear_score,
                        structure_pass = EXCLUDED.structure_pass,
                        momentum_pass = EXCLUDED.momentum_pass,
                        trend_pass = EXCLUDED.trend_pass,
                        volume_pass = EXCLUDED.volume_pass,
                        obv_label = EXCLUDED.obv_label,
                        market_phase = EXCLUDED.market_phase,
                        reversal_risk = EXCLUDED.reversal_risk,
                        computed_at = EXCLUDED.computed_at,
                        tps_score = EXCLUDED.tps_score,
                        ecs_score = EXCLUDED.ecs_score,
                        transition_state = EXCLUDED.transition_state,
                        vwap_reclaim_status = EXCLUDED.vwap_reclaim_status,
                        ema_reclaim_status = EXCLUDED.ema_reclaim_status,
                        rsi_transition_status = EXCLUDED.rsi_transition_status,
                        pullback_quality_score = EXCLUDED.pullback_quality_score,
                        extension_risk_score = EXCLUDED.extension_risk_score,
                        execution_validated = EXCLUDED.execution_validated,
                        execution_step = EXCLUDED.execution_step,
                        pipeline_stage = EXCLUDED.pipeline_stage,
                        alertable = EXCLUDED.alertable,
                        ees_score = EXCLUDED.ees_score,
                        entry_state = EXCLUDED.entry_state,
                        enter_action = EXCLUDED.enter_action,
                        enter_enabled = EXCLUDED.enter_enabled,
                        ees_alerts = EXCLUDED.ees_alerts,
                        trade_quality_score = EXCLUDED.trade_quality_score,
                        discovery_score = EXCLUDED.discovery_score,
                        conviction_score = EXCLUDED.conviction_score,
                        risk_efficiency_score = EXCLUDED.risk_efficiency_score,
                        primary_blocker = EXCLUDED.primary_blocker,
                        qualification_stage = EXCLUDED.qualification_stage,
                        execution_score = EXCLUDED.execution_score
                    """
                ),
                {
                    "session_date": session_date,
                    "stock": row["stock"],
                    "future_symbol": row["future_symbol"],
                    "instrument_key": row["instrument_key"],
                    "trade_type": row["trade_type"],
                    "confidence": row["confidence"],
                    "bull_score": row["bull_score"],
                    "bear_score": row["bear_score"],
                    "structure_pass": row["structure_pass"],
                    "momentum_pass": row["momentum_pass"],
                    "trend_pass": row["trend_pass"],
                    "volume_pass": row["volume_pass"],
                    "obv_label": row["obv"],
                    "market_phase": row["market_phase"],
                    "reversal_risk": row["reversal_risk"],
                    "computed_at": row["computed_at"],
                    "tps_score": row.get("tps_score"),
                    "ecs_score": row.get("ecs_score"),
                    "transition_state": row.get("transition_state"),
                    "vwap_reclaim_status": row.get("vwap_reclaim_status"),
                    "ema_reclaim_status": row.get("ema_reclaim_status"),
                    "rsi_transition_status": row.get("rsi_transition_status"),
                    "pullback_quality_score": row.get("pullback_quality_score"),
                    "extension_risk_score": row.get("extension_risk_score"),
                    "execution_validated": row.get("execution_validated", False),
                    "execution_step": row.get("execution_step"),
                    "pipeline_stage": row.get("pipeline_stage"),
                    "alertable": row.get("alertable", False),
                    "ees_score": row.get("ees_score"),
                    "entry_state": row.get("entry_state"),
                    "enter_action": row.get("enter_action"),
                    "enter_enabled": row.get("enter_enabled", False),
                    "ees_alerts": json.dumps(row.get("ees_alerts") or []),
                    "trade_quality_score": row.get("trade_quality_score"),
                    "discovery_score": row.get("discovery_score"),
                    "conviction_score": row.get("conviction_score") or row.get("confidence"),
                    "risk_efficiency_score": row.get("risk_efficiency_score"),
                    "primary_blocker": row.get("primary_blocker"),
                    "qualification_stage": row.get("qualification_stage"),
                    "execution_score": row.get("execution_score"),
                    "evs_score": row.get("evs_score"),
                    "breakout_phase": row.get("breakout_phase"),
                },
            )
        db.commit()
        clear_vajra_live_cache(session_date)
    except Exception as e:
        db.rollback()
        logger.exception("vajra_rating: persist failed: %s", e)
        return {"error": str(e), "scan_trigger": scan_trigger, "rated": rated}
    finally:
        db.close()

    logger.info(
        "vajra_rating [%s]: session=%s rated=%s skipped=%s errors=%s universe=%s",
        scan_trigger,
        session_date,
        rated,
        skipped,
        errors,
        len(universe),
    )
    return {
        "scan_trigger": scan_trigger,
        "session_date": session_date.isoformat(),
        "universe": len(universe),
        "rated": rated,
        "skipped": skipped,
        "errors": errors,
        "computed_at": computed_at.isoformat(),
    }

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


def sort_vajra_rows_for_display(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Sort for UI: ENTER-enabled first, then EES, then TPS."""

    def _sort_key(r: Dict[str, Any]) -> tuple:
        enter = (
            1
            if r.get("enter_enabled")
            or str(r.get("enter_action") or "").upper() == "ENTER"
            else 0
        )
        ees = float(r["ees_score"]) if r.get("ees_score") is not None else -1.0
        tps = float(r.get("tps_score") or 0)
        sym = str(r.get("security") or r.get("stock") or "")
        return (-enter, -ees, -tps, sym)

    return sorted(rows, key=_sort_key)


def fetch_vajra_ratings_for_session(session_date: Optional[date] = None) -> List[Dict[str, Any]]:
    sd = session_date or effective_session_date_ist_for_trend()
    db = SessionLocal()
    try:
        ensure_vajra_futures_rating_table(db)
        rows = db.execute(
            text(
                """
                SELECT stock, future_symbol, instrument_key, trade_type, confidence,
                       structure_pass, momentum_pass, trend_pass, volume_pass,
                       obv_label, market_phase, reversal_risk, computed_at,
                       tps_score, ecs_score, transition_state,
                       vwap_reclaim_status, ema_reclaim_status, rsi_transition_status,
                       pullback_quality_score, extension_risk_score,
                       execution_validated, execution_step, pipeline_stage, alertable,
                       ees_score, entry_state, enter_action, enter_enabled, ees_alerts
                FROM vajra_futures_rating
                WHERE session_date = :sd
                ORDER BY trade_type, confidence DESC, stock
                """
            ),
            {"sd": sd},
        ).fetchall()
        out: List[Dict[str, Any]] = []
        for r in rows:
            raw_alerts = r[29]
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
                    "structure": "✔ PASS" if r[5] else "✘ FAIL",
                    "momentum": "✔ PASS" if r[6] else "✘ FAIL",
                    "trend": "✔ PASS" if r[7] else "✘ FAIL",
                    "volume": "✔ PASS" if r[8] else "✘ FAIL",
                    "obv": r[9],
                    "market_phase": r[10],
                    "reversal_risk": r[11],
                    "computed_at": r[12].isoformat() if r[12] else None,
                    "tps_score": float(r[13]) if r[13] is not None else None,
                    "ecs_score": float(r[14]) if r[14] is not None else None,
                    "transition_state": r[15],
                    "vwap_reclaim_status": r[16],
                    "ema_reclaim_status": r[17],
                    "rsi_transition_status": r[18],
                    "pullback_quality_score": float(r[19]) if r[19] is not None else None,
                    "extension_risk_score": float(r[20]) if r[20] is not None else None,
                    "execution_validated": bool(r[21]) if r[21] is not None else False,
                    "execution_step": r[22],
                    "pipeline_stage": r[23],
                    "alertable": bool(r[24]) if r[24] is not None else False,
                    "ees_score": float(r[25]) if r[25] is not None else None,
                    "entry_state": r[26],
                    "enter_action": r[27],
                    "enter_enabled": bool(r[28]) if r[28] is not None else False,
                    "ees_alerts": ees_alerts,
                }
            )
        return sort_vajra_rows_for_display(out)
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
        cache_key = f"{sd.isoformat()}:transition"
        now = time.time()
        if use_cache and cache_key in _LIVE_CACHE:
            ts, rows = _LIVE_CACHE[cache_key]
            if now - ts < _LIVE_CACHE_TTL_SEC:
                return rows
        db_rows = fetch_vajra_ratings_for_session(sd)
        if db_rows:
            if use_cache:
                _LIVE_CACHE[cache_key] = (now, db_rows)
            return db_rows
        universe = load_arbitrage_curr_mth_universe()
        if not universe:
            return []
        upstox = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)

        def _fetch(key: str, tf: str) -> List[dict]:
            return _fetch_candles_for_tf(upstox, key, tf)

        rows = sort_vajra_rows_for_display(
            run_transition_pipeline(universe, _fetch, computed_at=datetime.now(IST))
        )
        if use_cache:
            _LIVE_CACHE[cache_key] = (now, rows)
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

    computed_at = datetime.now(IST)
    rated = 0
    skipped = 0
    errors = 0
    persist_rows: List[Dict[str, Any]] = []

    def _fetch(key: str, tf: str) -> List[dict]:
        return _fetch_candles_for_tf(upstox, key, tf)

    pipeline_rows = run_transition_pipeline(universe, _fetch, computed_at=computed_at)
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
                        ees_score, entry_state, enter_action, enter_enabled, ees_alerts
                    ) VALUES (
                        :session_date, :stock, :future_symbol, :instrument_key,
                        :trade_type, :confidence, :bull_score, :bear_score,
                        :structure_pass, :momentum_pass, :trend_pass, :volume_pass,
                        :obv_label, :market_phase, :reversal_risk, :computed_at,
                        :tps_score, :ecs_score, :transition_state,
                        :vwap_reclaim_status, :ema_reclaim_status, :rsi_transition_status,
                        :pullback_quality_score, :extension_risk_score,
                        :execution_validated, :execution_step, :pipeline_stage, :alertable,
                        :ees_score, :entry_state, :enter_action, :enter_enabled, :ees_alerts
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
                        alertable = EXCLUDED.alertable
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
                },
            )
        db.commit()
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
    }

"""
Batch Vajra rating job: arbitrage_master current-month futures, every 15 minutes (IST).
"""
from __future__ import annotations

import logging
import os
from datetime import date, datetime
from typing import Any, Dict, List, Optional

import pytz
from sqlalchemy import text

from backend.config import settings
from backend.database import SessionLocal
from backend.services.smart_futures_session_date import effective_session_date_ist_for_trend
from backend.services.upstox_service import UpstoxService
from backend.services.vajra.engine import compute_vajra_rating, sort_vajra_rows
from backend.services.vajra.tables import ensure_vajra_futures_rating_table

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")


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
                SELECT stock, future_symbol, trade_type, confidence,
                       structure_pass, momentum_pass, trend_pass, volume_pass,
                       obv_label, market_phase, reversal_risk, computed_at
                FROM vajra_futures_rating
                WHERE session_date = :sd
                ORDER BY trade_type, confidence DESC, stock
                """
            ),
            {"sd": sd},
        ).fetchall()
        out: List[Dict[str, Any]] = []
        for r in rows:
            out.append(
                {
                    "security": r[1] or r[0],
                    "stock": r[0],
                    "trade_type": r[2],
                    "confidence": float(r[3]) if r[3] is not None else 0.0,
                    "structure": "✔ PASS" if r[4] else "✘ FAIL",
                    "momentum": "✔ PASS" if r[5] else "✘ FAIL",
                    "trend": "✔ PASS" if r[6] else "✘ FAIL",
                    "volume": "✔ PASS" if r[7] else "✘ FAIL",
                    "obv": r[8],
                    "market_phase": r[9],
                    "reversal_risk": r[10],
                    "computed_at": r[11].isoformat() if r[11] else None,
                }
            )
        return sort_vajra_rows(out)
    finally:
        db.close()


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

    for item in universe:
        stock = item["stock"]
        fut_sym = item["future_symbol"]
        fut_key = item["instrument_key"]
        try:
            m15 = _sort_candles(
                upstox.get_historical_candles_by_instrument_key(
                    fut_key, interval="minutes/15", days_back=12
                )
            )
            m60 = _sort_candles(
                upstox.get_historical_candles_by_instrument_key(
                    fut_key, interval="hours/1", days_back=45
                )
            )
            rating = compute_vajra_rating(m15, m60)
            if rating is None:
                skipped += 1
                continue
            row = rating.to_row_dict()
            row.update(
                {
                    "session_date": session_date,
                    "stock": stock,
                    "future_symbol": fut_sym,
                    "instrument_key": fut_key,
                    "security": fut_sym or stock,
                    "computed_at": computed_at,
                }
            )
            persist_rows.append(row)
            rated += 1
        except Exception as e:
            errors += 1
            logger.debug("vajra_rating: skip %s: %s", stock, e)

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
                        obv_label, market_phase, reversal_risk, computed_at
                    ) VALUES (
                        :session_date, :stock, :future_symbol, :instrument_key,
                        :trade_type, :confidence, :bull_score, :bear_score,
                        :structure_pass, :momentum_pass, :trend_pass, :volume_pass,
                        :obv_label, :market_phase, :reversal_risk, :computed_at
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
                        computed_at = EXCLUDED.computed_at
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

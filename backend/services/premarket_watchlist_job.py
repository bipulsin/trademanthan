"""
Pre-market F&O watchlist: rank top N equities from arbitrage_master using the same logic as
``test_premkt_scanner.py`` (``premarket_scoring``): OBV, gap, 52w range position, prior-session
momentum — weighted composite 30/25/25/20.

Scheduled weekdays 9:14 IST (configurable); persists to ``premarket_watchlist``.
"""
from __future__ import annotations

import logging
import time
from datetime import date, datetime
from typing import Any, Dict, List, Optional

import pytz
from sqlalchemy import text
from sqlalchemy.orm import Session

from backend.config import settings
from backend.database import SessionLocal
from backend.services.premarket_scoring import (
    composite_weighted,
    min_max_norm,
    score_premarket_raw,
)
from backend.services.upstox_service import UpstoxService

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")
SLEEP_BETWEEN_SYMBOLS_SEC = 0.04


def _universe_limit() -> int:
    return max(50, min(500, int(getattr(settings, "PREMKET_UNIVERSE_LIMIT", 203))))


def _top_n() -> int:
    return max(1, min(50, int(getattr(settings, "PREMKET_TOP_N", 10))))


def _persist_rows(db: Session, session_date: date, rows: List[Dict[str, Any]], computed_at: datetime) -> None:
    db.execute(text("DELETE FROM premarket_watchlist WHERE session_date = :sd"), {"sd": session_date})
    for r in rows:
        params = {
            "session_date": session_date,
            "rank": int(r["rank"]),
            "stock": r["stock"],
            "instrument_key": r.get("instrument_key") or "",
            "obv_slope": float(r["obv_slope"]),
            "gap_strength": float(r["gap_strength"]),
            "gap_pct_signed": float(r.get("gap_pct_signed") or 0.0),
            "range_position": float(r["range_position"]),
            "composite_score": float(r["composite_score"]),
            "ltp": float(r.get("ltp") or 0.0),
            "computed_at": computed_at,
            "momentum": float(r.get("momentum") or 0.0),
        }
        # momentum column optional until migration applied
        try:
            db.execute(
                text(
                    """
                    INSERT INTO premarket_watchlist (
                        session_date, rank, stock, instrument_key,
                        obv_slope, gap_strength, gap_pct_signed, range_position,
                        composite_score, ltp, computed_at, momentum
                    ) VALUES (
                        :session_date, :rank, :stock, :instrument_key,
                        :obv_slope, :gap_strength, :gap_pct_signed, :range_position,
                        :composite_score, :ltp, :computed_at, :momentum
                    )
                    """
                ),
                params,
            )
        except Exception:
            db.execute(
                text(
                    """
                    INSERT INTO premarket_watchlist (
                        session_date, rank, stock, instrument_key,
                        obv_slope, gap_strength, gap_pct_signed, range_position,
                        composite_score, ltp, computed_at
                    ) VALUES (
                        :session_date, :rank, :stock, :instrument_key,
                        :obv_slope, :gap_strength, :gap_pct_signed, :range_position,
                        :composite_score, :ltp, :computed_at
                    )
                    """
                ),
                {k: v for k, v in params.items() if k != "momentum"},
            )
    db.commit()


def run_premarket_watchlist_job(session_date: Optional[date] = None) -> Dict[str, Any]:
    """
    Scan arbitrage_master universe, score, persist Top N.

    ``session_date``: defaults to today (IST). Use for historical backfill (weekday only).
    """
    now_ist = datetime.now(IST)
    sd = session_date if session_date is not None else now_ist.date()
    if sd.weekday() >= 5:
        return {"skipped": "weekend", "session_date": sd.isoformat(), "top": []}

    lim = _universe_limit()
    db = SessionLocal()
    try:
        qrows = db.execute(
            text(
                """
                SELECT stock, stock_instrument_key
                FROM arbitrage_master
                WHERE stock_instrument_key IS NOT NULL
                  AND TRIM(stock_instrument_key) <> ''
                ORDER BY stock
                LIMIT :lim
                """
            ),
            {"lim": lim},
        ).fetchall()
    finally:
        db.close()

    if not qrows:
        logger.warning("premarket_watchlist: empty arbitrage_master universe")
        return {"skipped": "no_universe", "top": []}

    try:
        upstox = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    except Exception as e:
        logger.error("premarket_watchlist: Upstox init failed: %s", e)
        return {"error": str(e), "top": []}

    raw_rows: List[Dict[str, Any]] = []
    for stock, ikey in qrows:
        st = str(stock or "").strip().upper()
        if not st:
            continue
        m = score_premarket_raw(upstox, st, str(ikey).strip(), sd)
        if m.get("error"):
            logger.debug("premarket_watchlist skip %s: %s", st, m.get("error"))
            continue
        raw_rows.append(m)
        time.sleep(SLEEP_BETWEEN_SYMBOLS_SEC)

    if len(raw_rows) < 5:
        logger.warning("premarket_watchlist: too few scored symbols (%s)", len(raw_rows))
        return {"error": "insufficient_data", "scored": len(raw_rows), "top": [], "session_date": sd.isoformat()}

    obv_list = [float(r["obv_slope"]) for r in raw_rows]
    gap_list = [float(r["gap_strength"]) for r in raw_rows]
    rng_list = [float(r["range_position"]) for r in raw_rows]
    mom_list = [float(r["momentum"]) for r in raw_rows]

    obv_n = min_max_norm(obv_list)
    gap_n = min_max_norm(gap_list)
    rng_n = min_max_norm(rng_list)
    mom_n = min_max_norm(mom_list)

    scored: List[Dict[str, Any]] = []
    for i, r in enumerate(raw_rows):
        comp = composite_weighted(obv_n[i], gap_n[i], rng_n[i], mom_n[i])
        scored.append(
            {
                **r,
                "obv_norm": obv_n[i],
                "gap_norm": gap_n[i],
                "range_norm": rng_n[i],
                "mom_norm": mom_n[i],
                "composite_score": comp,
            }
        )

    scored.sort(key=lambda x: float(x["composite_score"]), reverse=True)
    top = scored[: _top_n()]
    computed_at = datetime.now(IST)
    for idx, row in enumerate(top, start=1):
        row["rank"] = idx

    dbw = SessionLocal()
    try:
        _persist_rows(dbw, sd, top, computed_at)
    except Exception as e:
        logger.exception("premarket_watchlist: persist failed: %s", e)
        dbw.rollback()
        return {"error": str(e), "top": [], "session_date": sd.isoformat()}
    finally:
        dbw.close()

    logger.info(
        "premarket_watchlist: session_date=%s saved_top=%s first=%s",
        sd,
        len(top),
        top[0]["stock"] if top else None,
    )
    return {
        "session_date": sd.isoformat(),
        "computed_at": computed_at.isoformat(),
        "universe_scored": len(raw_rows),
        "top": [
            {
                "rank": r["rank"],
                "stock": r["stock"],
                "obv_slope": round(r["obv_slope"], 4),
                "gap_strength": round(r["gap_strength"], 3),
                "gap_pct_signed": round(r["gap_pct_signed"], 3),
                "range_position": round(r["range_position"], 4),
                "momentum": round(r.get("momentum") or 0.0, 4),
                "composite_score": round(r["composite_score"], 4),
                "ltp": round(r.get("ltp") or 0.0, 2),
            }
            for r in top
        ],
    }


def fetch_premarket_watchlist_for_date(session_date: date) -> List[Dict[str, Any]]:
    """Read persisted rows for dashboard API."""
    db = SessionLocal()
    try:
        try:
            rows = db.execute(
                text(
                    """
                    SELECT rank, stock, obv_slope, gap_strength, gap_pct_signed, range_position,
                           composite_score, ltp, computed_at, momentum
                    FROM premarket_watchlist
                    WHERE session_date = :sd
                    ORDER BY rank ASC
                    """
                ),
                {"sd": session_date},
            ).mappings().all()
        except Exception:
            rows = db.execute(
                text(
                    """
                    SELECT rank, stock, obv_slope, gap_strength, gap_pct_signed, range_position,
                           composite_score, ltp, computed_at
                    FROM premarket_watchlist
                    WHERE session_date = :sd
                    ORDER BY rank ASC
                    """
                ),
                {"sd": session_date},
            ).mappings().all()
        out: List[Dict[str, Any]] = []
        for r in rows:
            d = dict(r)
            ca = d.get("computed_at")
            if ca is not None and hasattr(ca, "isoformat"):
                d["computed_at"] = ca.isoformat()
            out.append(d)
        return out
    finally:
        db.close()

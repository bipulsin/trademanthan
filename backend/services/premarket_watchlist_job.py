"""
Pre-market F&O watchlist: rank top N equities from arbitrage_master by composite of
OBV slope (10-day daily), gap strength (|open − prev close| / prev close), and range position
(20-day high–low).

Scheduled weekdays ~9:15 IST; persists Top 10 to ``premarket_watchlist`` for dashboard API.
"""
from __future__ import annotations

import logging
import time
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

import pytz
from sqlalchemy import text
from sqlalchemy.orm import Session

from backend.config import settings
from backend.database import SessionLocal
from backend.services.smart_futures_picker.indicators import compute_obv_slope_daily
from backend.services.upstox_service import UpstoxService

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")
UNIVERSE_LIMIT = 200
TOP_N = 10
SLEEP_BETWEEN_SYMBOLS_SEC = 0.04


def _sort_candles(candles: Optional[List[dict]]) -> List[dict]:
    if not candles:
        return []
    return sorted(candles, key=lambda c: str(c.get("timestamp") or ""))


def _min_max_norm(values: List[float]) -> List[float]:
    if not values:
        return []
    lo = min(values)
    hi = max(values)
    span = hi - lo
    if span <= 1e-12:
        return [0.5 for _ in values]
    return [(v - lo) / span for v in values]


def _score_one_symbol(
    upstox: UpstoxService,
    stock: str,
    instrument_key: str,
) -> Optional[Dict[str, Any]]:
    """Return raw metrics or None if data insufficient."""
    ikey = (instrument_key or "").strip()
    if not ikey:
        return None
    try:
        daily_raw = upstox.get_historical_candles_by_instrument_key(
            ikey, interval="days/1", days_back=80
        )
        daily = _sort_candles(daily_raw)
        if len(daily) < 22:
            return None

        # Last 10 days for OBV slope (oldest → newest)
        tail = daily[-10:]
        closes = [float(x["close"]) for x in tail]
        vols = [float(x.get("volume") or 0) for x in tail]
        obv_slope = compute_obv_slope_daily(closes, vols)

        prev_bar = daily[-2]
        last_bar = daily[-1]
        prev_close = float(prev_bar["close"])
        if prev_close <= 0:
            return None

        q = upstox.get_market_quote_by_key(ikey) or {}
        ohlc = q.get("ohlc") if isinstance(q.get("ohlc"), dict) else {}
        day_open = float(ohlc.get("open") or 0)
        if day_open <= 0:
            day_open = float(last_bar.get("open") or 0)
        ltp = float(q.get("last_price") or 0)
        if ltp <= 0:
            ltp = float(last_bar.get("close") or 0)

        gap_pct = (day_open - prev_close) / prev_close * 100.0
        gap_strength = abs(gap_pct)

        slice20 = daily[-20:]
        hi = max(float(x["high"]) for x in slice20)
        lo = min(float(x["low"]) for x in slice20)
        range_pos = (ltp - lo) / (hi - lo + 1e-12)
        range_pos = max(0.0, min(1.0, float(range_pos)))

        return {
            "stock": stock.upper().strip(),
            "instrument_key": ikey,
            "obv_slope": float(obv_slope),
            "gap_strength": float(gap_strength),
            "range_position": float(range_pos),
            "gap_pct_signed": float(gap_pct),
            "ltp": float(ltp),
        }
    except Exception as e:
        logger.debug("premarket_watchlist skip %s: %s", stock, e)
        return None


def _persist_rows(db: Session, session_date: date, rows: List[Dict[str, Any]], computed_at: datetime) -> None:
    db.execute(text("DELETE FROM premarket_watchlist WHERE session_date = :sd"), {"sd": session_date})
    for r in rows:
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
            {
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
            },
        )
    db.commit()


def run_premarket_watchlist_job() -> Dict[str, Any]:
    """
    Scan up to 200 F&O names from arbitrage_master, score, persist Top 10 for today (IST session date).
    """
    now_ist = datetime.now(IST)
    if now_ist.weekday() >= 5:
        return {"skipped": "weekend", "top": []}

    session_date = now_ist.date()
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
            {"lim": UNIVERSE_LIMIT},
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
        m = _score_one_symbol(upstox, st, str(ikey).strip())
        if m:
            raw_rows.append(m)
        time.sleep(SLEEP_BETWEEN_SYMBOLS_SEC)

    if len(raw_rows) < 5:
        logger.warning("premarket_watchlist: too few scored symbols (%s)", len(raw_rows))
        return {"error": "insufficient_data", "scored": len(raw_rows), "top": []}

    obv_list = [r["obv_slope"] for r in raw_rows]
    gap_list = [r["gap_strength"] for r in raw_rows]
    rng_list = [r["range_position"] for r in raw_rows]

    obv_n = _min_max_norm(obv_list)
    gap_n = _min_max_norm(gap_list)
    rng_n = _min_max_norm(rng_list)

    scored: List[Dict[str, Any]] = []
    for i, r in enumerate(raw_rows):
        comp = (obv_n[i] + gap_n[i] + rng_n[i]) / 3.0
        scored.append(
            {
                **r,
                "obv_norm": obv_n[i],
                "gap_norm": gap_n[i],
                "range_norm": rng_n[i],
                "composite_score": comp,
            }
        )

    scored.sort(key=lambda x: float(x["composite_score"]), reverse=True)
    top = scored[:TOP_N]
    computed_at = datetime.now(IST)
    for idx, row in enumerate(top, start=1):
        row["rank"] = idx

    dbw = SessionLocal()
    try:
        _persist_rows(dbw, session_date, top, computed_at)
    except Exception as e:
        logger.exception("premarket_watchlist: persist failed: %s", e)
        dbw.rollback()
        return {"error": str(e), "top": []}
    finally:
        dbw.close()

    logger.info(
        "premarket_watchlist: session_date=%s saved_top=%s first=%s",
        session_date,
        len(top),
        top[0]["stock"] if top else None,
    )
    return {
        "session_date": session_date.isoformat(),
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

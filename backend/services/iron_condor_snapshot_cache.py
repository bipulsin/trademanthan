"""
Per-day broker-backed cache for Iron Condor: India VIX, monthly ATR inputs, equity daily closes.

Populated by a pre-market scheduled job (and optional authenticated refresh). Read paths fall back
to live Upstox if cache rows are missing.
"""
from __future__ import annotations

import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple

from sqlalchemy import text
from sqlalchemy.orm import Session

from backend.database import engine
from backend.services.upstox_service import UpstoxService
from backend.config import settings
from backend.services.market_holiday import IST
from backend.services import market_holiday as mh

logger = logging.getLogger(__name__)


def ensure_iron_condor_snapshot_tables() -> None:
    if engine is None:
        return
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS iron_condor_daily_global_cache (
                    trade_date DATE PRIMARY KEY,
                    india_vix NUMERIC(14, 6),
                    fetched_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS iron_condor_daily_underlying_cache (
                    trade_date DATE NOT NULL,
                    underlying VARCHAR(32) NOT NULL,
                    monthly_atr_14 NUMERIC(18, 6),
                    monthly_candles_json JSONB,
                    daily_closes_json JSONB,
                    fetched_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (trade_date, underlying)
                );
                CREATE INDEX IF NOT EXISTS idx_ic_daily_und_trade ON iron_condor_daily_underlying_cache(trade_date);
                """
            )
        )


def _ist_today() -> date:
    return mh._normalize_ist(None).date()


def _row_vix(row: Any) -> Optional[float]:
    if not row or row[0] is None:
        return None
    try:
        v = float(row[0])
        return v if v > 0 else None
    except (TypeError, ValueError):
        return None


def get_cached_india_vix(conn: Any, trade_date: date) -> Optional[float]:
    row = conn.execute(
        text("SELECT india_vix FROM iron_condor_daily_global_cache WHERE trade_date = CAST(:d AS DATE)"),
        {"d": str(trade_date)},
    ).fetchone()
    return _row_vix(row)


def read_india_vix_session(db: Session, trade_date: date) -> Optional[float]:
    """Session-based read for India VIX snapshot."""
    row = db.execute(
        text("SELECT india_vix FROM iron_condor_daily_global_cache WHERE trade_date = CAST(:d AS DATE)"),
        {"d": str(trade_date)},
    ).fetchone()
    return _row_vix(row)


def _parse_closes_json(raw: Any) -> Optional[List[float]]:
    if raw is None:
        return None
    try:
        if isinstance(raw, str):
            data = json.loads(raw)
        else:
            data = raw
        if isinstance(data, list):
            return [float(x) for x in data if x is not None]
    except (TypeError, ValueError, json.JSONDecodeError):
        pass
    return None


def read_underlying_atr_closes_session(db: Session, trade_date: date, underlying: str) -> Tuple[Optional[float], Optional[List[float]]]:
    u = (underlying or "").strip().upper()
    row = db.execute(
        text(
            """
            SELECT monthly_atr_14, daily_closes_json
            FROM iron_condor_daily_underlying_cache
            WHERE trade_date = CAST(:d AS DATE) AND UPPER(underlying) = UPPER(:u)
            """
        ),
        {"d": str(trade_date), "u": u},
    ).fetchone()
    if not row:
        return None, None
    atr_v: Optional[float] = None
    try:
        if row[0] is not None:
            atr_v = float(row[0])
            if atr_v <= 0:
                atr_v = None
    except (TypeError, ValueError):
        atr_v = None
    closes = _parse_closes_json(row[1])
    return atr_v, closes


def get_cached_underlying_bundle(
    conn: Any, trade_date: date, underlying: str
) -> Tuple[Optional[float], Optional[List[float]]]:
    """Returns (monthly_atr_14, daily_close_history newest-last) if present."""
    u = (underlying or "").strip().upper()
    row = conn.execute(
        text(
            """
            SELECT monthly_atr_14, daily_closes_json
            FROM iron_condor_daily_underlying_cache
            WHERE trade_date = CAST(:d AS DATE) AND UPPER(underlying) = UPPER(:u)
            """
        ),
        {"d": str(trade_date), "u": u},
    ).fetchone()
    if not row:
        return None, None
    atr_v: Optional[float] = None
    try:
        if row[0] is not None:
            atr_v = float(row[0])
            if atr_v <= 0:
                atr_v = None
    except (TypeError, ValueError):
        atr_v = None
    closes: Optional[List[float]] = None
    raw = row[1]
    if raw is not None:
        try:
            if isinstance(raw, str):
                data = json.loads(raw)
            else:
                data = raw
            if isinstance(data, list):
                closes = [float(x) for x in data if x is not None]
        except (TypeError, ValueError, json.JSONDecodeError):
            closes = None
    return atr_v, closes


def upsert_global_vix(sess: Session, trade_date: date, vix: float) -> None:
    sess.execute(
        text(
            """
            INSERT INTO iron_condor_daily_global_cache (trade_date, india_vix, fetched_at)
            VALUES (CAST(:d AS DATE), :v, CURRENT_TIMESTAMP)
            ON CONFLICT (trade_date) DO UPDATE SET
                india_vix = EXCLUDED.india_vix,
                fetched_at = CURRENT_TIMESTAMP
            """
        ),
        {"d": str(trade_date), "v": float(vix)},
    )


def upsert_underlying_cache(
    sess: Session,
    trade_date: date,
    underlying: str,
    *,
    monthly_atr_14: Optional[float],
    monthly_candles_json: Optional[List[Dict[str, Any]]],
    daily_closes: Optional[List[float]],
) -> None:
    mc = json.dumps(monthly_candles_json if monthly_candles_json is not None else [])
    dc = json.dumps(daily_closes if daily_closes is not None else [])
    sess.execute(
        text(
            """
            INSERT INTO iron_condor_daily_underlying_cache (
                trade_date, underlying, monthly_atr_14, monthly_candles_json, daily_closes_json, fetched_at
            ) VALUES (
                CAST(:d AS DATE), :u, :atr, CAST(:mj AS JSONB), CAST(:dj AS JSONB), CURRENT_TIMESTAMP
            )
            ON CONFLICT (trade_date, underlying) DO UPDATE SET
                monthly_atr_14 = EXCLUDED.monthly_atr_14,
                monthly_candles_json = EXCLUDED.monthly_candles_json,
                daily_closes_json = EXCLUDED.daily_closes_json,
                fetched_at = CURRENT_TIMESTAMP
            """
        ),
        {
            "d": str(trade_date),
            "u": (underlying or "").strip().upper(),
            "atr": monthly_atr_14,
            "mj": mc,
            "dj": dc,
        },
    )


def _fetch_one_underlying(svc: UpstoxService, und: str) -> Tuple[str, Optional[float], List[Dict[str, Any]], List[float]]:
    from backend.services.iron_condor_service import _monthly_atr_wilder_14, option_chain_underlying

    api_sym = option_chain_underlying(und)
    eq_key = svc.get_instrument_key(api_sym)
    if not eq_key:
        return und, None, [], []
    months = svc.get_monthly_candles_by_instrument_key(eq_key, months_back=48) or []
    atr = _monthly_atr_wilder_14(months)
    candles = (
        svc.get_historical_candles_by_instrument_key(eq_key, interval="days/1", days_back=400) or []
    )
    bars = sorted([dict(x) for x in candles if isinstance(x, dict)], key=lambda c: str(c.get("timestamp") or ""))
    closes: List[float] = []
    for c in bars:
        try:
            lp = float(c.get("close") or c.get("last_price") or 0)
            if lp > 0:
                closes.append(lp)
        except (TypeError, ValueError):
            continue
    return und, atr, months, closes


def _fetch_india_vix(svc: UpstoxService) -> Optional[float]:
    try:
        q = svc.get_market_quote_by_key(svc.INDIA_VIX_KEY) or {}
        v = float(q.get("last_price") or q.get("ltp") or 0)
        return v if v > 0 else None
    except Exception as e:
        logger.warning("iron_condor snapshot: VIX fetch failed: %s", e)
        return None


def run_iron_condor_daily_snapshot_job(*, symbols: Optional[Sequence[str]] = None, max_workers: int = 6) -> Dict[str, Any]:
    """
    Fetch India VIX + monthly ATR equity history + daily closes for Iron Condor universe; upsert rows for IST today.
    Safe to call multiple times (refreshes same trade_date rows).
    """
    ensure_iron_condor_snapshot_tables()
    td = _ist_today()
    from backend.services.iron_condor_service import ic_universe_symbol_list_ordered

    sym_default = ic_universe_symbol_list_ordered()
    syms = [s.strip().upper() for s in (symbols or sym_default) if s.strip()]
    if not syms:
        return {"ok": False, "error": "no symbols"}

    try:
        svc = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    except Exception as e:
        logger.error("iron_condor snapshot: Upstox init failed: %s", e)
        return {"ok": False, "error": str(e)}

    vix_val: Optional[float] = None
    results: Dict[str, Any] = {"trade_date": str(td), "vix": None, "underlyings": {}}

    und_payloads: Dict[str, Tuple[Optional[float], List[Dict[str, Any]], List[float]]] = {}

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futs_u = {pool.submit(_fetch_one_underlying, svc, u): u for u in syms}
        vix_future = pool.submit(_fetch_india_vix, svc)
        for fut in as_completed(futs_u):
            sym = futs_u[fut]
            try:
                _und, atr, months, closes = fut.result()
                und_payloads[sym] = (atr, months, closes)
            except Exception as e:
                logger.warning("iron_condor snapshot: %s failed: %s", sym, e)
                und_payloads[sym] = (None, [], [])
        try:
            vix_val = vix_future.result()
        except Exception as e:
            logger.warning("iron_condor snapshot: VIX future failed: %s", e)

    results["vix"] = vix_val

    if engine is None:
        return {"ok": False, "error": "no database engine"}

    from backend.database import SessionLocal

    if SessionLocal is None:
        return {"ok": False, "error": "no SessionLocal"}

    db = SessionLocal()
    try:
        if vix_val is not None and vix_val > 0:
            upsert_global_vix(db, td, float(vix_val))
        for u in syms:
            atr, months, closes = und_payloads.get(u, (None, [], []))
            upsert_underlying_cache(
                db,
                td,
                u,
                monthly_atr_14=atr,
                monthly_candles_json=months or None,
                daily_closes=closes or None,
            )
            results["underlyings"][u] = {
                "monthly_atr_14": atr,
                "daily_closes_n": len(closes),
            }
        try:
            from backend.services.iron_condor_service import iron_condor_universe_master_update_previous_day_closes_from_upstox

            results["universe_master_previous_close"] = iron_condor_universe_master_update_previous_day_closes_from_upstox(
                db, svc, only_if_null_previous_close=False, commit=False
            )
        except Exception as e:
            logger.warning("iron_condor snapshot: universe previous_close refresh failed: %s", e)
        db.commit()
    except Exception as e:
        db.rollback()
        logger.error("iron_condor snapshot: DB commit failed: %s", e, exc_info=True)
        return {"ok": False, "error": str(e)}
    finally:
        db.close()

    return {"ok": True, **results}

"""Fetch NSE_EQ dailies, compute Analysis snapshot, persist analysis_symbol_snapshot."""
from __future__ import annotations

import json
import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple

from sqlalchemy import text

from backend.config import settings
from backend.database import SessionLocal, engine
from backend.services.analysis_page.compute import (
    classify_action,
    classify_trend,
    daily_to_weekly,
    detect_patterns,
    rs_vs_ma50,
    weekly_rsi_zone,
)
from backend.services.market_holiday import IST

logger = logging.getLogger(__name__)

_DAILY_DAYS_BACK = 180
_WEEKLY_DAYS_BACK = 220
_SLEEP_SEC = 0.15
_TRANCHE = 25
_TRANCHE_PAUSE = 2.0

_ENSURE_SQL = """
CREATE TABLE IF NOT EXISTS analysis_symbol_snapshot (
    symbol TEXT PRIMARY KEY,
    sector TEXT,
    sector_index TEXT,
    weekly_trend TEXT,
    daily_trend TEXT,
    weekly_rsi DOUBLE PRECISION,
    weekly_rsi_zone TEXT,
    rs_ratio DOUBLE PRECISION,
    rs_sma50 DOUBLE PRECISION,
    rs_ma50 TEXT,
    patterns JSONB NOT NULL DEFAULT '[]'::jsonb,
    fail_reason TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_analysis_symbol_snapshot_sector
    ON analysis_symbol_snapshot (sector);
"""

_INSERT_COLS = """
    INSERT INTO analysis_symbol_snapshot (
        symbol, sector, sector_index,
        weekly_trend, daily_trend, weekly_rsi, weekly_rsi_zone,
        rs_ratio, rs_sma50, rs_ma50, patterns, fail_reason, updated_at
    ) VALUES (
        :symbol, :sector, :sector_index,
        :weekly_trend, :daily_trend, :weekly_rsi, :weekly_rsi_zone,
        :rs_ratio, :rs_sma50, :rs_ma50, CAST(:patterns AS JSONB), :fail_reason, NOW()
    )
"""

_UPSERT_FULL = text(
    _INSERT_COLS
    + """
    ON CONFLICT (symbol) DO UPDATE SET
        sector = EXCLUDED.sector,
        sector_index = EXCLUDED.sector_index,
        weekly_trend = EXCLUDED.weekly_trend,
        daily_trend = EXCLUDED.daily_trend,
        weekly_rsi = EXCLUDED.weekly_rsi,
        weekly_rsi_zone = EXCLUDED.weekly_rsi_zone,
        rs_ratio = EXCLUDED.rs_ratio,
        rs_sma50 = EXCLUDED.rs_sma50,
        rs_ma50 = EXCLUDED.rs_ma50,
        patterns = EXCLUDED.patterns,
        fail_reason = EXCLUDED.fail_reason,
        updated_at = NOW()
    """
)

# Weekday daily-only: insert may bootstrap weekly_*; existing rows keep last Friday weekly_*.
_UPSERT_DAILY = text(
    _INSERT_COLS
    + """
    ON CONFLICT (symbol) DO UPDATE SET
        sector = EXCLUDED.sector,
        sector_index = EXCLUDED.sector_index,
        daily_trend = EXCLUDED.daily_trend,
        rs_ratio = EXCLUDED.rs_ratio,
        rs_sma50 = EXCLUDED.rs_sma50,
        rs_ma50 = EXCLUDED.rs_ma50,
        patterns = EXCLUDED.patterns,
        fail_reason = EXCLUDED.fail_reason,
        updated_at = NOW()
    """
)


def ensure_analysis_snapshot_table() -> None:
    if engine is None:
        return
    with engine.begin() as conn:
        for stmt in _ENSURE_SQL.split(";"):
            s = stmt.strip()
            if s:
                conn.execute(text(s))


def list_equity_universe() -> List[Dict[str, str]]:
    if SessionLocal is None:
        return []
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT UPPER(TRIM(stock)) AS symbol,
                       TRIM(COALESCE(sector, '')) AS sector,
                       TRIM(COALESCE(sector_index, '')) AS sector_index,
                       TRIM(COALESCE(sector_instrument_key, '')) AS sector_ikey,
                       TRIM(COALESCE(stock_instrument_key, '')) AS stock_ikey
                FROM arbitrage_master
                WHERE stock IS NOT NULL AND TRIM(stock) <> ''
                ORDER BY stock
                """
            )
        ).fetchall()
        out: List[Dict[str, str]] = []
        seen = set()
        for r in rows:
            sym = (r.symbol or "").strip().upper()
            if not sym or sym in seen:
                continue
            seen.add(sym)
            out.append(
                {
                    "symbol": sym,
                    "sector": (r.sector or "").strip(),
                    "sector_index": (r.sector_index or "").strip(),
                    "sector_ikey": (r.sector_ikey or "").strip(),
                    "stock_ikey": (r.stock_ikey or "").strip(),
                }
            )
        return out
    finally:
        db.close()


def _fetch_candles(upstox: Any, ikey: str, interval: str, days_back: int) -> List[Dict[str, Any]]:
    if not ikey:
        return []
    raw = upstox.get_historical_candles_by_instrument_key(
        ikey, interval=interval, days_back=days_back
    )
    return list(raw or [])


def should_refresh_weekly(*, now: Optional[datetime] = None) -> bool:
    """Friday IST: recompute weekly_* (candles, trend, RSI). Other weekdays keep last Friday values."""
    if now is None:
        now_ist = datetime.now(IST)
    elif now.tzinfo is None:
        now_ist = IST.localize(now)
    else:
        now_ist = now.astimezone(IST)
    return now_ist.weekday() == 4


def _prior_has_weekly(prior: Optional[Dict[str, Any]]) -> bool:
    if not prior:
        return False
    return any(
        prior.get(k) is not None
        for k in ("weekly_trend", "weekly_rsi", "weekly_rsi_zone")
    )


def compute_row(
    *,
    meta: Dict[str, str],
    daily: List[Dict[str, Any]],
    weekly: List[Dict[str, Any]],
    index_daily: List[Dict[str, Any]],
    refresh_weekly: bool = True,
    prior_weekly: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    ratio, sma, rs_lab = rs_vs_ma50(daily, index_daily)
    patterns = detect_patterns(daily)
    fail_parts = []
    if not daily:
        fail_parts.append("no_daily")
    if classify_trend(daily) is None and daily:
        fail_parts.append("daily_trend_short")

    use_prior = (not refresh_weekly) and _prior_has_weekly(prior_weekly)
    if use_prior and prior_weekly is not None:
        w_trend = prior_weekly.get("weekly_trend")
        w_rsi = prior_weekly.get("weekly_rsi")
        w_zone = prior_weekly.get("weekly_rsi_zone")
    else:
        if not weekly and daily:
            weekly = daily_to_weekly(daily)
        w_rsi, w_zone = weekly_rsi_zone(weekly)
        w_trend = classify_trend(weekly)
        if w_trend is None:
            fail_parts.append("weekly_trend_short")

    return {
        "symbol": meta["symbol"],
        "sector": meta.get("sector") or None,
        "sector_index": meta.get("sector_index") or None,
        "weekly_trend": w_trend,
        "daily_trend": classify_trend(daily),
        "weekly_rsi": w_rsi,
        "weekly_rsi_zone": w_zone,
        "rs_ratio": ratio,
        "rs_sma50": sma,
        "rs_ma50": rs_lab,
        "patterns": patterns,
        "fail_reason": ",".join(fail_parts) or None,
    }


def _load_prior_weekly(db, symbols: Sequence[str]) -> Dict[str, Dict[str, Any]]:
    want = {s.upper() for s in symbols}
    rows = db.execute(
        text(
            """
            SELECT symbol, weekly_trend, weekly_rsi, weekly_rsi_zone
            FROM analysis_symbol_snapshot
            """
        )
    ).fetchall()
    return {
        r.symbol: {
            "weekly_trend": r.weekly_trend,
            "weekly_rsi": r.weekly_rsi,
            "weekly_rsi_zone": r.weekly_rsi_zone,
        }
        for r in rows
        if (r.symbol or "").upper() in want
    }


def run_analysis_snapshot_job(
    *,
    trigger: str = "scheduled",
    symbols: Optional[Sequence[str]] = None,
    force: bool = False,
    refresh_weekly: Optional[bool] = None,
) -> Dict[str, Any]:
    from backend.services.breakfast_upstox_gate import (
        breakfast_exclusivity_active,
        defer_job_for_breakfast_exclusivity,
    )

    if not force and breakfast_exclusivity_active():
        defer_job_for_breakfast_exclusivity("analysis_symbol_snapshot")
        return {"ok": False, "skipped": "breakfast_exclusivity", "trigger": trigger}

    if refresh_weekly is None:
        refresh_weekly = should_refresh_weekly()

    ensure_analysis_snapshot_table()
    started = datetime.now(IST)
    universe = list_equity_universe()
    if symbols:
        want = {s.strip().upper() for s in symbols if str(s).strip()}
        universe = [u for u in universe if u["symbol"] in want]
    if not universe:
        return {"ok": False, "error": "empty_universe", "trigger": trigger}

    try:
        from backend.services.upstox_service import UpstoxService

        upstox = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    except Exception as exc:
        logger.error("analysis_snapshot: Upstox init failed: %s", exc)
        return {"ok": False, "error": str(exc), "trigger": trigger}

    index_cache: Dict[str, List[Dict[str, Any]]] = {}
    unique_idx = sorted({u["sector_ikey"] for u in universe if u.get("sector_ikey")})
    for i, ik in enumerate(unique_idx):
        try:
            index_cache[ik] = _fetch_candles(upstox, ik, "days/1", _DAILY_DAYS_BACK)
        except Exception as exc:
            logger.warning("analysis_snapshot: index fetch %s: %s", ik, exc)
            index_cache[ik] = []
        time.sleep(_SLEEP_SEC)
        if (i + 1) % _TRANCHE == 0:
            time.sleep(_TRANCHE_PAUSE)

    succeeded = 0
    failed: List[str] = []
    if SessionLocal is None:
        return {"ok": False, "error": "no SessionLocal", "trigger": trigger}
    db = SessionLocal()
    try:
        prior_map: Dict[str, Dict[str, Any]] = {}
        if not refresh_weekly:
            prior_map = _load_prior_weekly(db, [u["symbol"] for u in universe])
        upsert = _UPSERT_FULL if refresh_weekly else _UPSERT_DAILY
        for n, meta in enumerate(universe):
            if not force and breakfast_exclusivity_active():
                defer_job_for_breakfast_exclusivity("analysis_symbol_snapshot")
                db.commit()
                return {
                    "ok": False,
                    "skipped": "breakfast_exclusivity_mid_run",
                    "trigger": trigger,
                    "succeeded": succeeded,
                    "partial": True,
                }
            ik = meta.get("stock_ikey") or ""
            try:
                daily = _fetch_candles(upstox, ik, "days/1", _DAILY_DAYS_BACK) if ik else []
                time.sleep(_SLEEP_SEC)
                weekly: List[Dict[str, Any]] = []
                if refresh_weekly:
                    weekly = _fetch_candles(upstox, ik, "weeks/1", _WEEKLY_DAYS_BACK) if ik else []
                    time.sleep(_SLEEP_SEC)
            except Exception as exc:
                logger.warning("analysis_snapshot: fetch %s: %s", meta["symbol"], exc)
                daily, weekly = [], []
            idx_bars = index_cache.get(meta.get("sector_ikey") or "", [])
            row = compute_row(
                meta=meta,
                daily=daily,
                weekly=weekly,
                index_daily=idx_bars,
                refresh_weekly=refresh_weekly,
                prior_weekly=prior_map.get(meta["symbol"]),
            )
            db.execute(
                upsert,
                {
                    **row,
                    "patterns": json.dumps(row["patterns"]),
                },
            )
            if row.get("daily_trend") or row.get("weekly_trend"):
                succeeded += 1
            else:
                failed.append(meta["symbol"])
            if (n + 1) % _TRANCHE == 0:
                db.commit()
                time.sleep(_TRANCHE_PAUSE)
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()

    summary = {
        "ok": True,
        "trigger": trigger,
        "universe_n": len(universe),
        "succeeded": succeeded,
        "failed": len(failed),
        "failed_symbols": failed[:50],
        "index_keys": len(unique_idx),
        "refresh_weekly": refresh_weekly,
        "elapsed_sec": round((datetime.now(IST) - started).total_seconds(), 1),
    }
    logger.info("analysis_snapshot: %s", summary)
    return summary


def list_analysis_rows() -> Tuple[List[Dict[str, Any]], Optional[str]]:
    ensure_analysis_snapshot_table()
    if SessionLocal is None:
        return [], None
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT symbol, sector, sector_index, weekly_trend, daily_trend,
                       weekly_rsi, weekly_rsi_zone, rs_ma50, patterns, fail_reason,
                       updated_at
                FROM analysis_symbol_snapshot
                ORDER BY symbol
                """
            )
        ).fetchall()
        out = []
        latest = None
        for r in rows:
            pats = r.patterns
            if isinstance(pats, str):
                try:
                    pats = json.loads(pats)
                except json.JSONDecodeError:
                    pats = []
            if pats is None:
                pats = []
            ua = r.updated_at
            ua_s = ua.isoformat() if hasattr(ua, "isoformat") else (str(ua) if ua else None)
            if ua_s and (latest is None or ua_s > latest):
                latest = ua_s
            pats_list = list(pats)
            out.append(
                {
                    "symbol": r.symbol,
                    "sector": r.sector,
                    "weekly_trend": r.weekly_trend,
                    "daily_trend": r.daily_trend,
                    "weekly_rsi_zone": r.weekly_rsi_zone,
                    "rs_ma50": r.rs_ma50,
                    "patterns": pats_list,
                    "action": classify_action(
                        r.weekly_trend,
                        r.daily_trend,
                        r.weekly_rsi_zone,
                        r.rs_ma50,
                        pats_list,
                    ),
                    "updated_at": ua_s,
                }
            )
        return out, latest
    finally:
        db.close()

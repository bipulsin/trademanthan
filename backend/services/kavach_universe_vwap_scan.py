"""Full-universe VWAP slope sweep — research / shadow only.

Every ~5 minutes during RTH, score ``vwap_slope_score`` + ``vwap_extension_pct``
for the ~200 F&O universe using the shared market-data candle cache (no per-symbol
Upstox storm). Independent of READY / lock promotion. Live gate logic untouched.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, time, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple

import pytz
from sqlalchemy import text

from backend.database import SessionLocal, engine
from backend.services.rs_vwap_quality import (
    score_vwap_quality,
    signed_vwap_slope_atr,
    vwap_extension_pct,
)

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

_ENSURED = False

# Soft guard: if a single sweep would exceed this, still write but flag in meta.
ROW_WARN_THRESHOLD = 50_000


def ensure_universe_vwap_scan() -> None:
    global _ENSURED
    if _ENSURED:
        return
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS kavach_universe_vwap_scan (
                    id SERIAL PRIMARY KEY,
                    session_date DATE NOT NULL,
                    symbol VARCHAR(32) NOT NULL,
                    direction VARCHAR(8),
                    vwap_slope_score NUMERIC(12,4),
                    steep_ok BOOLEAN,
                    vwap_extension_pct NUMERIC(12,6),
                    in_lock_at_time BOOLEAN NOT NULL DEFAULT FALSE,
                    source VARCHAR(16) NOT NULL DEFAULT 'live',
                    logged_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS idx_universe_vwap_scan_session
                ON kavach_universe_vwap_scan (session_date, logged_at, symbol)
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS idx_universe_vwap_scan_steep
                ON kavach_universe_vwap_scan (session_date, steep_ok)
                WHERE steep_ok = TRUE
                """
            )
        )
    _ENSURED = True


def _session_date_ist(now: Optional[datetime] = None) -> str:
    n = now or datetime.now(IST)
    if n.tzinfo is None:
        n = IST.localize(n)
    return n.astimezone(IST).strftime("%Y-%m-%d")


def _parse_candle_ts(val: Any) -> Optional[datetime]:
    if val is None:
        return None
    if isinstance(val, datetime):
        dt = val
    else:
        try:
            dt = datetime.fromisoformat(str(val).replace("Z", "+00:00"))
        except Exception:
            return None
    if dt.tzinfo is None:
        return IST.localize(dt)
    return dt.astimezone(IST)


def _truncate_candles(candles: List[Dict[str, Any]], as_of: datetime) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for c in candles:
        ts = _parse_candle_ts(c.get("timestamp"))
        if ts is None or ts <= as_of:
            out.append(c)
    return out


def _direction_from_slope(signed: float) -> str:
    return "SHORT" if signed < 0 else "LONG"


def _score_row(
    candles: List[Dict[str, Any]],
    *,
    atr_pct: float,
    in_lock: bool,
    session_date: str,
    symbol: str,
    source: str,
    logged_at: Optional[datetime] = None,
) -> Optional[Dict[str, Any]]:
    if not candles or len(candles) < 20:
        return None
    atr = atr_pct if atr_pct > 0 else 1.0
    signed = float(signed_vwap_slope_atr(candles, atr))
    direction = _direction_from_slope(signed)
    vq = score_vwap_quality(candles, side=direction, atr_daily_pct=atr)
    ext = vwap_extension_pct(candles)
    row = {
        "session_date": session_date,
        "symbol": symbol.upper(),
        "direction": direction,
        "vwap_slope_score": vq.get("slope_score"),
        "steep_ok": bool(vq.get("steep_ok")),
        "vwap_extension_pct": ext,
        "in_lock_at_time": bool(in_lock),
        "source": source,
    }
    if logged_at is not None:
        row["logged_at"] = logged_at
    return row


def insert_scan_rows(db, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    ensure_universe_vwap_scan()
    n = 0
    for r in rows:
        params = {
            "d": r.get("session_date"),
            "sym": r.get("symbol"),
            "dir": r.get("direction"),
            "vs": r.get("vwap_slope_score"),
            "so": r.get("steep_ok"),
            "ext": r.get("vwap_extension_pct"),
            "il": bool(r.get("in_lock_at_time")),
            "src": r.get("source") or "live",
        }
        if r.get("logged_at") is not None:
            db.execute(
                text(
                    """
                    INSERT INTO kavach_universe_vwap_scan (
                        session_date, symbol, direction,
                        vwap_slope_score, steep_ok, vwap_extension_pct,
                        in_lock_at_time, source, logged_at
                    ) VALUES (
                        CAST(:d AS date), :sym, :dir,
                        :vs, :so, :ext,
                        :il, :src, :lat
                    )
                    """
                ),
                {**params, "lat": r["logged_at"]},
            )
        else:
            db.execute(
                text(
                    """
                    INSERT INTO kavach_universe_vwap_scan (
                        session_date, symbol, direction,
                        vwap_slope_score, steep_ok, vwap_extension_pct,
                        in_lock_at_time, source
                    ) VALUES (
                        CAST(:d AS date), :sym, :dir,
                        :vs, :so, :ext,
                        :il, :src
                    )
                    """
                ),
                params,
            )
        n += 1
    db.commit()
    return n


def _universe_keys(db) -> List[Tuple[str, str]]:
    """Return [(symbol, instrument_key), ...] from arbitrage_master."""
    rows = db.execute(
        text(
            """
            SELECT UPPER(stock) AS symbol, currmth_future_instrument_key AS ik
            FROM arbitrage_master
            WHERE stock IS NOT NULL
              AND currmth_future_instrument_key IS NOT NULL
              AND TRIM(currmth_future_instrument_key) <> ''
            ORDER BY 1
            """
        )
    ).fetchall()
    out: List[Tuple[str, str]] = []
    for r in rows:
        sym = str(r.symbol or "").upper()
        ik = str(r.ik or "").strip()
        if sym and ik:
            out.append((sym, ik))
    return out


def _atr_map(db, symbols: List[str]) -> Dict[str, float]:
    if not symbols:
        return {}
    rows = db.execute(
        text(
            """
            SELECT UPPER(symbol) AS symbol, atr14_pct
            FROM rs_scanner_history
            WHERE date = CURRENT_DATE AND UPPER(symbol) = ANY(:syms)
            """
        ),
        {"syms": symbols},
    ).fetchall()
    return {
        str(r.symbol).upper(): float(r.atr14_pct)
        for r in rows
        if r.atr14_pct is not None
    }


def _lock_set(db, session_date: str) -> Set[str]:
    from backend.services.daily_checklist_zones import morning_locked_symbols

    return set(morning_locked_symbols(db, session_date).keys())


def run_live_universe_vwap_scan(*, force: bool = False) -> Dict[str, Any]:
    """One RTH sweep: cache-only candles for full F&O universe. Shadow-only."""
    now = datetime.now(IST)
    if not force:
        t = now.time()
        if now.weekday() >= 5 or t < time(9, 20) or t > time(15, 30):
            return {"ok": True, "skipped": True, "reason": "outside_rth"}

    from backend.services.rs_conviction_candles import candles_cache_only

    ensure_universe_vwap_scan()
    session_date = _session_date_ist(now)
    db = SessionLocal()
    try:
        universe = _universe_keys(db)
        lock_syms = _lock_set(db, session_date)
        atrs = _atr_map(db, [s for s, _ in universe])
        rows: List[Dict[str, Any]] = []
        cache_hits = 0
        cache_miss = 0
        for sym, ik in universe:
            candles = candles_cache_only(ik)
            if not candles:
                cache_miss += 1
                continue
            cache_hits += 1
            scored = _score_row(
                candles,
                atr_pct=atrs.get(sym, 1.0),
                in_lock=sym in lock_syms,
                session_date=session_date,
                symbol=sym,
                source="live",
            )
            if scored:
                rows.append(scored)
        n = insert_scan_rows(db, rows)
        steep_n = sum(1 for r in rows if r.get("steep_ok"))
        steep_out = sum(
            1 for r in rows if r.get("steep_ok") and not r.get("in_lock_at_time")
        )
        meta = {
            "ok": True,
            "skipped": False,
            "session_date": session_date,
            "universe": len(universe),
            "scored": n,
            "cache_hits": cache_hits,
            "cache_miss": cache_miss,
            "steep_ok": steep_n,
            "steep_ok_not_in_lock": steep_out,
            "source": "live",
        }
        logger.info(
            "universe VWAP scan: scored=%s/%s cache_miss=%s steep=%s steep_out_of_lock=%s",
            n,
            len(universe),
            cache_miss,
            steep_n,
            steep_out,
        )
        return meta
    except Exception as exc:
        logger.warning("universe VWAP scan failed: %s", exc)
        try:
            db.rollback()
        except Exception:
            pass
        return {"ok": False, "error": str(exc)}
    finally:
        db.close()


def _rth_5m_timestamps(session_date: date) -> List[datetime]:
    """5m bar ends from 09:20 through 15:25 IST inclusive."""
    out: List[datetime] = []
    cur = IST.localize(datetime.combine(session_date, time(9, 20)))
    end = IST.localize(datetime.combine(session_date, time(15, 25)))
    while cur <= end:
        out.append(cur)
        cur += timedelta(minutes=5)
    return out


def _lock_membership_timeline(
    db, session_date: str
) -> List[Tuple[datetime, Set[str]]]:
    """Build (event_at, locked_set) snapshots from rs_lock_membership_audit."""
    try:
        rows = db.execute(
            text(
                """
                SELECT UPPER(symbol) AS symbol, event_type,
                       event_at AT TIME ZONE 'Asia/Kolkata' AS event_at
                FROM rs_lock_membership_audit
                WHERE session_date = CAST(:d AS date)
                ORDER BY event_at ASC, id ASC
                """
            ),
            {"d": session_date},
        ).fetchall()
    except Exception:
        return []
    locked: Set[str] = set()
    snaps: List[Tuple[datetime, Set[str]]] = []
    for r in rows:
        et = r.event_at
        if isinstance(et, datetime) and et.tzinfo is None:
            et = IST.localize(et)
        sym = str(r.symbol or "").upper()
        ev = (r.event_type or "").lower()
        if ev == "entry":
            locked.add(sym)
        elif ev == "remove":
            locked.discard(sym)
        snaps.append((et, set(locked)))
    return snaps


def _locked_at(timeline: List[Tuple[datetime, Set[str]]], as_of: datetime) -> Set[str]:
    cur: Set[str] = set()
    for ts, s in timeline:
        if ts <= as_of:
            cur = s
        else:
            break
    return cur


def backfill_universe_vwap_scan(
    session_dates: List[str],
    *,
    pace_sec: float = 0.25,
) -> Dict[str, Any]:
    """Historical 5m sweeps via Upstox candles (range_end_date). Shadow-only.

    One historical fetch per symbol per day; slope/extension derived locally at
    each RTH 5m timestamp. Prefer this over TradingView.
    """
    import time as time_mod

    from backend.config import settings
    from backend.services.upstox_service import UpstoxService

    ensure_universe_vwap_scan()
    upstox = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    summary: Dict[str, Any] = {"ok": True, "days": {}, "source": "backfill"}

    db = SessionLocal()
    try:
        universe = _universe_keys(db)
        for sd in session_dates:
            d = date.fromisoformat(sd)
            stamps = _rth_5m_timestamps(d)
            timeline = _lock_membership_timeline(db, sd)
            # Fallback: EOD snapshot membership if no audit trail
            eod_lock = _lock_set(db, sd)
            atrs = _atr_map_for_date(db, sd, [s for s, _ in universe])
            day_rows: List[Dict[str, Any]] = []
            fetched = 0
            failed = 0
            for sym, ik in universe:
                try:
                    candles = upstox.get_historical_candles_by_instrument_key(
                        ik,
                        interval="minutes/5",
                        days_back=3,
                        range_end_date=d,
                    )
                except Exception as exc:
                    logger.debug("backfill fetch %s %s failed: %s", sd, sym, exc)
                    candles = None
                if pace_sec > 0:
                    time_mod.sleep(pace_sec)
                if not candles or len(candles) < 20:
                    failed += 1
                    continue
                fetched += 1
                atr = atrs.get(sym, 1.0)
                for as_of in stamps:
                    sliced = _truncate_candles(candles, as_of)
                    lock_set = (
                        _locked_at(timeline, as_of) if timeline else eod_lock
                    )
                    scored = _score_row(
                        sliced,
                        atr_pct=atr,
                        in_lock=sym in lock_set,
                        session_date=sd,
                        symbol=sym,
                        source="backfill",
                        logged_at=as_of,
                    )
                    if scored:
                        day_rows.append(scored)
            # Clear prior backfill for this day to allow re-run
            db.execute(
                text(
                    """
                    DELETE FROM kavach_universe_vwap_scan
                    WHERE session_date = CAST(:d AS date) AND source = 'backfill'
                    """
                ),
                {"d": sd},
            )
            db.commit()
            n = insert_scan_rows(db, day_rows)
            summary["days"][sd] = {
                "rows": n,
                "symbols_fetched": fetched,
                "symbols_failed": failed,
                "timestamps": len(stamps),
                "warn_large": n >= ROW_WARN_THRESHOLD,
            }
            logger.info(
                "universe VWAP backfill %s: rows=%s fetched=%s failed=%s",
                sd,
                n,
                fetched,
                failed,
            )
        return summary
    except Exception as exc:
        logger.warning("universe VWAP backfill failed: %s", exc)
        try:
            db.rollback()
        except Exception:
            pass
        return {"ok": False, "error": str(exc)}
    finally:
        db.close()


def _atr_map_for_date(db, session_date: str, symbols: List[str]) -> Dict[str, float]:
    if not symbols:
        return {}
    rows = db.execute(
        text(
            """
            SELECT UPPER(symbol) AS symbol, atr14_pct
            FROM rs_scanner_history
            WHERE date = CAST(:d AS date) AND UPPER(symbol) = ANY(:syms)
            """
        ),
        {"d": session_date, "syms": symbols},
    ).fetchall()
    out = {
        str(r.symbol).upper(): float(r.atr14_pct)
        for r in rows
        if r.atr14_pct is not None
    }
    if out:
        return out
    # Fall back to any recent atr for the symbol
    rows2 = db.execute(
        text(
            """
            SELECT DISTINCT ON (UPPER(symbol))
                   UPPER(symbol) AS symbol, atr14_pct
            FROM rs_scanner_history
            WHERE UPPER(symbol) = ANY(:syms) AND atr14_pct IS NOT NULL
            ORDER BY UPPER(symbol), date DESC
            """
        ),
        {"syms": symbols},
    ).fetchall()
    return {
        str(r.symbol).upper(): float(r.atr14_pct)
        for r in rows2
        if r.atr14_pct is not None
    }

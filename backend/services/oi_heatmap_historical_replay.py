"""
Replay OI heatmap from Upstox historical candles (1m / 5m fallback) and persist to ``oi_heatmap_latest``.

Used to seed the dashboard when live batch quotes are unavailable; same row shape as
``refresh_oi_heatmap_live`` for ``GET /scan/dashboard/oi-heatmap``.
"""
from __future__ import annotations

import logging
import time
from datetime import date, datetime, time as dt_time
from typing import Any, Dict, List, Optional, Tuple

import pytz
from sqlalchemy import text

from backend.config import settings
from backend.database import SessionLocal
from backend.services.oi_heatmap import (
    _interpret_signal,
    _persist_snapshot,
    _score_row,
    finalize_heatmap_rows_for_store,
    load_nse_instruments_json,
    replace_cache_with_rows,
)
from backend.services.premarket_scoring import parse_candle_date_ist
from backend.services.upstox_service import UpstoxService

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")


def _parse_dt_ist(ts: Any) -> Optional[datetime]:
    if ts is None:
        return None
    s = str(ts).strip()
    try:
        if "T" in s:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = IST.localize(dt)
            else:
                dt = dt.astimezone(IST)
            return dt
        return None
    except ValueError:
        return None


def _sort_candles(candles: Optional[List[dict]]) -> List[dict]:
    if not candles:
        return []
    return sorted(candles, key=lambda c: str(c.get("timestamp") or ""))


def _find_candle_at_minute(
    candles: List[dict], session_d: date, hour: int, minute: int
) -> Optional[dict]:
    for c in candles:
        dt = _parse_dt_ist(c.get("timestamp"))
        if not dt or dt.date() != session_d:
            continue
        if dt.hour == hour and dt.minute == minute:
            return c
    return None


def _fetch_candles_for_day(
    ux: UpstoxService, instrument_key: str, session_d: date, interval: str
) -> Optional[List[Dict[str, Any]]]:
    raw = ux.get_historical_candles_by_instrument_key(
        instrument_key, interval=interval, days_back=0, range_end_date=session_d
    )
    if raw:
        return raw
    return ux.get_historical_candles_by_instrument_key(
        instrument_key, interval=interval, days_back=1, range_end_date=session_d
    )


def _candles_for_session_date(candles: List[dict], session_d: date) -> List[dict]:
    out = [c for c in candles if parse_candle_date_ist(c.get("timestamp")) == session_d]
    return _sort_candles(out)


def load_arbitrage_master_fut_universe(limit: int) -> List[Tuple[str, str, str, Any]]:
    """Return list of (currmth_future_instrument_key, stock, trading_symbol, expiry_hint)."""
    out: List[Tuple[str, str, str, Any]] = []
    if limit <= 0:
        return out
    db = SessionLocal()
    try:
        q = db.execute(
            text(
                """
                SELECT stock, currmth_future_instrument_key
                FROM arbitrage_master
                WHERE currmth_future_instrument_key IS NOT NULL
                  AND TRIM(currmth_future_instrument_key) <> ''
                ORDER BY stock
                LIMIT :lim
                """
            ),
            {"lim": int(limit)},
        ).fetchall()
        raw_inst = load_nse_instruments_json()
        ik_meta = {((r.get("instrument_key") or "").strip()): r for r in raw_inst if isinstance(r, dict)}
        for stock, ik in q:
            st = str(stock or "").strip().upper()
            ikey = str(ik or "").strip()
            if not st or not ikey:
                continue
            meta = ik_meta.get(ikey) or {}
            tsym = (meta.get("trading_symbol") or meta.get("tradingsymbol") or "").strip()
            exp = meta.get("expiry")
            out.append((ikey, st, tsym, exp))
    except Exception as e:
        logger.error("oi_heatmap_historical_replay: universe query failed: %s", e)
    finally:
        db.close()
    return out


def _row_from_candles(
    ik: str,
    und: str,
    tsym: str,
    expiry: Any,
    open_c: dict,
    target_c: dict,
) -> Optional[Dict[str, Any]]:
    o_open = float(open_c.get("open") or 0)
    lp = float(target_c.get("close") or 0)
    oi_s = open_c.get("oi")
    oi_e = target_c.get("oi")
    if oi_s is None or oi_e is None or o_open <= 0:
        return None
    oi_start = int(float(oi_s))
    oi_end = int(float(oi_e))
    if oi_start <= 0:
        return None
    oi_chg = oi_end - oi_start
    chg_pct = (lp - o_open) / o_open * 100.0
    oi_chg_pct = (oi_chg / max(1, oi_start)) * 100.0
    price_dp = lp - o_open
    sig = _interpret_signal(price_dp, float(oi_chg))
    vol = int(float(target_c.get("volume") or 0))
    score = round(_score_row(oi_chg, chg_pct), 4)
    return {
        "instrument_key": ik,
        "underlying_symbol": und,
        "trading_symbol": tsym,
        "expiry": expiry,
        "ltp": round(lp, 2),
        "chg_pct": round(chg_pct, 3),
        "oi": oi_end,
        "oi_chg": oi_chg,
        "oi_chg_pct": round(oi_chg_pct, 3),
        "oi_signal": sig,
        "volume": vol,
        "score": score,
    }


def compute_historical_heatmap_rows(
    session_d: date,
    open_h: int,
    open_m: int,
    target_h: int,
    target_m: int,
    limit: int,
    sleep_s: float = 0.1,
) -> List[Dict[str, Any]]:
    """Build live-shaped rows from historical candles (no DB write)."""
    ux = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    universe = load_arbitrage_master_fut_universe(limit)
    rows: List[Dict[str, Any]] = []
    for ik, und, tsym, exp in universe:
        time.sleep(sleep_s)
        raw_1m = _fetch_candles_for_day(ux, ik, session_d, "minutes/1")
        day_1m = _candles_for_session_date(_sort_candles(raw_1m or []), session_d)
        oc = _find_candle_at_minute(day_1m, session_d, open_h, open_m)
        tc = _find_candle_at_minute(day_1m, session_d, target_h, target_m)
        if not oc or not tc:
            raw_5m = _fetch_candles_for_day(ux, ik, session_d, "minutes/5")
            day_5m = _candles_for_session_date(_sort_candles(raw_5m or []), session_d)
            oc = _find_candle_at_minute(day_5m, session_d, open_h, open_m)
            tc = _find_candle_at_minute(day_5m, session_d, target_h, target_m)
        if not oc or not tc:
            logger.debug("oi_heatmap_historical_replay: skip %s (missing bars)", und)
            continue
        r = _row_from_candles(ik, und, tsym, exp, oc, tc)
        if not r:
            logger.debug("oi_heatmap_historical_replay: skip %s (no OI)", und)
            continue
        rows.append(r)

    return finalize_heatmap_rows_for_store(rows)


def apply_historical_replay_to_database(
    session_d: date,
    open_h: int,
    open_m: int,
    target_h: int,
    target_m: int,
    limit: int = 203,
    sleep_s: float = 0.1,
) -> Dict[str, Any]:
    """
    Compute historical replay, INSERT snapshot rows into ``oi_heatmap_latest`` (append-only history),
    then refresh in-memory cache to this snapshot.

    ``updated_at`` is the snapshot IST datetime (session date + target clock).
    """
    rows = compute_historical_heatmap_rows(
        session_d, open_h, open_m, target_h, target_m, limit, sleep_s=sleep_s
    )
    if not rows:
        return {"success": False, "message": "no rows computed", "count": 0}

    snap_dt = IST.localize(datetime.combine(session_d, dt_time(target_h, target_m, 0)))
    snap_iso = snap_dt.isoformat()

    _persist_snapshot(rows, snap_dt)
    replace_cache_with_rows(rows, snap_iso, source="snapshot")

    logger.info(
        "oi_heatmap_historical_replay: persisted %s rows for %s @ %02d:%02d IST",
        len(rows),
        session_d,
        target_h,
        target_m,
    )
    return {
        "success": True,
        "count": len(rows),
        "updated_at": snap_iso,
        "session_date": session_d.isoformat(),
        "open": f"{open_h:02d}:{open_m:02d}",
        "target": f"{target_h:02d}:{target_m:02d}",
    }

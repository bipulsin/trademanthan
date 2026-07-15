"""Shortlist ADX backfill joined to universe VWAP scan — research / shadow only.

Computes Kavach live ADX (10m bars, length=14 / lensig=14, include_forming)
at each ``kavach_universe_vwap_scan`` 5m timestamp for a fixed symbol/date shortlist.
Does not alter live gates or existing scan columns.
"""
from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

import pytz
from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.kavach_10m import (
    _10m_series_upto_live,
    last_live_10m_pair_end_idx,
)
from backend.services.kavach_universe_vwap_scan import (
    _truncate_candles,
    _universe_keys,
    ensure_universe_vwap_scan,
)
from backend.services.smart_futures_picker.indicators import adx_value

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

# Sustained steep-VWAP-outside-lock shortlist (research).
SHORTLIST: Dict[str, List[str]] = {
    "LTM": ["2026-07-13", "2026-07-14", "2026-07-15"],
    "KPITTECH": ["2026-07-13", "2026-07-14", "2026-07-15"],
    "HDFCAMC": ["2026-07-14", "2026-07-15"],
    "DIVISLAB": ["2026-07-13", "2026-07-14"],
    "POLICYBZR": ["2026-07-14", "2026-07-15"],
    "LICHSGFIN": ["2026-07-13", "2026-07-14", "2026-07-15"],
    "HDFCLIFE": ["2026-07-13", "2026-07-15"],
    "BANKINDIA": ["2026-07-14", "2026-07-15"],
    "OBEROIRLTY": ["2026-07-14", "2026-07-15"],
    "COFORGE": ["2026-07-13", "2026-07-15"],
}

ADX_LENGTH = 14


def _as_ist(dt: Any) -> Optional[datetime]:
    if dt is None:
        return None
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            return IST.localize(dt)
        return dt.astimezone(IST)
    try:
        parsed = datetime.fromisoformat(str(dt).replace("Z", "+00:00"))
    except Exception:
        return None
    if parsed.tzinfo is None:
        return IST.localize(parsed)
    return parsed.astimezone(IST)


def _adx_14_at(candles: List[Dict[str, Any]], as_of: datetime) -> Optional[float]:
    """Kavach live ADX path: 10m series + include_forming, ADX(14,14)."""
    sliced = _truncate_candles(candles, as_of)
    if not sliced or len(sliced) < 40:
        return None
    pair_end = last_live_10m_pair_end_idx(sliced, now=as_of)
    if pair_end < 0:
        return None
    bars_10m = _10m_series_upto_live(sliced, pair_end)
    if len(bars_10m) < 5:
        return None
    highs = [float(b["high"]) for b in bars_10m]
    lows = [float(b["low"]) for b in bars_10m]
    closes = [float(b["close"]) for b in bars_10m]
    return adx_value(highs, lows, closes, ADX_LENGTH)


def _load_scan_rows(
    db, symbol: str, session_date: str
) -> List[Dict[str, Any]]:
    rows = db.execute(
        text(
            """
            SELECT symbol, session_date, logged_at, direction,
                   vwap_slope_score, steep_ok, vwap_extension_pct, in_lock_at_time, source
            FROM kavach_universe_vwap_scan
            WHERE UPPER(symbol) = :sym
              AND session_date = CAST(:d AS date)
            ORDER BY logged_at
            """
        ),
        {"sym": symbol.upper(), "d": session_date},
    ).fetchall()
    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "logged_at": _as_ist(r.logged_at),
                "direction": r.direction,
                "vwap_slope_score": float(r.vwap_slope_score)
                if r.vwap_slope_score is not None
                else None,
                "steep_ok": bool(r.steep_ok) if r.steep_ok is not None else None,
                "vwap_extension_pct": float(r.vwap_extension_pct)
                if r.vwap_extension_pct is not None
                else None,
                "in_lock_at_time": bool(r.in_lock_at_time),
                "source": r.source,
            }
        )
    return out


def _fetch_candles_for_symbol(
    upstox,
    instrument_key: str,
    dates: List[str],
) -> Optional[List[Dict[str, Any]]]:
    """One Upstox pull covering all shortlist dates for a symbol (+ warmup)."""
    end = max(date.fromisoformat(d) for d in dates)
    start = min(date.fromisoformat(d) for d in dates)
    # Warmup for ADX(14) on 10m: ~34 bars ≈ 1 session; keep 3+ calendar days before start.
    span_days = (end - start).days + 1
    days_back = max(5, span_days + 3)
    try:
        candles = upstox.get_historical_candles_by_instrument_key(
            instrument_key,
            interval="minutes/5",
            days_back=days_back,
            range_end_date=end,
        )
    except Exception as exc:
        logger.warning("shortlist ADX fetch %s failed: %s", instrument_key, exc)
        return None
    return candles or None


def build_shortlist_adx_export(
    *,
    pace_sec: float = 0.15,
    shortlist: Optional[Dict[str, List[str]]] = None,
) -> Dict[str, Any]:
    """Fetch OHLC (Upstox; raw bars not retained from VWAP backfill) + join ADX.

    Returns ``{ok, exported_at, meta, pairs: [{symbol, session_date, rows: [...]}]}``.
    """
    import time as time_mod

    from backend.config import settings
    from backend.services.upstox_service import UpstoxService

    ensure_universe_vwap_scan()
    sl = shortlist or SHORTLIST
    upstox = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)

    db = SessionLocal()
    try:
        key_map = {s: ik for s, ik in _universe_keys(db)}
        pairs_out: List[Dict[str, Any]] = []
        fetch_meta: Dict[str, Any] = {}
        missing_keys: List[str] = []

        for sym in sorted(sl.keys()):
            dates = sorted(sl[sym])
            ik = key_map.get(sym.upper())
            if not ik:
                missing_keys.append(sym)
                for sd in dates:
                    pairs_out.append(
                        {
                            "symbol": sym,
                            "session_date": sd,
                            "rows": [],
                            "error": "instrument_key_not_found",
                        }
                    )
                continue

            candles = _fetch_candles_for_symbol(upstox, ik, dates)
            if pace_sec > 0:
                time_mod.sleep(pace_sec)
            n_candles = len(candles) if candles else 0
            fetch_meta[sym] = {
                "instrument_key": ik,
                "candles": n_candles,
                "dates": dates,
            }
            if not candles:
                for sd in dates:
                    pairs_out.append(
                        {
                            "symbol": sym,
                            "session_date": sd,
                            "rows": [],
                            "error": "candle_fetch_failed",
                        }
                    )
                continue

            for sd in dates:
                scan_rows = _load_scan_rows(db, sym, sd)
                joined: List[Dict[str, Any]] = []
                for sr in scan_rows:
                    as_of = sr["logged_at"]
                    if as_of is None:
                        continue
                    adx = _adx_14_at(candles, as_of)
                    joined.append(
                        {
                            "logged_at": as_of.isoformat(),
                            "vwap_slope_score": sr["vwap_slope_score"],
                            "steep_ok": sr["steep_ok"],
                            "direction": sr["direction"],
                            "vwap_extension_pct": sr["vwap_extension_pct"],
                            "in_lock_at_time": sr["in_lock_at_time"],
                            "adx_14": round(adx, 2) if adx is not None else None,
                        }
                    )
                pairs_out.append(
                    {
                        "symbol": sym,
                        "session_date": sd,
                        "rows": joined,
                        "scan_row_count": len(scan_rows),
                    }
                )
                logger.info(
                    "shortlist ADX %s %s: scan=%s joined=%s adx_nonnull=%s",
                    sym,
                    sd,
                    len(scan_rows),
                    len(joined),
                    sum(1 for r in joined if r["adx_14"] is not None),
                )

        # Stable order matching SHORTLIST table (symbol then date)
        order: List[Tuple[str, str]] = []
        for sym, dates in (shortlist or SHORTLIST).items():
            for sd in dates:
                order.append((sym, sd))
        rank = {p: i for i, p in enumerate(order)}
        pairs_out.sort(
            key=lambda p: rank.get((p["symbol"], p["session_date"]), 10_000)
        )

        return {
            "ok": True,
            "exported_at": datetime.now(IST).isoformat(),
            "meta": {
                "adx": "kavach_10m ADX(14,14) include_forming — same as live entry gate",
                "ohlc_source": "upstox historical minutes/5 (futures instrument_key; same as VWAP backfill)",
                "note": "Raw OHLC from prior VWAP backfill was not retained; re-fetched for this shortlist only.",
                "shortlist_pair_count": len(order),
                "fetch": fetch_meta,
                "missing_instrument_keys": missing_keys,
            },
            "pairs": pairs_out,
        }
    except Exception as exc:
        logger.exception("shortlist ADX export failed")
        return {"ok": False, "error": str(exc)}
    finally:
        db.close()

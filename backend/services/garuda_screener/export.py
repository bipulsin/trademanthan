"""Read-only Garuda shadow export for external analysis.

Joins ``garuda_screener_log`` Top-6 qualification events with nearest RS Top-10
snapshot state, same-day grade/score history, and forward price/candle refs.

No scoring, ranking, gating, or logging-behavior changes — data access only.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, time, timedelta
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pytz
from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.garuda_screener.job import ensure_garuda_screener_log

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

SCHEMA_VERSION = 1
DEFAULT_PAGE_LIMIT = 200
MAX_PAGE_LIMIT = 1000

# Nearest RS snapshot may lag/lead a Garuda 10m bar_end by a few minutes.
RS_MATCH_WINDOW = timedelta(minutes=20)

SESSION_END_IST = time(15, 30)

_EXPORT_NOTES = {
    "qualifiers": (
        "One row per Garuda Top-6 qualification event "
        "(garuda_screener_log where top6_rank IS NOT NULL)."
    ),
    "rs_top10": (
        "Nearest relative_strength_snapshot for the same symbol on the same "
        "IST session within ±20m of bar_end (persisted ranks 1–10)."
    ),
    "grade_history": (
        "Same-symbol Confidence Grade / Trade Score points for the remainder "
        "of that IST session from rs_live_kavach_audit, "
        "relative_strength_snapshot, and kavach_confidence_component_log."
    ),
    "forward_bars": (
        "No durable FO 10m OHLC table exists. close is taken from subsequent "
        "garuda_screener_log.price rows (same symbol/session). Full OHLC is "
        "filled from in-process candle_cache when present (cache-only, never "
        "refetches Upstox); otherwise open/high/low/volume are null."
    ),
    "data_completeness": (
        "Per-row flag listing missing/null expected joins so nulls are not "
        "misread as zeros."
    ),
}


def _iso(val: Any) -> Optional[str]:
    if val is None:
        return None
    if isinstance(val, datetime):
        dt = val
        if dt.tzinfo is None:
            dt = IST.localize(dt)
        return dt.isoformat()
    if isinstance(val, date):
        return val.isoformat()
    return str(val)


def _f(val: Any) -> Optional[float]:
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _i(val: Any) -> Optional[int]:
    if val is None:
        return None
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


def _as_ist(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return IST.localize(dt)
    return dt.astimezone(IST)


def _parse_date(val: Optional[str]) -> Optional[str]:
    if not val:
        return None
    s = str(val).strip()[:10]
    date.fromisoformat(s)  # raises ValueError if bad
    return s


def _session_end(session_date: str) -> datetime:
    d = date.fromisoformat(session_date)
    return IST.localize(datetime.combine(d, SESSION_END_IST))


def available_shadow_window(db) -> Tuple[Optional[str], Optional[str]]:
    """Return (min_session_date, max_session_date) from garuda_screener_log."""
    ensure_garuda_screener_log()
    row = db.execute(
        text(
            """
            SELECT MIN(session_date) AS d0, MAX(session_date) AS d1
            FROM garuda_screener_log
            WHERE top6_rank IS NOT NULL
            """
        )
    ).fetchone()
    if not row or row.d0 is None:
        return None, None
    return _iso(row.d0), _iso(row.d1)


def _fetch_qualifiers(
    db,
    *,
    start_date: str,
    end_date: str,
    symbol: Optional[str],
    limit: int,
    offset: int,
) -> Tuple[List[Any], int]:
    where = [
        "top6_rank IS NOT NULL",
        "session_date >= CAST(:d0 AS date)",
        "session_date <= CAST(:d1 AS date)",
    ]
    params: Dict[str, Any] = {"d0": start_date, "d1": end_date, "lim": limit, "off": offset}
    if symbol:
        where.append("UPPER(TRIM(symbol)) = :sym")
        params["sym"] = symbol.upper().strip()
    clause = " AND ".join(where)

    total = db.execute(
        text(f"SELECT COUNT(*) AS n FROM garuda_screener_log WHERE {clause}"),
        params,
    ).fetchone()
    n = int(total.n) if total else 0

    rows = db.execute(
        text(
            f"""
            SELECT id, session_date, bar_end, symbol, side,
                   imbalance_confirmed, imbalance_side, imbalance_hits, imbalance_legs,
                   direction_agreement, direction_side, direction,
                   day_rs, strength_percentile,
                   trend_adx, trend_adx_slope, trend_er,
                   momentum_percentile, momentum,
                   rank_score, top6_rank, price, components, logged_at
            FROM garuda_screener_log
            WHERE {clause}
            ORDER BY session_date ASC, bar_end ASC, top6_rank ASC NULLS LAST, symbol ASC
            LIMIT :lim OFFSET :off
            """
        ),
        params,
    ).fetchall()
    return list(rows), n


def _fetch_rs_snapshots(
    db, *, start_date: str, end_date: str, symbols: Sequence[str]
) -> List[Any]:
    if not symbols:
        return []
    try:
        return list(
            db.execute(
                text(
                    """
                    SELECT scan_time, symbol, current_price, previous_close,
                           stock_percent, nifty_percent, relative_strength,
                           trade_score, confidence_grade, kavach_state, kavach_strength,
                           ranking_type, rank_position, volume_label, volume_ratio,
                           volume_tod_ratio, vwap_purity_pct, market_regime, adx
                    FROM relative_strength_snapshot
                    WHERE (scan_time AT TIME ZONE 'Asia/Kolkata')::date
                          BETWEEN CAST(:d0 AS date) AND CAST(:d1 AS date)
                      AND UPPER(TRIM(symbol)) = ANY(:syms)
                      AND rank_position IS NOT NULL
                      AND rank_position <= 10
                    ORDER BY scan_time ASC
                    """
                ),
                {"d0": start_date, "d1": end_date, "syms": [s.upper() for s in symbols]},
            ).fetchall()
        )
    except Exception as exc:
        logger.warning("garuda export: RS snapshot query failed: %s", exc)
        return []


def _nearest_rs(rows: List[Any], symbol: str, bar_end: datetime) -> Optional[Dict[str, Any]]:
    be = _as_ist(bar_end)
    if be is None:
        return None
    sym = symbol.upper().strip()
    best = None
    best_delta = None
    for r in rows:
        if str(r.symbol or "").upper().strip() != sym:
            continue
        st = _as_ist(r.scan_time)
        if st is None:
            continue
        # Same IST calendar day only
        if st.date() != be.date():
            continue
        delta = abs((st - be).total_seconds())
        if delta > RS_MATCH_WINDOW.total_seconds():
            continue
        if best_delta is None or delta < best_delta:
            best_delta = delta
            best = r
    if best is None:
        return None
    rank = _i(best.rank_position)
    return {
        "matched": True,
        "in_top10": rank is not None and 1 <= rank <= 10,
        "scan_time": _iso(best.scan_time),
        "match_delta_seconds": int(best_delta) if best_delta is not None else None,
        "ranking_type": best.ranking_type,
        "rank_position": rank,
        "relative_strength": _f(best.relative_strength),
        "stock_percent": _f(getattr(best, "stock_percent", None)),
        "nifty_percent": _f(getattr(best, "nifty_percent", None)),
        "current_price": _f(best.current_price),
        "previous_close": _f(getattr(best, "previous_close", None)),
        "trade_score": _f(best.trade_score),
        "confidence_grade": best.confidence_grade,
        "kavach_state": best.kavach_state,
        "kavach_strength": _i(getattr(best, "kavach_strength", None)),
        "volume_label": getattr(best, "volume_label", None),
        "volume_ratio": _f(getattr(best, "volume_ratio", None)),
        "volume_tod_ratio": _f(getattr(best, "volume_tod_ratio", None)),
        "vwap_purity_pct": _f(getattr(best, "vwap_purity_pct", None)),
        "market_regime": getattr(best, "market_regime", None),
        "adx": _f(getattr(best, "adx", None)),
    }


def _fetch_grade_audit(
    db, *, start_date: str, end_date: str, symbols: Sequence[str]
) -> List[Any]:
    if not symbols:
        return []
    try:
        return list(
            db.execute(
                text(
                    """
                    SELECT session_date, symbol, bar_evaluated_at AS at,
                           trade_score, confidence_grade, kavach_state, price,
                           volume_label, vwap_purity_pct, market_regime, adx,
                           'rs_live_kavach_audit' AS source
                    FROM rs_live_kavach_audit
                    WHERE session_date BETWEEN CAST(:d0 AS date) AND CAST(:d1 AS date)
                      AND UPPER(TRIM(symbol)) = ANY(:syms)
                    ORDER BY bar_evaluated_at ASC
                    """
                ),
                {"d0": start_date, "d1": end_date, "syms": [s.upper() for s in symbols]},
            ).fetchall()
        )
    except Exception as exc:
        logger.warning("garuda export: rs_live_kavach_audit query failed: %s", exc)
        return []


def _fetch_component_log(
    db, *, start_date: str, end_date: str, symbols: Sequence[str]
) -> List[Any]:
    if not symbols:
        return []
    try:
        return list(
            db.execute(
                text(
                    """
                    SELECT session_date, symbol,
                           COALESCE(bar_at, logged_at) AS at,
                           trade_score, confidence_grade,
                           NULL::text AS kavach_state,
                           NULL::double precision AS price,
                           volume_label, purity_pct AS vwap_purity_pct,
                           regime AS market_regime,
                           NULL::double precision AS adx,
                           'kavach_confidence_component_log' AS source
                    FROM kavach_confidence_component_log
                    WHERE session_date BETWEEN CAST(:d0 AS date) AND CAST(:d1 AS date)
                      AND UPPER(TRIM(symbol)) = ANY(:syms)
                    ORDER BY COALESCE(bar_at, logged_at) ASC
                    """
                ),
                {"d0": start_date, "d1": end_date, "syms": [s.upper() for s in symbols]},
            ).fetchall()
        )
    except Exception as exc:
        # Table may not exist yet on fresh DBs
        logger.debug("garuda export: component log query skipped: %s", exc)
        return []


def _grade_points_for(
    *,
    symbol: str,
    session_date: str,
    bar_end: datetime,
    audit_rows: List[Any],
    component_rows: List[Any],
    rs_rows: List[Any],
) -> List[Dict[str, Any]]:
    be = _as_ist(bar_end)
    end = _session_end(session_date)
    sym = symbol.upper().strip()
    out: List[Dict[str, Any]] = []
    seen = set()

    def _add(at, source, trade_score, confidence_grade, kavach_state, price, extra=None):
        at_ist = _as_ist(at)
        if at_ist is None or be is None:
            return
        if at_ist.date() != date.fromisoformat(session_date):
            return
        if at_ist < be or at_ist > end:
            return
        key = (
            _iso(at_ist),
            source,
            confidence_grade,
            None if trade_score is None else round(float(trade_score), 4),
        )
        if key in seen:
            return
        seen.add(key)
        row = {
            "at": _iso(at_ist),
            "source": source,
            "trade_score": _f(trade_score),
            "confidence_grade": confidence_grade,
            "kavach_state": kavach_state,
            "price": _f(price),
        }
        if extra:
            row.update(extra)
        out.append(row)

    for r in audit_rows:
        if str(r.symbol or "").upper().strip() != sym:
            continue
        if str(r.session_date)[:10] != session_date:
            continue
        _add(
            r.at,
            "rs_live_kavach_audit",
            r.trade_score,
            r.confidence_grade,
            r.kavach_state,
            r.price,
            {
                "volume_label": getattr(r, "volume_label", None),
                "vwap_purity_pct": _f(getattr(r, "vwap_purity_pct", None)),
                "market_regime": getattr(r, "market_regime", None),
                "adx": _f(getattr(r, "adx", None)),
            },
        )

    for r in component_rows:
        if str(r.symbol or "").upper().strip() != sym:
            continue
        if str(r.session_date)[:10] != session_date:
            continue
        _add(
            r.at,
            "kavach_confidence_component_log",
            r.trade_score,
            r.confidence_grade,
            None,
            None,
            {
                "volume_label": getattr(r, "volume_label", None),
                "vwap_purity_pct": _f(getattr(r, "vwap_purity_pct", None)),
                "market_regime": getattr(r, "market_regime", None),
            },
        )

    for r in rs_rows:
        if str(r.symbol or "").upper().strip() != sym:
            continue
        st = _as_ist(r.scan_time)
        if st is None or st.date().isoformat() != session_date:
            continue
        _add(
            st,
            "relative_strength_snapshot",
            r.trade_score,
            r.confidence_grade,
            r.kavach_state,
            r.current_price,
            {
                "ranking_type": r.ranking_type,
                "rank_position": _i(r.rank_position),
                "relative_strength": _f(r.relative_strength),
            },
        )

    out.sort(key=lambda x: x.get("at") or "")
    return out


def _fetch_forward_prices(
    db, *, start_date: str, end_date: str, symbols: Sequence[str]
) -> List[Any]:
    if not symbols:
        return []
    return list(
        db.execute(
            text(
                """
                SELECT session_date, symbol, bar_end, price
                FROM garuda_screener_log
                WHERE session_date BETWEEN CAST(:d0 AS date) AND CAST(:d1 AS date)
                  AND UPPER(TRIM(symbol)) = ANY(:syms)
                ORDER BY bar_end ASC
                """
            ),
            {"d0": start_date, "d1": end_date, "syms": [s.upper() for s in symbols]},
        ).fetchall()
    )


def _instrument_keys(db, symbols: Sequence[str]) -> Dict[str, str]:
    if not symbols:
        return {}
    rows = db.execute(
        text(
            """
            SELECT UPPER(TRIM(stock)) AS symbol,
                   TRIM(currmth_future_instrument_key) AS ikey
            FROM arbitrage_master
            WHERE UPPER(TRIM(stock)) = ANY(:syms)
              AND currmth_future_instrument_key IS NOT NULL
              AND TRIM(currmth_future_instrument_key) <> ''
            """
        ),
        {"syms": [s.upper() for s in symbols]},
    ).fetchall()
    return {
        str(r.symbol).upper(): str(r.ikey).strip()
        for r in rows
        if r.symbol and r.ikey
    }


def _cache_ohlc_map(
    instrument_key: Optional[str], session_date: str, bar_end: datetime
) -> Dict[str, Dict[str, Any]]:
    """Best-effort OHLC from in-process candle_cache (no Upstox fetch)."""
    out: Dict[str, Dict[str, Any]] = {}
    if not instrument_key:
        return out
    try:
        from backend.services.garuda_screener.job import _bars_from_candles
        from backend.services.rs_conviction_candles import candles_cache_only

        candles = candles_cache_only(instrument_key)
        if not candles:
            return out
        bars = _bars_from_candles(candles)
        be = _as_ist(bar_end)
        end = _session_end(session_date)
        for b in bars:
            bend = _as_ist(b.get("bar_end"))
            if bend is None or be is None:
                continue
            if bend.date().isoformat() != session_date:
                continue
            if bend < be or bend > end:
                continue
            out[_iso(bend) or ""] = {
                "bar_end": _iso(bend),
                "open": _f(b.get("open")),
                "high": _f(b.get("high")),
                "low": _f(b.get("low")),
                "close": _f(b.get("close")),
                "volume": _f(b.get("volume")),
                "source": "candle_cache",
            }
    except Exception as exc:
        logger.debug(
            "garuda export: candle_cache OHLC skipped for %s: %s", instrument_key, exc
        )
    return out


def _forward_bars_for(
    *,
    symbol: str,
    session_date: str,
    bar_end: datetime,
    price_rows: List[Any],
    cache_ohlc: Optional[Dict[str, Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    be = _as_ist(bar_end)
    end = _session_end(session_date)
    sym = symbol.upper().strip()
    cache_ohlc = cache_ohlc or {}
    bars: List[Dict[str, Any]] = []
    for r in price_rows:
        if str(r.symbol or "").upper().strip() != sym:
            continue
        if str(r.session_date)[:10] != session_date:
            continue
        bend = _as_ist(r.bar_end)
        if bend is None or be is None:
            continue
        if bend < be or bend > end:
            continue
        key = _iso(bend) or ""
        cached = cache_ohlc.get(key)
        if cached and cached.get("close") is not None:
            bars.append(cached)
        else:
            bars.append(
                {
                    "bar_end": key,
                    "open": cached.get("open") if cached else None,
                    "high": cached.get("high") if cached else None,
                    "low": cached.get("low") if cached else None,
                    "close": _f(r.price),
                    "volume": cached.get("volume") if cached else None,
                    "source": (
                        "garuda_screener_log_price+candle_cache_partial"
                        if cached
                        else "garuda_screener_log_price"
                    ),
                }
            )
    bars.sort(key=lambda x: x.get("bar_end") or "")
    return bars


def build_data_completeness(
    *,
    qualifier: Dict[str, Any],
    rs_top10: Optional[Dict[str, Any]],
    grade_history: List[Dict[str, Any]],
    forward_bars: List[Dict[str, Any]],
) -> Dict[str, Any]:
    missing: List[str] = []
    notes: List[str] = []

    for field in ("price", "side", "rank_score", "top6_rank", "direction_side"):
        if qualifier.get(field) is None:
            missing.append(f"garuda.{field}")

    if not rs_top10 or not rs_top10.get("matched"):
        missing.append("rs_top10")
        notes.append("No relative_strength_snapshot within ±20m of bar_end for this symbol.")
    else:
        for field in ("rank_position", "relative_strength", "trade_score", "confidence_grade"):
            if rs_top10.get(field) is None:
                missing.append(f"rs_top10.{field}")

    if not grade_history:
        missing.append("grade_history")
        notes.append(
            "No Confidence Grade / Trade Score history for remainder of session "
            "(symbol may not have been on the locked checklist or RS Top-10)."
        )

    if not forward_bars:
        missing.append("forward_bars")
        notes.append("No subsequent garuda_screener_log price rows for remainder of session.")
    else:
        ohlc_incomplete = any(
            b.get("open") is None or b.get("high") is None or b.get("low") is None
            for b in forward_bars
        )
        if ohlc_incomplete:
            missing.append("forward_bars.ohlc")
            notes.append(
                "Full 10m OHLC unavailable from durable storage; close proxies from "
                "garuda_screener_log.price. OHLC only when candle_cache still holds bars."
            )
        close_missing = any(b.get("close") is None for b in forward_bars)
        if close_missing:
            missing.append("forward_bars.close")

    return {
        "complete": len(missing) == 0,
        "missing_fields": missing,
        "notes": notes,
    }


def _qualifier_dict(r: Any) -> Dict[str, Any]:
    direction = r.direction if isinstance(r.direction, dict) else (r.direction or {})
    momentum = r.momentum if isinstance(r.momentum, dict) else (r.momentum or {})
    components = r.components if isinstance(r.components, dict) else (r.components or {})
    return {
        "qualification_id": _i(r.id),
        "session_date": str(r.session_date)[:10],
        "bar_end": _iso(r.bar_end),
        "logged_at": _iso(r.logged_at),
        "symbol": str(r.symbol or "").upper().strip(),
        "side": r.side,
        "top6_rank": _i(r.top6_rank),
        "rank_score": _f(r.rank_score),
        "price": _f(r.price),
        "imbalance_confirmed": bool(r.imbalance_confirmed),
        "imbalance_side": r.imbalance_side,
        "imbalance_hits": r.imbalance_hits if r.imbalance_hits is not None else [],
        "imbalance_legs": r.imbalance_legs if r.imbalance_legs is not None else {},
        "direction_agreement": r.direction_agreement,
        "direction_side": r.direction_side,
        "direction": direction,
        "day_rs": _f(r.day_rs),
        "strength_percentile": _f(r.strength_percentile),
        "trend_adx": _f(r.trend_adx),
        "trend_adx_slope": _f(r.trend_adx_slope),
        "trend_er": _f(r.trend_er),
        "momentum_percentile": _f(r.momentum_percentile),
        "momentum": momentum,
        "components": components,
    }


def export_garuda_shadow(
    *,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    symbol: Optional[str] = None,
    limit: int = DEFAULT_PAGE_LIMIT,
    offset: int = 0,
    include_cache_ohlc: bool = True,
) -> Dict[str, Any]:
    """Build paginated Garuda shadow export payload."""
    ensure_garuda_screener_log()
    lim = max(1, min(int(limit or DEFAULT_PAGE_LIMIT), MAX_PAGE_LIMIT))
    off = max(0, int(offset or 0))
    sym = (symbol or "").strip().upper() or None

    db = SessionLocal()
    try:
        win0, win1 = available_shadow_window(db)
        try:
            d0 = _parse_date(start_date) or win0
            d1 = _parse_date(end_date) or win1
        except ValueError as exc:
            return {"ok": False, "error": f"invalid_date: {exc}"}

        if not d0 or not d1:
            return {
                "ok": True,
                "schema_version": SCHEMA_VERSION,
                "start_date": d0,
                "end_date": d1,
                "available_window": {"start_date": win0, "end_date": win1},
                "symbol": sym,
                "row_count": 0,
                "total_count": 0,
                "limit": lim,
                "offset": off,
                "has_more": False,
                "notes": _EXPORT_NOTES,
                "rows": [],
                "empty": True,
                "message": "No Garuda Top-6 rows in garuda_screener_log yet.",
            }

        if d0 > d1:
            return {"ok": False, "error": "start_date must be <= end_date"}

        qualifiers, total = _fetch_qualifiers(
            db, start_date=d0, end_date=d1, symbol=sym, limit=lim, offset=off
        )
        symbols = sorted(
            {str(r.symbol or "").upper().strip() for r in qualifiers if r.symbol}
        )

        rs_rows = _fetch_rs_snapshots(
            db, start_date=d0, end_date=d1, symbols=symbols
        )
        audit_rows = _fetch_grade_audit(
            db, start_date=d0, end_date=d1, symbols=symbols
        )
        component_rows = _fetch_component_log(
            db, start_date=d0, end_date=d1, symbols=symbols
        )
        price_rows = _fetch_forward_prices(
            db, start_date=d0, end_date=d1, symbols=symbols
        )
        ikeys = _instrument_keys(db, symbols) if include_cache_ohlc else {}

        # Cache OHLC once per (symbol, session_date) for this page
        cache_maps: Dict[Tuple[str, str], Dict[str, Dict[str, Any]]] = {}
        rows_out: List[Dict[str, Any]] = []
        for r in qualifiers:
            q = _qualifier_dict(r)
            be = _as_ist(r.bar_end)
            rs = _nearest_rs(rs_rows, q["symbol"], be) if be else None
            grades = _grade_points_for(
                symbol=q["symbol"],
                session_date=q["session_date"],
                bar_end=be,
                audit_rows=audit_rows,
                component_rows=component_rows,
                rs_rows=rs_rows,
            )
            ck = (q["symbol"], q["session_date"])
            if include_cache_ohlc and ck not in cache_maps and be is not None:
                cache_maps[ck] = _cache_ohlc_map(
                    ikeys.get(q["symbol"]), q["session_date"], be
                )
            forward = _forward_bars_for(
                symbol=q["symbol"],
                session_date=q["session_date"],
                bar_end=be,
                price_rows=price_rows,
                cache_ohlc=cache_maps.get(ck) if include_cache_ohlc else None,
            )
            completeness = build_data_completeness(
                qualifier=q,
                rs_top10=rs,
                grade_history=grades,
                forward_bars=forward,
            )
            rows_out.append(
                {
                    **q,
                    "rs_top10": rs
                    or {
                        "matched": False,
                        "in_top10": False,
                        "scan_time": None,
                        "match_delta_seconds": None,
                        "ranking_type": None,
                        "rank_position": None,
                        "relative_strength": None,
                        "trade_score": None,
                        "confidence_grade": None,
                        "kavach_state": None,
                        "current_price": None,
                    },
                    "grade_history": grades,
                    "forward_bars": forward,
                    "data_completeness": completeness,
                }
            )

        return {
            "ok": True,
            "schema_version": SCHEMA_VERSION,
            "start_date": d0,
            "end_date": d1,
            "available_window": {"start_date": win0, "end_date": win1},
            "symbol": sym,
            "row_count": len(rows_out),
            "total_count": total,
            "limit": lim,
            "offset": off,
            "has_more": (off + len(rows_out)) < total,
            "notes": _EXPORT_NOTES,
            "rows": rows_out,
        }
    except Exception as exc:
        logger.warning("garuda shadow export failed: %s", exc, exc_info=True)
        return {"ok": False, "error": str(exc)}
    finally:
        db.close()

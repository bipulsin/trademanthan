"""Garuda live shadow job — cache-only 10m FO scan, no gating.

Writes ``garuda_screener_log`` (one row per symbol per bar_end) and serves
Top-6 for UI + trade_log confluence lookup. Independent of Kavach.
"""
from __future__ import annotations

import json
import logging
from datetime import date, datetime, time, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pytz
from sqlalchemy import text

from backend.database import SessionLocal, engine
from backend.services.garuda_screener.config import (
    EXCLUDED_SYMBOLS,
    NIFTY_KEY,
    RS_WINDOW_BARS,
    TOP_N,
    GarudaConfig,
)
from backend.services.garuda_screener.screener import (
    GarudaBarContext,
    evaluate_symbol,
    rank_top_n,
)

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

_ENSURED = False

# Confluence enum values stored on trade_log.garuda_confluence
CONFLUENCE_MATCH = "MATCH"
CONFLUENCE_NO_MATCH = "NO_MATCH"
CONFLUENCE_NOT_AVAILABLE = "NOT_AVAILABLE"


def ensure_garuda_screener_log() -> None:
    global _ENSURED
    if _ENSURED:
        return
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS garuda_screener_log (
                    id BIGSERIAL PRIMARY KEY,
                    session_date DATE NOT NULL,
                    bar_end TIMESTAMPTZ NOT NULL,
                    symbol TEXT NOT NULL,
                    side TEXT,
                    imbalance_confirmed BOOLEAN NOT NULL DEFAULT FALSE,
                    imbalance_side TEXT,
                    imbalance_hits JSONB,
                    imbalance_legs JSONB,
                    direction_agreement BOOLEAN,
                    direction_side TEXT,
                    direction JSONB,
                    day_rs DOUBLE PRECISION,
                    strength_percentile DOUBLE PRECISION,
                    trend_adx DOUBLE PRECISION,
                    trend_adx_slope DOUBLE PRECISION,
                    trend_er DOUBLE PRECISION,
                    momentum_percentile DOUBLE PRECISION,
                    momentum JSONB,
                    rank_score DOUBLE PRECISION,
                    top6_rank INTEGER,
                    price DOUBLE PRECISION,
                    components JSONB,
                    logged_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (session_date, bar_end, symbol)
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS idx_garuda_log_session_bar
                ON garuda_screener_log (session_date, bar_end DESC)
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS idx_garuda_log_top6
                ON garuda_screener_log (session_date, bar_end DESC, top6_rank)
                WHERE top6_rank IS NOT NULL
                """
            )
        )
    _ENSURED = True


def _session_date_ist(now: Optional[datetime] = None) -> str:
    n = now or datetime.now(IST)
    if n.tzinfo is None:
        n = IST.localize(n)
    return n.astimezone(IST).strftime("%Y-%m-%d")


def _parse_ts(val: Any) -> Optional[datetime]:
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


def _universe_keys(db) -> List[Tuple[str, str, Optional[str]]]:
    rows = db.execute(
        text(
            """
            SELECT UPPER(TRIM(stock)) AS symbol,
                   TRIM(currmth_future_instrument_key) AS ikey,
                   TRIM(sector_index) AS sector_key
            FROM arbitrage_master
            WHERE stock IS NOT NULL
              AND TRIM(stock) <> ''
              AND currmth_future_instrument_key IS NOT NULL
              AND TRIM(currmth_future_instrument_key) <> ''
            ORDER BY 1
            """
        )
    ).fetchall()
    out: List[Tuple[str, str, Optional[str]]] = []
    seen = set()
    for r in rows:
        sym = str(r.symbol or "").strip().upper()
        ik = str(r.ikey or "").strip()
        if not sym or not ik or sym in seen or sym in EXCLUDED_SYMBOLS:
            continue
        seen.add(sym)
        sk = str(r.sector_key or "").strip() or None
        out.append((sym, ik, sk))
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
        if r.atr14_pct is not None and float(r.atr14_pct) > 0
    }


def _enrich_10m_bars(raw_10m: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    from backend.services.vajra.engine import ADX_LEN, _dmi_adx
    from backend.services.vajra.indicators import cumulative_vwap, ema_series

    if not raw_10m:
        return []
    highs = [float(b["high"]) for b in raw_10m]
    lows = [float(b["low"]) for b in raw_10m]
    closes = [float(b["close"]) for b in raw_10m]
    vols = [float(b.get("volume") or 0) for b in raw_10m]
    ema5 = ema_series(closes, 5)
    ema10 = ema_series(closes, 10)
    vwap = cumulative_vwap(highs, lows, closes, vols)
    _, _, adx_series = _dmi_adx(highs, lows, closes, ADX_LEN)
    out = []
    for i, b in enumerate(raw_10m):
        bar_end = b.get("bar_end")
        if bar_end is None:
            ts = _parse_ts(b.get("timestamp"))
            bar_end = ts + timedelta(minutes=5) if ts else None
        elif isinstance(bar_end, datetime) and bar_end.tzinfo is None:
            bar_end = IST.localize(bar_end)
        out.append(
            {
                "bar_end": bar_end,
                "open": float(b["open"]),
                "high": float(b["high"]),
                "low": float(b["low"]),
                "close": float(b["close"]),
                "volume": float(b.get("volume") or 0),
                "ema5": ema5[i] if i < len(ema5) else None,
                "ema10": ema10[i] if i < len(ema10) else None,
                "vwap": vwap[i] if i < len(vwap) else None,
                "adx": adx_series[i] if i < len(adx_series) else None,
            }
        )
    return out


def _bars_from_candles(candles: List[Dict[str, Any]], *, now: datetime) -> List[Dict[str, Any]]:
    from backend.services.kavach_10m import (
        aggregate_10m_bars,
        last_closed_10m_pair_end_idx,
    )

    if not candles or len(candles) < 20:
        return []
    pair_end = last_closed_10m_pair_end_idx(candles, now=now)
    if pair_end < 0:
        return []
    raw = [b for b in aggregate_10m_bars(candles) if b["end_5m_idx"] <= pair_end]
    return _enrich_10m_bars(raw)


def _session_slice(bars: List[Dict[str, Any]], session: str) -> List[Dict[str, Any]]:
    out = []
    for b in bars:
        be = b.get("bar_end")
        if be is None:
            continue
        if be.astimezone(IST).strftime("%Y-%m-%d") == session:
            out.append(b)
    return out


def _prior_close(bars: List[Dict[str, Any]], session: str) -> Optional[float]:
    """Last close of the most recent prior session present in bars."""
    prior: Dict[str, float] = {}
    for b in bars:
        be = b.get("bar_end")
        if be is None:
            continue
        sd = be.astimezone(IST).strftime("%Y-%m-%d")
        if sd < session:
            prior[sd] = float(b["close"])
    if not prior:
        return None
    return prior[max(prior.keys())]


def _window_pct(bars: List[Dict[str, Any]], session: str, as_of: datetime, n: int) -> Optional[float]:
    sub = [
        b
        for b in bars
        if b.get("bar_end")
        and b["bar_end"].astimezone(IST).strftime("%Y-%m-%d") == session
        and b["bar_end"] <= as_of
    ]
    if len(sub) < n + 1:
        return None
    c0 = float(sub[-n - 1]["close"])
    c1 = float(sub[-1]["close"])
    if not c0:
        return None
    return (c1 - c0) / c0 * 100.0


def _gap_filled(session_bars: List[Dict[str, Any]], prior_close: float, n_bars: int, atr_pct: float) -> bool:
    if not session_bars or not prior_close:
        return True
    open_px = float(session_bars[0]["open"])
    gap = open_px - prior_close
    if abs(gap) / prior_close * 100.0 < atr_pct * 0.75:
        return True
    hold = session_bars[:n_bars]
    if gap > 0:
        return any(float(b["low"]) <= prior_close for b in hold)
    return any(float(b["high"]) >= prior_close for b in hold)


def _row_for_insert(eval_row: Dict[str, Any], *, session_date: str, bar_end: datetime) -> Dict[str, Any]:
    direction = eval_row.get("direction") or {}
    strength = eval_row.get("strength") or {}
    trend = eval_row.get("trend") or {}
    momentum = eval_row.get("momentum") or {}
    return {
        "session_date": session_date,
        "bar_end": bar_end,
        "symbol": eval_row["symbol"],
        "side": eval_row.get("side"),
        "imbalance_confirmed": bool(eval_row.get("imbalance_confirmed")),
        "imbalance_side": eval_row.get("imbalance_side"),
        "imbalance_hits": json.dumps(eval_row.get("imbalance_hits") or []),
        "imbalance_legs": json.dumps(eval_row.get("imbalance_legs") or {}),
        "direction_agreement": direction.get("agreement"),
        "direction_side": direction.get("side"),
        "direction": json.dumps(direction),
        "day_rs": strength.get("day_rs"),
        "strength_percentile": strength.get("percentile"),
        "trend_adx": trend.get("adx"),
        "trend_adx_slope": trend.get("adx_slope"),
        "trend_er": trend.get("efficiency_ratio"),
        "momentum_percentile": momentum.get("percentile_roc3"),
        "momentum": json.dumps(momentum),
        "rank_score": eval_row.get("rank_score"),
        "top6_rank": eval_row.get("rank"),
        "price": eval_row.get("price"),
        "components": json.dumps(
            {
                "strength": strength,
                "trend": trend,
                "momentum": momentum,
                "imbalance_hits": eval_row.get("imbalance_hits"),
            }
        ),
    }


def _insert_rows(db, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    sql = text(
        """
        INSERT INTO garuda_screener_log (
            session_date, bar_end, symbol, side,
            imbalance_confirmed, imbalance_side, imbalance_hits, imbalance_legs,
            direction_agreement, direction_side, direction,
            day_rs, strength_percentile,
            trend_adx, trend_adx_slope, trend_er,
            momentum_percentile, momentum,
            rank_score, top6_rank, price, components, logged_at
        ) VALUES (
            CAST(:session_date AS date), :bar_end, :symbol, :side,
            :imbalance_confirmed, :imbalance_side, CAST(:imbalance_hits AS jsonb), CAST(:imbalance_legs AS jsonb),
            :direction_agreement, :direction_side, CAST(:direction AS jsonb),
            :day_rs, :strength_percentile,
            :trend_adx, :trend_adx_slope, :trend_er,
            :momentum_percentile, CAST(:momentum AS jsonb),
            :rank_score, :top6_rank, :price, CAST(:components AS jsonb), NOW()
        )
        ON CONFLICT (session_date, bar_end, symbol) DO UPDATE SET
            side = EXCLUDED.side,
            imbalance_confirmed = EXCLUDED.imbalance_confirmed,
            imbalance_side = EXCLUDED.imbalance_side,
            imbalance_hits = EXCLUDED.imbalance_hits,
            imbalance_legs = EXCLUDED.imbalance_legs,
            direction_agreement = EXCLUDED.direction_agreement,
            direction_side = EXCLUDED.direction_side,
            direction = EXCLUDED.direction,
            day_rs = EXCLUDED.day_rs,
            strength_percentile = EXCLUDED.strength_percentile,
            trend_adx = EXCLUDED.trend_adx,
            trend_adx_slope = EXCLUDED.trend_adx_slope,
            trend_er = EXCLUDED.trend_er,
            momentum_percentile = EXCLUDED.momentum_percentile,
            momentum = EXCLUDED.momentum,
            rank_score = EXCLUDED.rank_score,
            top6_rank = EXCLUDED.top6_rank,
            price = EXCLUDED.price,
            components = EXCLUDED.components,
            logged_at = NOW()
        """
    )
    for r in rows:
        db.execute(sql, r)
    db.commit()
    return len(rows)


def run_live_garuda_screener(*, force: bool = False) -> Dict[str, Any]:
    """One RTH 10m sweep over FO universe using shared candle_cache. Shadow-only."""
    now = datetime.now(IST)
    if not force:
        t = now.time()
        if now.weekday() >= 5 or t < time(9, 25) or t > time(15, 30):
            return {"ok": True, "skipped": True, "reason": "outside_rth"}

    from backend.services.rs_conviction_candles import candles_cache_only

    ensure_garuda_screener_log()
    session_date = _session_date_ist(now)
    cfg = GarudaConfig()
    db = SessionLocal()
    try:
        universe = _universe_keys(db)
        atrs = _atr_map(db, [s for s, _, _ in universe])

        nifty_candles = candles_cache_only(NIFTY_KEY) or []
        nifty_bars = _bars_from_candles(nifty_candles, now=now)
        if not nifty_bars:
            return {"ok": False, "error": "nifty_bars_unavailable", "cache_miss_nifty": True}

        # Determine common bar_end from nifty last closed 10m
        nifty_sess = _session_slice(nifty_bars, session_date)
        if not nifty_sess:
            return {"ok": True, "skipped": True, "reason": "no_nifty_session_bars"}
        bar_end = nifty_sess[-1]["bar_end"]
        if bar_end is None:
            return {"ok": False, "error": "nifty_bar_end_missing"}

        # Dedup: skip if this bar_end already logged for ≥1 symbol
        existing = db.execute(
            text(
                """
                SELECT 1 FROM garuda_screener_log
                WHERE session_date = CAST(:d AS date) AND bar_end = :be
                LIMIT 1
                """
            ),
            {"d": session_date, "be": bar_end},
        ).fetchone()
        if existing and not force:
            return {
                "ok": True,
                "skipped": True,
                "reason": "bar_already_logged",
                "bar_end": bar_end.isoformat(),
            }

        nifty_prior = _prior_close(nifty_bars, session_date)
        nifty_day_pct = 0.0
        if nifty_prior and nifty_sess:
            nifty_day_pct = (float(nifty_sess[-1]["close"]) - nifty_prior) / nifty_prior * 100.0
        nifty_win = _window_pct(nifty_bars, session_date, bar_end, RS_WINDOW_BARS) or 0.0

        # Sector index bars (optional cache)
        sector_cache: Dict[str, List[Dict[str, Any]]] = {}

        prepared: List[Tuple[str, GarudaBarContext, Dict[str, Any]]] = []
        cache_hits = 0
        cache_miss = 0
        for sym, ik, sector_key in universe:
            candles = candles_cache_only(ik)
            if not candles:
                cache_miss += 1
                continue
            bars = _bars_from_candles(candles, now=now)
            if not bars:
                cache_miss += 1
                continue
            sess_bars = _session_slice(bars, session_date)
            if not sess_bars:
                cache_miss += 1
                continue
            # Align to nifty bar_end
            idx_global = None
            for i, b in enumerate(bars):
                if b.get("bar_end") == bar_end or (
                    b.get("bar_end") and abs((b["bar_end"] - bar_end).total_seconds()) < 1
                ):
                    idx_global = i
                    break
            if idx_global is None:
                # take last session bar if its bar_end <= nifty bar_end
                last = sess_bars[-1]
                if last["bar_end"] and last["bar_end"] <= bar_end:
                    for i, b in enumerate(bars):
                        if b is last or b.get("bar_end") == last["bar_end"]:
                            idx_global = i
                            break
            if idx_global is None:
                cache_miss += 1
                continue
            cache_hits += 1

            prior_close = _prior_close(bars, session_date)
            atr = atrs.get(sym, 1.0)
            open_px = float(sess_bars[0]["open"]) if sess_bars else None
            gf = (
                _gap_filled(sess_bars, prior_close, cfg.gap_hold_bars, atr)
                if prior_close
                else True
            )

            sec_win = None
            if sector_key:
                if sector_key not in sector_cache:
                    sc = candles_cache_only(sector_key)
                    sector_cache[sector_key] = _bars_from_candles(sc or [], now=now)
                sec_win = _window_pct(sector_cache[sector_key], session_date, bar_end, RS_WINDOW_BARS)

            ctx = GarudaBarContext(
                symbol=sym,
                idx=idx_global,
                bars=bars,
                prior_session_close=prior_close or 0.0,
                nifty_day_pct=nifty_day_pct,
                nifty_window_pct=nifty_win,
                sector_window_pct=sec_win,
                peer_window_pct=None,
                atr_daily_pct=atr,
                beta=None,  # screener falls back to beta=1
                bar_minutes=10,
                session_open_price=open_px,
                gap_filled=gf,
            )
            # Precompute day_rs / roc3 for cross-section (evaluate twice-light)
            closes = [float(b["close"]) for b in bars]
            c = closes[idx_global]
            day_rs = None
            if prior_close:
                day_rs = (c - prior_close) / prior_close * 100.0 - nifty_day_pct
            roc3 = None
            if idx_global >= 3 and closes[idx_global - 3]:
                roc3 = (c - closes[idx_global - 3]) / closes[idx_global - 3] * 100.0
            prepared.append((sym, ctx, {"day_rs": day_rs, "roc3": roc3}))

        day_rs_list = [p[2]["day_rs"] for p in prepared if p[2]["day_rs"] is not None]
        roc3_list = [p[2]["roc3"] for p in prepared if p[2]["roc3"] is not None]
        cross = {"day_rs": day_rs_list, "roc3": roc3_list, "roc3_neg": [-x for x in roc3_list]}

        evals: List[Dict[str, Any]] = []
        for _sym, ctx, _pre in prepared:
            row = evaluate_symbol(ctx, cfg=cfg, cross_section=cross)
            if row:
                evals.append(row)

        ranked = rank_top_n(evals, top_n=TOP_N)
        top_by_sym = {r["symbol"]: r for r in ranked["top_n"]}
        insert_rows: List[Dict[str, Any]] = []
        for r in evals:
            merged = {**r}
            if r["symbol"] in top_by_sym:
                merged["rank"] = top_by_sym[r["symbol"]]["rank"]
            insert_rows.append(_row_for_insert(merged, session_date=session_date, bar_end=bar_end))

        n = _insert_rows(db, insert_rows)
        meta = {
            "ok": True,
            "skipped": False,
            "session_date": session_date,
            "bar_end": bar_end.isoformat(),
            "universe": len(universe),
            "evaluated": len(evals),
            "imbalance_confirmed": ranked["n_imbalance_confirmed"],
            "top6": ranked["top_symbols"],
            "rows_written": n,
            "cache_hits": cache_hits,
            "cache_miss": cache_miss,
        }
        logger.info(
            "garuda screener: bar=%s eval=%s/%s top6=%s cache_miss=%s",
            bar_end.isoformat(),
            len(evals),
            len(universe),
            ranked["top_symbols"],
            cache_miss,
        )
        return meta
    except Exception as exc:
        logger.warning("garuda screener failed: %s", exc, exc_info=True)
        try:
            db.rollback()
        except Exception:
            pass
        return {"ok": False, "error": str(exc)}
    finally:
        db.close()


def get_latest_top6(session_date: Optional[str] = None) -> Dict[str, Any]:
    """Latest Top-6 snapshot for UI (read shadow table)."""
    ensure_garuda_screener_log()
    sd = session_date or _session_date_ist()
    db = SessionLocal()
    try:
        latest = db.execute(
            text(
                """
                SELECT bar_end
                FROM garuda_screener_log
                WHERE session_date = CAST(:d AS date)
                ORDER BY bar_end DESC
                LIMIT 1
                """
            ),
            {"d": sd},
        ).fetchone()
        if not latest:
            return {
                "session_date": sd,
                "bar_end": None,
                "top_n": [],
                "warning": (
                    "TESTING IN PROGRESS — Garuda is unvalidated. "
                    "Forward-performance testing has not been completed. "
                    "Do not use for trade decisions."
                ),
                "empty": True,
            }
        bar_end = latest.bar_end
        rows = db.execute(
            text(
                """
                SELECT symbol, side, top6_rank, imbalance_confirmed, imbalance_side,
                       imbalance_hits, imbalance_legs,
                       direction_agreement, direction_side, direction,
                       day_rs, strength_percentile,
                       trend_adx, trend_adx_slope, trend_er,
                       momentum_percentile, momentum, rank_score, price, components
                FROM garuda_screener_log
                WHERE session_date = CAST(:d AS date)
                  AND bar_end = :be
                  AND top6_rank IS NOT NULL
                ORDER BY top6_rank ASC
                """
            ),
            {"d": sd, "be": bar_end},
        ).fetchall()
        top_n = []
        for r in rows:
            top_n.append(
                {
                    "symbol": r.symbol,
                    "side": r.side,
                    "rank": r.top6_rank,
                    "imbalance_confirmed": r.imbalance_confirmed,
                    "imbalance_side": r.imbalance_side,
                    "imbalance_hits": r.imbalance_hits,
                    "imbalance_legs": r.imbalance_legs,
                    "direction": {
                        "agreement": r.direction_agreement,
                        "side": r.direction_side,
                        **(r.direction if isinstance(r.direction, dict) else {}),
                    },
                    "strength": {
                        "day_rs": r.day_rs,
                        "percentile": r.strength_percentile,
                    },
                    "trend": {
                        "adx": r.trend_adx,
                        "adx_slope": r.trend_adx_slope,
                        "efficiency_ratio": r.trend_er,
                    },
                    "momentum": r.momentum
                    if isinstance(r.momentum, dict)
                    else {"percentile_roc3": r.momentum_percentile},
                    "rank_score": r.rank_score,
                    "price": r.price,
                }
            )
        try:
            from backend.services.fo_display_symbol import attach_future_symbols

            attach_future_symbols(top_n, db=db)
        except Exception as exc:
            logger.debug("garuda FO display enrichment failed: %s", exc)
        return {
            "session_date": sd,
            "bar_end": bar_end.isoformat() if hasattr(bar_end, "isoformat") else str(bar_end),
            "top_n": top_n,
            "n": len(top_n),
            "empty": len(top_n) == 0,
            "warning": (
                "TESTING IN PROGRESS — Garuda is unvalidated. "
                "Forward-performance testing has not been completed. "
                "Do not use for trade decisions."
            ),
        }
    finally:
        db.close()


def lookup_garuda_confluence(
    symbol: str,
    direction: str,
    entry_at: datetime,
    *,
    session_date: Optional[str] = None,
) -> Dict[str, Any]:
    """Read-only Top-6 confluence at nearest bar_end ≤ entry_at. Never gates."""
    ensure_garuda_screener_log()
    sym = (symbol or "").strip().upper()
    direction = (direction or "").strip().upper()
    if entry_at.tzinfo is None:
        entry_at = IST.localize(entry_at)
    else:
        entry_at = entry_at.astimezone(IST)
    sd = session_date or entry_at.strftime("%Y-%m-%d")

    out: Dict[str, Any] = {
        "garuda_confluence": CONFLUENCE_NOT_AVAILABLE,
        "garuda_rank": None,
        "garuda_direction": None,
        "garuda_bar_end": None,
    }
    if not sym or direction not in ("LONG", "SHORT"):
        return out

    db = SessionLocal()
    try:
        bar = db.execute(
            text(
                """
                SELECT DISTINCT bar_end
                FROM garuda_screener_log
                WHERE session_date = CAST(:d AS date)
                  AND bar_end <= :et
                ORDER BY bar_end DESC
                LIMIT 1
                """
            ),
            {"d": sd, "et": entry_at},
        ).fetchone()
        if not bar:
            return out
        bar_end = bar.bar_end
        out["garuda_bar_end"] = bar_end.isoformat() if hasattr(bar_end, "isoformat") else str(bar_end)

        # Symbol's row at that bar (may or may not be Top-6)
        own = db.execute(
            text(
                """
                SELECT side, top6_rank
                FROM garuda_screener_log
                WHERE session_date = CAST(:d AS date)
                  AND bar_end = :be
                  AND UPPER(symbol) = :sym
                LIMIT 1
                """
            ),
            {"d": sd, "be": bar_end, "sym": sym},
        ).fetchone()

        # Top-6 set for direction match
        top = db.execute(
            text(
                """
                SELECT symbol, side, top6_rank
                FROM garuda_screener_log
                WHERE session_date = CAST(:d AS date)
                  AND bar_end = :be
                  AND top6_rank IS NOT NULL
                """
            ),
            {"d": sd, "be": bar_end},
        ).fetchall()
        if not top:
            return out

        match = None
        for r in top:
            if str(r.symbol).upper() == sym:
                match = r
                break

        if own is not None:
            out["garuda_direction"] = own.side
            out["garuda_rank"] = int(own.top6_rank) if own.top6_rank is not None else None
        if match is not None:
            out["garuda_direction"] = match.side
            out["garuda_rank"] = int(match.top6_rank) if match.top6_rank is not None else None
            if str(match.side or "").upper() == direction:
                out["garuda_confluence"] = CONFLUENCE_MATCH
            else:
                out["garuda_confluence"] = CONFLUENCE_NO_MATCH
        else:
            # Not in Top-6 — still record own side if present
            if out["garuda_direction"] is None and own is not None:
                out["garuda_direction"] = own.side
            out["garuda_confluence"] = CONFLUENCE_NO_MATCH
            out["garuda_rank"] = None
        return out
    except Exception as exc:
        logger.debug("garuda confluence lookup failed: %s", exc)
        return out
    finally:
        db.close()

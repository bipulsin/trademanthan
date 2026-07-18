"""Shadow Confidence component audit + Structural Alignment Score.

Research-only instrumentation. Does not change live grades, READY gates, or cards.
Logged on each locked-symbol 10m recompute (checklist 5m refresh cadence).
"""
from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import pytz
from sqlalchemy import text

from backend.database import SessionLocal, engine

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

# EMA5 proximity threshold — same as READY convergence_atr default (conviction config).
EMA5_PROX_ATR = 0.35
# |vwap_extension_pct| in (0, this] counts as Ext/Sweet (not a single touch at 0).
VWAP_EXT_SWEET_MAX = 0.015  # 1.5%
ADX_ALIGN_MIN = 20.0

_ENSURED = False


def ensure_confidence_audit_tables() -> None:
    global _ENSURED
    if _ENSURED:
        return
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS kavach_confidence_component_log (
                    id SERIAL PRIMARY KEY,
                    session_date DATE NOT NULL,
                    symbol VARCHAR(32) NOT NULL,
                    direction VARCHAR(8),
                    logged_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    bar_at TIMESTAMPTZ,
                    source VARCHAR(32) NOT NULL DEFAULT 'live',
                    trade_score INTEGER,
                    confidence_grade VARCHAR(8),
                    banding_rule VARCHAR(64),
                    volume_label VARCHAR(16),
                    purity_pct NUMERIC(8,2),
                    regime VARCHAR(16),
                    components JSONB NOT NULL DEFAULT '{}'::jsonb,
                    structural JSONB NOT NULL DEFAULT '{}'::jsonb
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS idx_conf_comp_log_session_sym
                ON kavach_confidence_component_log (session_date, symbol, logged_at)
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS kavach_structural_alignment_log (
                    id SERIAL PRIMARY KEY,
                    session_date DATE NOT NULL,
                    symbol VARCHAR(32) NOT NULL,
                    direction VARCHAR(8),
                    logged_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    bar_at TIMESTAMPTZ,
                    source VARCHAR(32) NOT NULL DEFAULT 'live',
                    confidence_grade VARCHAR(8),
                    trade_score INTEGER,
                    alignment_score INTEGER,
                    alignment_max INTEGER NOT NULL DEFAULT 5,
                    persist_bars INTEGER NOT NULL DEFAULT 0,
                    aligned BOOLEAN NOT NULL DEFAULT FALSE,
                    dimensions JSONB NOT NULL DEFAULT '{}'::jsonb,
                    ema5_prox_atr NUMERIC(12,6),
                    ema5_prox_threshold NUMERIC(12,6),
                    vwap_extension_pct NUMERIC(12,6),
                    adx NUMERIC(12,4)
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS idx_struct_align_log_session_sym
                ON kavach_structural_alignment_log (session_date, symbol, logged_at)
                """
            )
        )
    _ENSURED = True


def build_component_payload(
    metrics: Dict[str, Any],
    *,
    direction: str,
    atr_pct: float = 1.0,
    candles: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """Trade Score breakdown + Confidence banding explanation from live metrics."""
    from backend.services.kavach_confidence import explain_confidence_grade
    from backend.services.kavach_engine import trade_score_breakdown

    ranking = metrics.get("ranking_type") or (
        "BEARISH" if (direction or "").upper() == "SHORT" else "BULLISH"
    )
    breakdown = trade_score_breakdown(
        rs=float(metrics.get("relative_strength") or 0.0),
        state=str(metrics.get("kavach_state") or "NEUTRAL"),
        volume_ratio=float(metrics.get("volume_ratio") or 0.0),
        adx=float(metrics.get("adx") or 0.0),
        price=float(metrics.get("price") or 0.0),
        vwap=float(metrics.get("vwap") or 0.0),
        ranking_type=ranking,
    )
    ema10 = metrics.get("ema10_10m")
    if ema10 is None:
        ema10 = metrics.get("ema10")
    raw_score = float(
        metrics.get("trade_score_raw")
        if metrics.get("trade_score_raw") is not None
        else (metrics.get("trade_score") or breakdown["trade_score"])
    )
    grade_x = explain_confidence_grade(
        raw_score,
        metrics.get("volume_label") or "Low",  # type: ignore[arg-type]
        float(metrics.get("vwap_purity_pct") or 0.0),
        str(metrics.get("market_regime") or "TREND"),
        close=metrics.get("price"),
        ema10=ema10,
        vwap=metrics.get("vwap"),
    )
    stretch = grade_x.get("stretch") or {}
    # Panel / structure labels (not Trade Score buckets — logged for TV parity review)
    breakdown["panel_trend"] = metrics.get("panel_trend")
    breakdown["supertrend"] = metrics.get("supertrend")
    breakdown["macd"] = metrics.get("macd")
    breakdown["macd_signal"] = metrics.get("macd_signal")
    breakdown["macd_histogram"] = metrics.get("macd_histogram")
    breakdown["price"] = metrics.get("price")
    breakdown["ema5"] = metrics.get("ema5")
    breakdown["ema10"] = ema10
    breakdown["vwap"] = metrics.get("vwap")
    breakdown["stretch"] = stretch
    breakdown["trade_score_pre_stretch"] = stretch.get("trade_score_pre_stretch")
    breakdown["trade_score_post_stretch"] = stretch.get("trade_score_post_stretch")
    breakdown["note"] = (
        "Trade Score = rs+kavach+volume+adx+vwap_side (+optional persist). "
        "Confidence grade adds volume_label + VWAP purity≥60% + TRANSITION floor — "
        "volume can suppress grade even when structure is aligned. "
        "Stretch penalty (Pine v13) is shadow-logged; live write gated by STRETCH_PENALTY_LIVE."
    )
    return {
        "components": breakdown,
        "grade": grade_x,
        "trade_score": grade_x.get("score_int", breakdown["trade_score"]),
        "confidence_grade": grade_x.get("display_grade") or grade_x.get("grade"),
        "stretch": stretch,
    }


def compute_structural_alignment(
    metrics: Dict[str, Any],
    *,
    direction: str,
    atr_pct: float,
    candles: Optional[List[Dict[str, Any]]] = None,
    persist_bars_prev: int = 0,
) -> Dict[str, Any]:
    """Shadow Structural Alignment Score (0–5 dims) + consecutive persist count.

    Dimensions (all must be true for aligned=True):
      1. VWAP Gate: steep_ok AND not flip_flop/unstable (quality_pass-style 2/2)
      2. VWAP Persist Ext/Sweet: |extension| in (0, 1.5%] OR steep persist ≥2 bars
      3. EMA5 proximity ≤ 0.35 ATR (READY convergence_atr default)
      4. Supertrend aligned with trade direction
      5. ADX > 20
    """
    from backend.services.rs_vwap_quality import (
        consecutive_steep_bars,
        score_vwap_quality,
        vwap_extension_pct,
    )

    side = "SHORT" if (direction or "").upper() == "SHORT" else "LONG"
    is_long = side == "LONG"
    price = float(metrics.get("price") or 0.0)
    ema5 = float(metrics.get("ema5") or 0.0)
    adx = float(metrics.get("adx") or 0.0)
    st = metrics.get("supertrend")
    try:
        st_f = float(st) if st is not None else 0.0
    except (TypeError, ValueError):
        st_f = 0.0
    st_aligned = (st_f > 0 and is_long) or (st_f < 0 and not is_long)

    atr_abs = (price * float(atr_pct) / 100.0) if price > 0 and atr_pct > 0 else None
    ema5_prox = (abs(price - ema5) / atr_abs) if atr_abs and atr_abs > 0 else None
    ema5_ok = ema5_prox is not None and ema5_prox <= EMA5_PROX_ATR

    vq: Dict[str, Any] = {}
    ext = None
    steep_persist = 0
    if candles:
        vq = score_vwap_quality(
            candles,
            side=side,
            atr_daily_pct=atr_pct if atr_pct > 0 else 1.0,
        )
        ext = vwap_extension_pct(candles)
        persist = consecutive_steep_bars(
            candles,
            atr_daily_pct=atr_pct if atr_pct > 0 else 1.0,
            n_bars=3,
        )
        if persist.get("ok"):
            steep_persist = int(persist.get("count") or 0)
        else:
            # count how many of last 2 bars are steep in direction
            soft = consecutive_steep_bars(
                candles,
                atr_daily_pct=atr_pct if atr_pct > 0 else 1.0,
                n_bars=2,
            )
            steep_persist = int(soft.get("count") or 0) if soft.get("ok") else 0

    gate_ok = bool(vq.get("quality_pass")) if vq else False
    if not vq and metrics.get("vwap_purity_pct") is not None:
        # Fallback when candles missing: purity ≥60% as weak gate proxy
        gate_ok = float(metrics.get("vwap_purity_pct") or 0) >= 60.0

    ext_f = float(ext) if ext is not None else None
    if is_long:
        ext_sweet = ext_f is not None and 0 < ext_f <= VWAP_EXT_SWEET_MAX
    else:
        ext_sweet = ext_f is not None and -VWAP_EXT_SWEET_MAX <= ext_f < 0
    persist_ok = steep_persist >= 2 or bool(ext_sweet)
    adx_ok = adx > ADX_ALIGN_MIN

    dims = {
        "vwap_gate": gate_ok,
        "vwap_persist_ext_sweet": persist_ok,
        "ema5_proximity": ema5_ok,
        "supertrend_aligned": st_aligned,
        "adx_gt_20": adx_ok,
        "steep_ok": vq.get("steep_ok"),
        "flip_flop": vq.get("flip_flop"),
        "unstable": vq.get("unstable"),
        "quality_pass": vq.get("quality_pass"),
        "steep_persist_bars": steep_persist,
        "vwap_extension_pct": ext_f,
        "ema5_prox_atr": round(ema5_prox, 4) if ema5_prox is not None else None,
        "ema5_prox_threshold": EMA5_PROX_ATR,
        "vwap_ext_sweet_max": VWAP_EXT_SWEET_MAX,
    }
    keys = (
        "vwap_gate",
        "vwap_persist_ext_sweet",
        "ema5_proximity",
        "supertrend_aligned",
        "adx_gt_20",
    )
    score = sum(1 for k in keys if dims.get(k))
    aligned = score == len(keys)
    # Consecutive recheck persistence: +1 if still aligned, else reset
    if aligned:
        persist_bars = int(persist_bars_prev or 0) + 1
    else:
        persist_bars = 0

    return {
        "alignment_score": score,
        "alignment_max": len(keys),
        "aligned": aligned,
        "persist_bars": persist_bars,
        "dimensions": dims,
        "ema5_prox_atr": dims["ema5_prox_atr"],
        "ema5_prox_threshold": EMA5_PROX_ATR,
        "vwap_extension_pct": ext_f,
        "adx": adx,
    }


def _last_struct_persist(db, session_date: str, symbol: str) -> int:
    row = db.execute(
        text(
            """
            SELECT persist_bars, aligned
            FROM kavach_structural_alignment_log
            WHERE session_date = CAST(:d AS date) AND UPPER(symbol) = :sym
            ORDER BY logged_at DESC, id DESC
            LIMIT 1
            """
        ),
        {"d": session_date, "sym": symbol.upper()},
    ).fetchone()
    if not row or not row.aligned:
        return 0
    try:
        return int(row.persist_bars or 0)
    except (TypeError, ValueError):
        return 0


def log_confidence_and_structural(
    db,
    *,
    session_date: str,
    symbol: str,
    direction: str,
    metrics: Dict[str, Any],
    candles: Optional[List[Dict[str, Any]]] = None,
    atr_pct: float = 1.0,
    source: str = "live",
    logged_at: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Insert component audit + structural alignment shadow rows."""
    ensure_confidence_audit_tables()
    sym = (symbol or "").upper()
    now = logged_at or datetime.now(IST)
    if now.tzinfo is None:
        now = IST.localize(now)
    bar_at = metrics.get("bar_evaluated_at") or metrics.get("bar_end") or now
    if isinstance(bar_at, datetime) and bar_at.tzinfo is None:
        bar_at = IST.localize(bar_at)

    payload = build_component_payload(
        metrics, direction=direction, atr_pct=atr_pct, candles=candles
    )
    prev_persist = _last_struct_persist(db, session_date, sym)
    structural = compute_structural_alignment(
        metrics,
        direction=direction,
        atr_pct=atr_pct,
        candles=candles,
        persist_bars_prev=prev_persist,
    )

    try:
        db.execute(
            text(
                """
                INSERT INTO kavach_confidence_component_log (
                    session_date, symbol, direction, logged_at, bar_at, source,
                    trade_score, confidence_grade, banding_rule, volume_label,
                    purity_pct, regime, components, structural
                ) VALUES (
                    CAST(:d AS date), :sym, :dir, :lat, :bat, :src,
                    :ts, :cg, :br, :vl,
                    :pp, :reg, CAST(:comp AS jsonb), CAST(:struct AS jsonb)
                )
                """
            ),
            {
                "d": session_date,
                "sym": sym,
                "dir": (direction or "LONG").upper(),
                "lat": now,
                "bat": bar_at,
                "src": source,
                "ts": payload.get("trade_score"),
                "cg": payload.get("confidence_grade"),
                "br": (payload.get("grade") or {}).get("banding_rule"),
                "vl": (payload.get("grade") or {}).get("volume_label"),
                "pp": (payload.get("grade") or {}).get("purity_pct"),
                "reg": (payload.get("grade") or {}).get("regime"),
                "comp": json.dumps(payload.get("components") or {}),
                "struct": json.dumps(structural),
            },
        )
        db.execute(
            text(
                """
                INSERT INTO kavach_structural_alignment_log (
                    session_date, symbol, direction, logged_at, bar_at, source,
                    confidence_grade, trade_score, alignment_score, alignment_max,
                    persist_bars, aligned, dimensions,
                    ema5_prox_atr, ema5_prox_threshold, vwap_extension_pct, adx
                ) VALUES (
                    CAST(:d AS date), :sym, :dir, :lat, :bat, :src,
                    :cg, :ts, :ascore, :amax,
                    :pb, :al, CAST(:dims AS jsonb),
                    :eprox, :ethr, :vext, :adx
                )
                """
            ),
            {
                "d": session_date,
                "sym": sym,
                "dir": (direction or "LONG").upper(),
                "lat": now,
                "bat": bar_at,
                "src": source,
                "cg": payload.get("confidence_grade"),
                "ts": payload.get("trade_score"),
                "ascore": structural.get("alignment_score"),
                "amax": structural.get("alignment_max"),
                "pb": structural.get("persist_bars"),
                "al": bool(structural.get("aligned")),
                "dims": json.dumps(structural.get("dimensions") or {}),
                "eprox": structural.get("ema5_prox_atr"),
                "ethr": structural.get("ema5_prox_threshold"),
                "vext": structural.get("vwap_extension_pct"),
                "adx": structural.get("adx"),
            },
        )
    except Exception as exc:
        logger.warning("confidence/structural audit log failed %s: %s", sym, exc)

    return {"components": payload, "structural": structural}


def backfill_symbol_session(
    symbol: str,
    session_date: str,
    *,
    direction: str = "LONG",
    start_hm: str = "09:45",
    end_hm: str = "12:45",
) -> Dict[str, Any]:
    """Replay 10m metrics for each closed bar in [start, end] and shadow-log.

    Uses Upstox historical 5m (same as live recompute). Shadow-only.
    """
    from datetime import date, time, timedelta

    from backend.config import settings
    from backend.services.daily_checklist_live import _latest_nifty_pct, _ranking_for_direction
    from backend.services.kavach_10m import metrics_from_10m_candles
    from backend.services.kavach_universe_vwap_scan import _atr_map
    from backend.services.rs_conviction_candles import load_instrument_atr_maps
    from backend.services.relative_strength_scanner import (
        CANDLE_DAYS_BACK,
        CANDLE_INTERVAL,
        MIN_BARS,
        _sorted_candles,
    )
    from backend.services.upstox_service import UpstoxService

    ensure_confidence_audit_tables()
    sym = symbol.upper()
    d = date.fromisoformat(session_date)
    sh, sm = map(int, start_hm.split(":"))
    eh, em = map(int, end_hm.split(":"))
    start = IST.localize(datetime.combine(d, time(sh, sm)))
    end = IST.localize(datetime.combine(d, time(eh, em)))

    db = SessionLocal()
    try:
        ikey_map, _ = load_instrument_atr_maps(db, {sym})
        ikey = ikey_map.get(sym)
        if not ikey:
            return {"ok": False, "error": "no_instrument_key"}
        atr = float((_atr_map(db, [sym]) or {}).get(sym) or 1.0)
        nifty_pct = _latest_nifty_pct(db)
        ranking = _ranking_for_direction(direction)
        raw = UpstoxService(
            settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET
        ).get_historical_candles_by_instrument_key(
            ikey,
            interval=CANDLE_INTERVAL,
            days_back=max(CANDLE_DAYS_BACK, 5),
            range_end_date=d,
        )
        if not raw or len(raw) < MIN_BARS:
            return {"ok": False, "error": "candle_fetch_failed"}
        candles = _sorted_candles(raw)

        # Clear prior backfill for this symbol/day so re-runs are clean
        db.execute(
            text(
                """
                DELETE FROM kavach_confidence_component_log
                WHERE session_date = CAST(:d AS date) AND UPPER(symbol) = :sym
                  AND source = 'backfill'
                """
            ),
            {"d": session_date, "sym": sym},
        )
        db.execute(
            text(
                """
                DELETE FROM kavach_structural_alignment_log
                WHERE session_date = CAST(:d AS date) AND UPPER(symbol) = :sym
                  AND source = 'backfill'
                """
            ),
            {"d": session_date, "sym": sym},
        )
        db.commit()

        # Step every 10 minutes through window (closed 10m bar ends)
        n = 0
        cur = start
        rows_out: List[Dict[str, Any]] = []
        while cur <= end:
            m = metrics_from_10m_candles(
                candles, ranking_type=ranking, nifty_pct=nifty_pct, now=cur
            )
            if m:
                # Truncate candles for structural slope/extension as of cur
                from backend.services.rs_vwap_quality import _parse_ts

                sliced = [
                    c
                    for c in candles
                    if (_parse_ts(c.get("timestamp")) or cur) <= cur
                ]
                out = log_confidence_and_structural(
                    db,
                    session_date=session_date,
                    symbol=sym,
                    direction=direction,
                    metrics=m,
                    candles=sliced,
                    atr_pct=atr,
                    source="backfill",
                    logged_at=cur,
                )
                rows_out.append(
                    {
                        "at": cur.isoformat(),
                        "trade_score": out["components"].get("trade_score"),
                        "grade": out["components"].get("confidence_grade"),
                        "banding": (out["components"].get("grade") or {}).get(
                            "banding_rule"
                        ),
                        "align": out["structural"].get("alignment_score"),
                        "persist": out["structural"].get("persist_bars"),
                        "vol": (out["components"].get("grade") or {}).get(
                            "volume_label"
                        ),
                        "components": out["components"].get("components"),
                    }
                )
                n += 1
            cur += timedelta(minutes=10)
        db.commit()
        return {"ok": True, "symbol": sym, "rows": n, "samples": rows_out}
    except Exception as exc:
        logger.exception("backfill confidence audit failed")
        try:
            db.rollback()
        except Exception:
            pass
        return {"ok": False, "error": str(exc)}
    finally:
        db.close()

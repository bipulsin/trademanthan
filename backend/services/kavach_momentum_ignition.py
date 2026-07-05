"""Kavach Momentum Ignition — leading 15–30 min momentum likelihood layer (Phase 1).

Combines WS order-flow + OI triangulation (highest tier) with reused 5m conviction
signals and new pullback-depth logic. Feeds conviction board when enabled; surfaces
on daily checklist when validation gate passes (``ignition_ui_enabled``).
"""
from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pytz
from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.rs_conviction_candles import candles_cache_only, load_instrument_atr_maps
from backend.services.rs_conviction_config import get_config
from backend.services.rs_conviction_signals import (
    accumulation_signal,
    compute_symbol_signals,
    normalized_vwap_slope,
)

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

SIDE_BULL = "BULL"
SIDE_BEAR = "BEAR"


def today_ist() -> str:
    return datetime.now(IST).strftime("%Y-%m-%d")


def _is_bull_side(side: str) -> bool:
    return (side or "").upper() in (SIDE_BULL, "LONG", "BULLISH")


def _f(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def get_fii_dii_multiplier(session_date: str) -> float:
    """Low-weight contextual multiplier from page-level FII/DII field (1.0 if unset)."""
    db = SessionLocal()
    try:
        row = db.execute(
            text(
                "SELECT fii_dii_flow FROM daily_checklist WHERE session_date = :d "
                "AND fii_dii_flow IS NOT NULL AND TRIM(fii_dii_flow) <> '' LIMIT 1"
            ),
            {"d": session_date},
        ).fetchone()
    except Exception as exc:
        logger.debug("fii_dii lookup failed: %s", exc)
        return 1.0
    finally:
        db.close()
    if not row or not row.fii_dii_flow:
        return 1.0
    val = str(row.fii_dii_flow).strip().lower()
    if "buy" in val or "long" in val:
        return 1.08
    if "sell" in val or "short" in val:
        return 0.92
    if "neutral" in val or "mixed" in val:
        return 1.0
    return 1.0


def order_flow_scores(instrument_key: str, side: str) -> Tuple[float, float, Dict[str, Any]]:
    """Depth imbalance + tbq/tsq pressure → 0–100 scores (highest tier)."""
    from backend.services.upstox_market_feed import get_ws_feed_row

    row = get_ws_feed_row(instrument_key)
    if not row:
        return 0.0, 0.0, {}
    bull = _is_bull_side(side)
    depth_ratio = _f(row.get("depth_imbalance_ratio"), 1.0)
    pressure = _f(row.get("pressure_ratio"), 1.0)
    # Map ratio to 0–100 favoring the trade direction
    if bull:
        depth_score = max(0.0, min(100.0, (depth_ratio - 0.8) / 0.8 * 100.0))
        pressure_score = max(0.0, min(100.0, (pressure - 0.9) / 0.9 * 100.0))
    else:
        depth_score = max(0.0, min(100.0, (1.0 / max(depth_ratio, 0.01) - 0.8) / 0.8 * 100.0))
        pressure_score = max(0.0, min(100.0, (1.0 / max(pressure, 0.01) - 0.9) / 0.9 * 100.0))
    meta = {
        "bid_depth_qty": row.get("bid_depth_qty"),
        "ask_depth_qty": row.get("ask_depth_qty"),
        "depth_imbalance_ratio": round(depth_ratio, 3),
        "tbq": row.get("tbq"),
        "tsq": row.get("tsq"),
        "pressure_ratio": round(pressure, 3),
        "oi_change_tick": row.get("oi_change"),
    }
    return round(depth_score, 2), round(pressure_score, 2), meta


def classify_oi_price_volume(
    price_pct: float, oi_change: int, volume_ratio: float, side: str
) -> Tuple[str, float]:
    """WS tick OI vs price/volume triangulation → label + score 0–100."""
    bull = _is_bull_side(side)
    oi_up = oi_change > 0
    oi_dn = oi_change < 0
    px_up = price_pct > 0.05
    px_dn = price_pct < -0.05
    vol_ok = volume_ratio >= 1.2

    if bull and px_up and oi_up and vol_ok:
        return "LONG_BUILDUP", 90.0
    if bull and px_up and oi_dn:
        return "SHORT_COVERING", 75.0
    if bull and px_dn and oi_up and vol_ok:
        return "DISTRIBUTION", 20.0
    if not bull and px_dn and oi_up and vol_ok:
        return "SHORT_BUILDUP", 90.0
    if not bull and px_dn and oi_dn:
        return "LONG_UNWIND", 75.0
    if not bull and px_up and oi_up and vol_ok:
        return "SHORT_SQUEEZE_RISK", 25.0
    if oi_up and vol_ok:
        return "ACCUMULATION", 55.0
    return "NEUTRAL", 40.0


def oi_triangulation_score(
    instrument_key: str, side: str, candles: Optional[List[Dict]], atr_pct: float
) -> Tuple[float, str, Dict[str, Any]]:
    from backend.services.upstox_market_feed import get_ws_feed_row

    row = get_ws_feed_row(instrument_key)
    if not row or not candles or len(candles) < 5:
        return 0.0, "NO_DATA", {}
    closed = candles[-2] if len(candles) >= 2 else candles[-1]
    prev = candles[-3] if len(candles) >= 3 else closed
    price_pct = (_f(closed.get("close")) - _f(prev.get("close"))) / max(_f(prev.get("close")), 1.0) * 100.0
    oi_chg = int(row.get("oi_change") or 0)
    vols = [_f(c.get("volume")) for c in candles[-12:]]
    session_avg = sum(vols) / max(len(vols), 1)
    vol_ratio = (vols[-1] / session_avg) if session_avg > 0 else 1.0
    label, score = classify_oi_price_volume(price_pct, oi_chg, vol_ratio, side)
    return score, label, {"oi_change": oi_chg, "price_pct_5m": round(price_pct, 3), "vol_ratio": round(vol_ratio, 2)}


def pullback_depth_contraction(
    candles: Optional[List[Dict]], side: str, atr_pct: float
) -> Tuple[float, Dict[str, Any]]:
    """Shrinking pullbacks toward EMA10 reference → continuation score 0–100."""
    from backend.services.relative_strength_scanner import _f as rf, _parse_ist_date
    from backend.services.rs_conviction_signals import ema10_10min

    if not candles or len(candles) < 20:
        return 0.0, {}
    last_date = _parse_ist_date(candles[-1].get("timestamp"))
    first_today = 0
    for i, c in enumerate(candles):
        if _parse_ist_date(c.get("timestamp")) == last_date:
            first_today = i
            break
    today = candles[first_today:]
    if len(today) < 12:
        return 0.0, {}
    ref = ema10_10min(candles)
    if ref is None:
        return 0.0, {}
    price = rf(candles[-1].get("close"), 1.0)
    atr = price * max(atr_pct, 0.001) / 100.0
    if atr <= 0:
        return 0.0, {}
    bull = _is_bull_side(side)
    pullbacks: List[float] = []
    closes = [rf(c.get("close")) for c in today]
    for i in range(2, len(closes) - 1):
        c = closes[i]
        dist = abs(c - ref) / atr
        if dist > 0.15:
            if bull and c < ref and closes[i + 1] > c:
                pullbacks.append(dist)
            elif not bull and c > ref and closes[i + 1] < c:
                pullbacks.append(dist)
    if len(pullbacks) < 2:
        return 0.0, {"pullback_count": len(pullbacks)}
    shrinking = all(pullbacks[j] <= pullbacks[j - 1] * 1.05 for j in range(1, len(pullbacks)))
    deepening = pullbacks[-1] > pullbacks[0] * 1.2
    if shrinking and not deepening:
        return min(100.0, 60.0 + (len(pullbacks) - 2) * 15.0), {
            "pullback_depths_atr": [round(p, 2) for p in pullbacks[-3:]],
            "pattern": "CONTRACTING",
        }
    if deepening:
        return 15.0, {"pullback_depths_atr": [round(p, 2) for p in pullbacks[-3:]], "pattern": "DEEPENING"}
    return 35.0, {"pullback_depths_atr": [round(p, 2) for p in pullbacks[-3:]], "pattern": "MIXED"}


def coincident_confirmation(candles: Optional[List[Dict]], side: str) -> Tuple[float, Dict[str, Any]]:
    """One-sided closes + range expansion with volume surge (medium tier)."""
    if not candles or len(candles) < 8:
        return 0.0, {}
    window = candles[-6:]
    bull = _is_bull_side(side)
    hits = 0
    ranges: List[float] = []
    vols = [_f(c.get("volume")) for c in window]
    for c in window:
        h, l, cl = _f(c.get("high")), _f(c.get("low")), _f(c.get("close"))
        rng = h - l
        if rng <= 0:
            continue
        ranges.append(rng)
        pos = (cl - l) / rng
        if bull and pos >= 0.72:
            hits += 1
        elif not bull and pos <= 0.28:
            hits += 1
    range_exp = False
    if len(ranges) >= 3:
        range_exp = ranges[-1] > sum(ranges[:-1]) / max(len(ranges) - 1, 1) * 1.3
    vol_surge = vols[-1] > (sum(vols[:-1]) / max(len(vols) - 1, 1)) * 1.4 if vols else False
    score = min(100.0, hits * 18.0 + (25.0 if range_exp else 0) + (20.0 if vol_surge else 0))
    return round(score, 2), {"one_sided_bars": hits, "range_expansion": range_exp, "volume_surge": vol_surge}


def compute_ignition(
    symbol: str,
    side: str,
    instrument_key: str,
    *,
    candles: Optional[List[Dict]] = None,
    atr_pct: float = 1.0,
    cfg: Optional[Dict[str, Any]] = None,
    session_date: Optional[str] = None,
) -> Dict[str, Any]:
    """Full ignition composite for one symbol."""
    cfg = cfg or get_config()
    sd = session_date or today_ist()
    if candles is None:
        candles = candles_cache_only(instrument_key)

    depth_sc, pressure_sc, of_meta = order_flow_scores(instrument_key, side)
    orderflow_sc = round((depth_sc * 0.55 + pressure_sc * 0.45), 2)

    oi_sc, oi_label, oi_meta = oi_triangulation_score(instrument_key, side, candles, atr_pct)

    sig = compute_symbol_signals(candles, side=side, atr_daily_pct=atr_pct, cfg=cfg)
    accum_sc, _, _ = accumulation_signal(candles or [], side, cfg) if candles else (0.0, False, True)
    slope_sc = normalized_vwap_slope(candles or [], atr_pct, cfg) if candles else 0.0
    pullback_sc, pb_meta = pullback_depth_contraction(candles, side, atr_pct)
    confirm_sc, confirm_meta = coincident_confirmation(candles, side)

    w_of = float(cfg.get("W_ignition_orderflow") or 0.35)
    w_oi = float(cfg.get("W_ignition_oi_tri") or 0.25)
    w_acc = float(cfg.get("W_ignition_absorption") or 0.15)
    w_slope = float(cfg.get("W_ignition_slope") or 0.10)
    w_pb = float(cfg.get("W_ignition_pullback") or 0.10)
    w_conf = float(cfg.get("W_ignition_confirm") or 0.05)

    raw = (
        w_of * orderflow_sc
        + w_oi * oi_sc
        + w_acc * accum_sc
        + w_slope * slope_sc
        + w_pb * pullback_sc
        + w_conf * confirm_sc
    )
    fii_mult = get_fii_dii_multiplier(sd)
    ctx_w = float(cfg.get("W_ignition_fii_context") or 0.05)
    # Context: blend multiplier toward 1.0 by (1 - ctx_w) + ctx_w * mult
    blend = (1.0 - ctx_w) + ctx_w * fii_mult
    score = max(0.0, min(100.0, raw * blend))

    threshold = float(cfg.get("ignition_flag_threshold") or 65)
    flag = score >= threshold

    return {
        "symbol": symbol,
        "side": side,
        "ignition_score": round(score, 2),
        "ignition_building": flag,
        "components": {
            "orderflow": orderflow_sc,
            "oi_triangulation": oi_sc,
            "oi_label": oi_label,
            "absorption": accum_sc,
            "vwap_slope": round(slope_sc, 2),
            "pullback": pullback_sc,
            "confirmation": confirm_sc,
            "fii_dii_multiplier": round(fii_mult, 3),
        },
        "meta": {"orderflow": of_meta, "oi": oi_meta, "pullback": pb_meta, "confirm": confirm_meta},
        "accum_active": sig.get("accum_active"),
    }


def run_ignition_cycle(symbols_sides: List[Tuple[str, str]]) -> Dict[str, Any]:
    """Compute ignition for checklist/core symbols; persist log for forward validation."""
    if not symbols_sides:
        return {"ok": True, "updated": 0}
    cfg = get_config()
    sd = today_ist()
    now = datetime.now(IST)
    db = SessionLocal()
    updated = 0
    results: Dict[str, Dict[str, Any]] = {}
    try:
        sym_set = {s for s, _ in symbols_sides}
        ikey_map, atr_map = load_instrument_atr_maps(db, sym_set)
        for sym, side in symbols_sides:
            ik = ikey_map.get(sym, "")
            payload = compute_ignition(
                sym, side, ik,
                atr_pct=atr_map.get(sym, 1.0),
                cfg=cfg,
                session_date=sd,
            )
            results[sym] = payload
            db.execute(
                text(
                    """
                    INSERT INTO rs_momentum_ignition_log (
                        session_date, computed_at, symbol, side, ignition_score,
                        ignition_building, components_json
                    ) VALUES (:d, :t, :sym, :side, :score, :flag, :comp)
                    """
                ),
                {
                    "d": sd,
                    "t": now,
                    "sym": sym,
                    "side": side,
                    "score": payload["ignition_score"],
                    "flag": payload["ignition_building"],
                    "comp": json.dumps(payload["components"]),
                },
            )
            updated += 1
        db.commit()
    finally:
        db.close()
    return {"ok": True, "updated": updated, "results": results}


def get_ignition_for_symbols(symbols: List[str], session_date: Optional[str] = None) -> Dict[str, Dict[str, Any]]:
    """Latest ignition scores for symbols (from last log row today)."""
    if not symbols:
        return {}
    sd = session_date or today_ist()
    cfg = get_config()
    ui_enabled = bool(cfg.get("ignition_ui_enabled"))
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT DISTINCT ON (symbol) symbol, side, ignition_score, ignition_building, components_json
                FROM rs_momentum_ignition_log
                WHERE session_date = :d AND symbol = ANY(:syms)
                ORDER BY symbol, computed_at DESC
                """
            ),
            {"d": sd, "syms": symbols},
        ).fetchall()
    finally:
        db.close()
    out: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        building = bool(r.ignition_building) and ui_enabled
        out[r.symbol] = {
            "ignition_score": float(r.ignition_score or 0),
            "ignition_building": building,
            "ignition_side": r.side,
        }
    return out

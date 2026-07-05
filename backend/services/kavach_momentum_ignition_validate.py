"""Historical REST backtest for Kavach Momentum Ignition (read-only diagnostics).

Order-flow imbalance requires live WS depth — not backtestable from REST candles.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pytz
from sqlalchemy import text
from sqlalchemy.orm import Session

from backend.config import settings
from backend.services.kavach_momentum_ignition import (
    classify_oi_price_volume,
    coincident_confirmation,
    pullback_depth_contraction,
)
from backend.services.rs_conviction_config import DEFAULTS
from backend.services.rs_conviction_signals import accumulation_signal, normalized_vwap_slope
from backend.services.upstox_service import UpstoxService

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

# Signal thresholds aligned with ignition scoring tiers
THRESHOLD_VWAP_SLOPE = 50.0
THRESHOLD_ABSORPTION = 80.0
THRESHOLD_PULLBACK = 60.0
THRESHOLD_OI_TRI = 70.0
THRESHOLD_CONFIRM = 40.0
FORWARD_BARS = 3
FORWARD_MOVE_PCT = 0.15
MIN_CANDLES = 60
WARMUP_BARS = 40

COMPONENT_KEYS = (
    "order_flow_imbalance",
    "oi_triangulation",
    "pullback_depth",
    "absorption",
    "vwap_slope",
)


def _now_ist_iso() -> str:
    return datetime.now(IST).isoformat()


def _forward_return(candles: List[Dict], idx: int, bars: int = FORWARD_BARS) -> Optional[float]:
    if idx + bars >= len(candles):
        return None
    c0 = float(candles[idx].get("close") or 0)
    c1 = float(candles[idx + bars].get("close") or 0)
    if c0 <= 0:
        return None
    return (c1 - c0) / c0 * 100.0


def _moved_favorably(fwd: float, side: str) -> bool:
    bull = (side or "").upper() == "BULL"
    return fwd > FORWARD_MOVE_PCT if bull else fwd < -FORWARD_MOVE_PCT


def oi_triangulation_from_candles(window: List[Dict], side: str) -> Tuple[float, str]:
    """OI-price-volume triangulation from REST candle OI (proxy for live WS tick)."""
    if len(window) < 3:
        return 0.0, "NO_DATA"
    closed = window[-2]
    prev = window[-3]
    p0 = float(prev.get("close") or 0)
    p1 = float(closed.get("close") or 0)
    if p0 <= 0:
        return 0.0, "NO_DATA"
    price_pct = (p1 - p0) / p0 * 100.0
    oi0 = int(float(prev.get("oi") or 0))
    oi1 = int(float(closed.get("oi") or 0))
    oi_chg = oi1 - oi0
    vols = [float(c.get("volume") or 0) for c in window[-12:]]
    session_avg = sum(vols) / max(len(vols), 1)
    vol_ratio = (vols[-1] / session_avg) if session_avg > 0 else 1.0
    label, score = classify_oi_price_volume(price_pct, oi_chg, vol_ratio, side)
    return score, label


def _empty_hits() -> Dict[str, int]:
    out: Dict[str, int] = {}
    for k in COMPONENT_KEYS:
        out[f"{k}_signals"] = 0
        out[f"{k}_hits"] = 0
    return out


def _record_hit(hits: Dict[str, int], key: str, fired: bool, moved: bool) -> None:
    if not fired:
        return
    hits[f"{key}_signals"] += 1
    if moved:
        hits[f"{key}_hits"] += 1


def _precision(hits: Dict[str, int], key: str) -> Optional[float]:
    n = hits.get(f"{key}_signals", 0)
    if not n:
        return None
    return round(hits.get(f"{key}_hits", 0) / n, 4)


def _aggregate_precision(per_symbol: Dict[str, Dict[str, int]]) -> Dict[str, Any]:
    totals = _empty_hits()
    for sym_hits in per_symbol.values():
        for k, v in sym_hits.items():
            totals[k] = totals.get(k, 0) + v
    out: Dict[str, Any] = {}
    for key in COMPONENT_KEYS:
        if key == "order_flow_imbalance":
            out[key] = {
                "precision_3bar": None,
                "signals": 0,
                "hits": 0,
                "status": "not_applicable",
                "note": (
                    "Live WS depth/tbq/tsq only — validate via View Live Ignition Log during market hours."
                ),
            }
            continue
        prec = _precision(totals, key)
        out[key] = {
            "precision_3bar": prec,
            "signals": totals.get(f"{key}_signals", 0),
            "hits": totals.get(f"{key}_hits", 0),
        }
    return out


def _load_universe(db: Session, limit: int) -> List[Tuple[str, str]]:
    rows = db.execute(
        text(
            """
            SELECT stock, currmth_future_instrument_key
            FROM arbitrage_master
            WHERE currmth_future_instrument_key IS NOT NULL
            ORDER BY stock
            LIMIT :n
            """
        ),
        {"n": limit},
    ).fetchall()
    return [(r.stock, r.currmth_future_instrument_key) for r in rows]


def _analyze_symbol_candles(
    candles: List[Dict], side: str, cfg: Dict[str, Any], atr_pct: float = 1.2
) -> Tuple[Dict[str, int], int]:
    hits = _empty_hits()
    samples = 0
    for i in range(WARMUP_BARS, len(candles) - FORWARD_BARS):
        window = candles[: i + 1]
        fwd = _forward_return(candles, i)
        if fwd is None:
            continue
        samples += 1
        moved = _moved_favorably(fwd, side)

        slope = normalized_vwap_slope(window, atr_pct, cfg)
        accum, _, _ = accumulation_signal(window, side, cfg)
        pb, _ = pullback_depth_contraction(window, side, atr_pct)
        oi_sc, _ = oi_triangulation_from_candles(window, side)

        _record_hit(hits, "vwap_slope", slope >= THRESHOLD_VWAP_SLOPE, moved)
        _record_hit(hits, "absorption", accum >= THRESHOLD_ABSORPTION, moved)
        _record_hit(hits, "pullback_depth", pb >= THRESHOLD_PULLBACK, moved)
        _record_hit(hits, "oi_triangulation", oi_sc >= THRESHOLD_OI_TRI, moved)
        # order_flow_imbalance: never fired in REST backtest

    return hits, samples


def run_momentum_ignition_backtest(
    db: Session,
    *,
    days: int = 10,
    symbols: int = 20,
    side: str = "BULL",
    throttle_sec: float = 0.35,
) -> Dict[str, Any]:
    """
    Read-only historical validation. Does not touch config, ignition log, or live jobs.
    """
    started_at = _now_ist_iso()
    ux = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    ux.reload_token_from_storage()
    if not ux.access_token:
        return {
            "ok": False,
            "error": "No Upstox token — configure credentials for REST backtest",
            "started_at": started_at,
            "finished_at": _now_ist_iso(),
        }

    universe = _load_universe(db, symbols)
    cfg = DEFAULTS
    per_symbol: Dict[str, Dict[str, int]] = {}
    total_samples = 0
    symbols_fetched = 0
    errors: List[str] = []

    for sym, ik in universe:
        try:
            candles = ux.get_historical_candles_by_instrument_key(
                ik.replace(":", "|"),
                interval="minutes/5",
                days_back=min(days + 5, 31),
            )
        except Exception as exc:
            logger.warning("backtest fetch %s: %s", sym, exc)
            errors.append(f"{sym}: {exc}")
            if throttle_sec > 0:
                time.sleep(throttle_sec)
            continue
        if throttle_sec > 0:
            time.sleep(throttle_sec)
        if not candles or len(candles) < MIN_CANDLES:
            continue
        symbols_fetched += 1
        sym_hits, samples = _analyze_symbol_candles(candles, side, cfg)
        per_symbol[sym] = sym_hits
        total_samples += samples

    components = _aggregate_precision(per_symbol)
    finished_at = _now_ist_iso()

    return {
        "ok": True,
        "started_at": started_at,
        "finished_at": finished_at,
        "parameters": {"days": days, "symbols": symbols, "side": side.upper()},
        "universe_requested": len(universe),
        "symbols_with_data": symbols_fetched,
        "bar_samples": total_samples,
        "forward_bars": FORWARD_BARS,
        "forward_move_pct": FORWARD_MOVE_PCT,
        "thresholds": {
            "vwap_slope": THRESHOLD_VWAP_SLOPE,
            "absorption": THRESHOLD_ABSORPTION,
            "pullback_depth": THRESHOLD_PULLBACK,
            "oi_triangulation": THRESHOLD_OI_TRI,
        },
        "components": components,
        "per_symbol": per_symbol,
        "fetch_errors": errors[:20],
        "recommendation": (
            "Keep ignition_ui_enabled=false until live WS order-flow validation completes. "
            "Enable 5m REST components if precision >= 0.55 on slope+absorption."
        ),
        "plain_text": format_backtest_plain_text(
            {
                "ok": True,
                "started_at": started_at,
                "finished_at": finished_at,
                "parameters": {"days": days, "symbols": symbols, "side": side.upper()},
                "universe_requested": len(universe),
                "symbols_with_data": symbols_fetched,
                "bar_samples": total_samples,
                "components": components,
                "recommendation": (
                    "Keep ignition_ui_enabled=false until live WS order-flow validation completes."
                ),
            }
        ),
    }


def format_backtest_plain_text(result: Dict[str, Any]) -> str:
    lines = [
        "=== Kavach Momentum Ignition — Historical Backtest ===",
        f"Started:  {result.get('started_at', '—')}",
        f"Finished: {result.get('finished_at', '—')}",
        "",
    ]
    params = result.get("parameters") or {}
    lines.append(f"Parameters: days={params.get('days')} symbols={params.get('symbols')} side={params.get('side')}")
    lines.append(
        f"Universe: {result.get('symbols_with_data', 0)}/{result.get('universe_requested', 0)} symbols, "
        f"{result.get('bar_samples', 0)} bar samples"
    )
    lines.append("")
    lines.append("Per-component precision (3-bar forward, favorably moved):")
    comps = result.get("components") or {}
    labels = {
        "order_flow_imbalance": "Order-flow imbalance (WS)",
        "oi_triangulation": "OI-price-volume triangulation",
        "pullback_depth": "Pullback-depth contraction",
        "absorption": "Absorption",
        "vwap_slope": "VWAP slope",
    }
    for key in COMPONENT_KEYS:
        c = comps.get(key) or {}
        label = labels.get(key, key)
        if c.get("status") == "not_applicable":
            lines.append(f"  {label}: N/A — {c.get('note', '')}")
            continue
        prec = c.get("precision_3bar")
        sig = c.get("signals", 0)
        hit = c.get("hits", 0)
        prec_s = f"{prec:.1%}" if prec is not None else "—"
        lines.append(f"  {label}: precision={prec_s} ({hit}/{sig} hits)")
    rec = result.get("recommendation")
    if rec:
        lines.append("")
        lines.append(f"Recommendation: {rec}")
    return "\n".join(lines)

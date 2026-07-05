"""Historical REST backtest for Kavach Momentum Ignition (read-only diagnostics).

Order-flow imbalance requires live WS depth — not backtestable from REST candles.
"""
from __future__ import annotations

import logging
import math
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

COMPOSITE_KEYS = (
    "composite_full",
    "composite_no_pullback",
)


def _wilson_ci(hits: int, n: int, z: float = 1.96) -> Tuple[Optional[float], Optional[float]]:
    """95% Wilson score interval for binomial proportion."""
    if n <= 0:
        return None, None
    p = hits / n
    z2 = z * z
    denom = 1 + z2 / n
    center = (p + z2 / (2 * n)) / denom
    margin = z * math.sqrt((p * (1 - p) + z2 / (4 * n)) / n) / denom
    return round(max(0.0, center - margin), 4), round(min(1.0, center + margin), 4)


def _credibility_label(hits: int, n: int, baseline_rate: Optional[float]) -> str:
    """
    credible_positive / credible_negative: Wilson 95% CI excludes baseline.
    not_distinguishable: CI overlaps baseline.
  insufficient_sample: n too small or no baseline.
    """
    if n <= 0 or baseline_rate is None:
        return "insufficient_sample"
    lo, hi = _wilson_ci(hits, n)
    if lo is None or hi is None:
        return "insufficient_sample"
    if lo > baseline_rate:
        return "credible_positive"
    if hi < baseline_rate:
        return "credible_negative"
    return "not_distinguishable"


def _rest_composite_score(
    oi_sc: float,
    accum: float,
    slope: float,
    pb: float,
    conf: float,
    cfg: Dict[str, Any],
    *,
    include_pullback: bool = True,
) -> float:
    """
    REST backtest composite: order-flow excluded (0); remaining weights renormalized to 0–100.
    Matches live tier weights from rs_conviction_config when WS order-flow is absent.
    """
    w_oi = float(cfg.get("W_ignition_oi_tri") or 0.25)
    w_acc = float(cfg.get("W_ignition_absorption") or 0.15)
    w_slope = float(cfg.get("W_ignition_slope") or 0.10)
    w_pb = float(cfg.get("W_ignition_pullback") or 0.10) if include_pullback else 0.0
    w_conf = float(cfg.get("W_ignition_confirm") or 0.05)
    w_sum = w_oi + w_acc + w_slope + w_pb + w_conf
    if w_sum <= 0:
        return 0.0
    raw = (w_oi * oi_sc + w_acc * accum + w_slope * slope + w_pb * pb + w_conf * conf) / w_sum
    return min(100.0, max(0.0, raw))


def _metric_block(
    hits: int, signals: int, baseline_rate: Optional[float]
) -> Dict[str, Any]:
    prec = round(hits / signals, 4) if signals else None
    lifts = _lift_fields(prec, baseline_rate)
    lo, hi = _wilson_ci(hits, signals) if signals else (None, None)
    return {
        "precision_3bar": prec,
        "signals": signals,
        "hits": hits,
        "lift_pp": lifts["lift_pp"],
        "lift_ratio": lifts["lift_ratio"],
        "ci_95_low": lo,
        "ci_95_high": hi,
        "credibility": _credibility_label(hits, signals, baseline_rate),
    }


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
    for k in COMPONENT_KEYS + COMPOSITE_KEYS:
        out[f"{k}_signals"] = 0
        out[f"{k}_hits"] = 0
    return out


def _record_hit(hits: Dict[str, int], key: str, fired: bool, moved: bool) -> None:
    if not fired:
        return
    hits[f"{key}_signals"] += 1
    if moved:
        hits[f"{key}_hits"] += 1


def _lift_fields(precision: Optional[float], baseline_rate: Optional[float]) -> Dict[str, Optional[float]]:
    if precision is None or baseline_rate is None:
        return {"lift_pp": None, "lift_ratio": None}
    lift_pp = round(precision - baseline_rate, 4)
    lift_ratio = round(precision / baseline_rate, 4) if baseline_rate > 0 else None
    return {"lift_pp": lift_pp, "lift_ratio": lift_ratio}


def _precision(hits: Dict[str, int], key: str) -> Optional[float]:
    n = hits.get(f"{key}_signals", 0)
    if not n:
        return None
    return round(hits.get(f"{key}_hits", 0) / n, 4)


def _aggregate_precision(
    per_symbol: Dict[str, Dict[str, int]], baseline_rate: Optional[float]
) -> Dict[str, Any]:
    totals = _empty_hits()
    for sym_hits in per_symbol.values():
        for k, v in sym_hits.items():
            totals[k] = totals.get(k, 0) + v
    out: Dict[str, Any] = {}
    for key in COMPONENT_KEYS:
        if key == "order_flow_imbalance":
            out[key] = {
                **_metric_block(0, 0, baseline_rate),
                "status": "not_applicable",
                "note": (
                    "Live WS depth/tbq/tsq only — validate via View Live Ignition Log during market hours."
                ),
            }
            continue
        out[key] = _metric_block(
            totals.get(f"{key}_hits", 0),
            totals.get(f"{key}_signals", 0),
            baseline_rate,
        )
    for key in COMPOSITE_KEYS:
        out[key] = _metric_block(
            totals.get(f"{key}_hits", 0),
            totals.get(f"{key}_signals", 0),
            baseline_rate,
        )
    return out


def _compute_baseline(bar_samples: int, favorable_moves: int) -> Dict[str, Any]:
    rate = round(favorable_moves / bar_samples, 4) if bar_samples else None
    return {
        "bar_samples": bar_samples,
        "favorable_moves": favorable_moves,
        "favorable_rate_3bar": rate,
    }


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
) -> Tuple[Dict[str, int], int, int, Dict[str, int]]:
    hits = _empty_hits()
    pb_pattern_hits = {
        "contracting_signals": 0,
        "contracting_hits": 0,
        "mixed_signals": 0,
        "mixed_hits": 0,
        "deepening_signals": 0,
        "deepening_hits": 0,
    }
    threshold = float(cfg.get("ignition_flag_threshold") or 65)
    is_bull = (side or "").upper() == "BULL"
    samples = 0
    favorable_moves = 0
    for i in range(WARMUP_BARS, len(candles) - FORWARD_BARS):
        window = candles[: i + 1]
        fwd = _forward_return(candles, i)
        if fwd is None:
            continue
        samples += 1
        moved = _moved_favorably(fwd, side)
        if moved:
            favorable_moves += 1

        slope = normalized_vwap_slope(window, atr_pct, cfg)
        accum, _, _ = accumulation_signal(window, side, cfg)
        pb, pb_meta = pullback_depth_contraction(window, side, atr_pct)
        oi_sc, _ = oi_triangulation_from_candles(window, side)
        conf, _ = coincident_confirmation(window, side)

        _record_hit(hits, "vwap_slope", slope >= THRESHOLD_VWAP_SLOPE, moved)
        _record_hit(hits, "absorption", accum >= THRESHOLD_ABSORPTION, moved)
        _record_hit(hits, "pullback_depth", pb >= THRESHOLD_PULLBACK, moved)
        _record_hit(hits, "oi_triangulation", oi_sc >= THRESHOLD_OI_TRI, moved)

        comp_full = _rest_composite_score(oi_sc, accum, slope, pb, conf, cfg, include_pullback=True)
        comp_no_pb = _rest_composite_score(oi_sc, accum, slope, pb, conf, cfg, include_pullback=False)
        _record_hit(hits, "composite_full", comp_full >= threshold, moved)
        if is_bull:
            _record_hit(hits, "composite_no_pullback", comp_no_pb >= threshold, moved)

        if is_bull:
            pattern = (pb_meta or {}).get("pattern")
            if pattern == "CONTRACTING" and pb >= THRESHOLD_PULLBACK:
                pb_pattern_hits["contracting_signals"] += 1
                if moved:
                    pb_pattern_hits["contracting_hits"] += 1
            elif pattern == "MIXED":
                pb_pattern_hits["mixed_signals"] += 1
                if moved:
                    pb_pattern_hits["mixed_hits"] += 1
            elif pattern == "DEEPENING":
                pb_pattern_hits["deepening_signals"] += 1
                if moved:
                    pb_pattern_hits["deepening_hits"] += 1

    return hits, samples, favorable_moves, pb_pattern_hits


def _aggregate_pullback_pattern(per_symbol_pb: Dict[str, Dict[str, int]], baseline_rate: Optional[float]) -> Dict[str, Any]:
    totals = {
        "contracting_signals": 0,
        "contracting_hits": 0,
        "mixed_signals": 0,
        "mixed_hits": 0,
        "deepening_signals": 0,
        "deepening_hits": 0,
    }
    for row in per_symbol_pb.values():
        for k, v in row.items():
            totals[k] = totals.get(k, 0) + v
    out: Dict[str, Any] = {}
    for name in ("contracting", "mixed", "deepening"):
        out[name] = _metric_block(
            totals.get(f"{name}_hits", 0),
            totals.get(f"{name}_signals", 0),
            baseline_rate,
        )
    return out


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
    per_symbol_pb: Dict[str, Dict[str, int]] = {}
    total_samples = 0
    total_favorable = 0
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
        sym_hits, samples, favorable, pb_pat = _analyze_symbol_candles(candles, side, cfg)
        per_symbol[sym] = sym_hits
        per_symbol_pb[sym] = pb_pat
        total_samples += samples
        total_favorable += favorable

    baseline = _compute_baseline(total_samples, total_favorable)
    bl_rate = baseline.get("favorable_rate_3bar")
    components = _aggregate_precision(per_symbol, bl_rate)
    pullback_pattern_bull: Optional[Dict[str, Any]] = None
    if side.upper() == "BULL":
        pullback_pattern_bull = _aggregate_pullback_pattern(per_symbol_pb, bl_rate)
    finished_at = _now_ist_iso()

    result_body = {
        "ok": True,
        "started_at": started_at,
        "finished_at": finished_at,
        "parameters": {"days": days, "symbols": symbols, "side": side.upper()},
        "universe_requested": len(universe),
        "symbols_with_data": symbols_fetched,
        "bar_samples": total_samples,
        "baseline": baseline,
        "forward_bars": FORWARD_BARS,
        "forward_move_pct": FORWARD_MOVE_PCT,
        "composite_note": (
            "REST composite renormalizes OI/absorption/slope/pullback/confirm weights "
            "(order-flow=0). Threshold from ignition_flag_threshold (default 65)."
        ),
        "thresholds": {
            "vwap_slope": THRESHOLD_VWAP_SLOPE,
            "absorption": THRESHOLD_ABSORPTION,
            "pullback_depth": THRESHOLD_PULLBACK,
            "oi_triangulation": THRESHOLD_OI_TRI,
            "ignition_flag_threshold": float(cfg.get("ignition_flag_threshold") or 65),
        },
        "components": components,
        "pullback_pattern_bull": pullback_pattern_bull,
        "per_symbol": per_symbol,
        "fetch_errors": errors[:20],
        "recommendation": (
            "Keep ignition_ui_enabled=false until live WS order-flow validation completes. "
            "Use credibility label + lift_pp vs baseline; credible_negative => consider down-weight/remove."
        ),
    }
    result_body["plain_text"] = format_backtest_plain_text(result_body)
    return result_body


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
    baseline = result.get("baseline") or {}
    bl_rate = baseline.get("favorable_rate_3bar")
    bl_moves = baseline.get("favorable_moves", 0)
    bl_bars = baseline.get("bar_samples", 0)
    bl_s = f"{bl_rate:.1%}" if bl_rate is not None else "—"
    lines.append("")
    lines.append(
        f"Baseline (unconditional {params.get('side', 'BULL')}-favorable 3-bar move): "
        f"{bl_s} ({bl_moves}/{bl_bars} bars, no signal filter)"
    )
    lines.append("")
    lines.append("Per-component precision vs baseline (95% Wilson CI, credibility):")
    comps = result.get("components") or {}
    labels = {
        "order_flow_imbalance": "Order-flow imbalance (WS)",
        "oi_triangulation": "OI-price-volume triangulation",
        "pullback_depth": "Pullback-depth contraction",
        "absorption": "Absorption",
        "vwap_slope": "VWAP slope",
        "composite_full": "Composite (REST, incl. pullback)",
        "composite_no_pullback": "Composite (REST, BULL no-pullback)",
    }
    for key in COMPONENT_KEYS + COMPOSITE_KEYS:
        if key == "composite_no_pullback" and (params.get("side") or "").upper() != "BULL":
            continue
        c = comps.get(key) or {}
        label = labels.get(key, key)
        if c.get("status") == "not_applicable":
            lines.append(f"  {label}: N/A — {c.get('note', '')}")
            continue
        prec = c.get("precision_3bar")
        sig = c.get("signals", 0)
        hit = c.get("hits", 0)
        lift_pp = c.get("lift_pp")
        lift_ratio = c.get("lift_ratio")
        cred = c.get("credibility", "—")
        lo, hi = c.get("ci_95_low"), c.get("ci_95_high")
        prec_s = f"{prec:.1%}" if prec is not None else "—"
        lift_pp_s = f"{lift_pp:+.1%}pp" if lift_pp is not None else "—"
        lift_x_s = f"{lift_ratio:.2f}×" if lift_ratio is not None else "—"
        ci_s = f"[{lo:.1%}, {hi:.1%}]" if lo is not None and hi is not None else "—"
        lines.append(
            f"  {label}: precision={prec_s} ({hit}/{sig}) | lift={lift_pp_s} | {lift_x_s} | CI={ci_s} | {cred}"
        )
    pb_pat = result.get("pullback_pattern_bull")
    if pb_pat:
        lines.append("")
        lines.append("BULL pullback pattern breakdown (diagnostic):")
        for pname in ("contracting", "mixed", "deepening"):
            p = pb_pat.get(pname) or {}
            prec = p.get("precision_3bar")
            prec_s = f"{prec:.1%}" if prec is not None else "—"
            lift_pp = p.get("lift_pp")
            lift_s = f" lift={lift_pp:+.1%}pp" if lift_pp is not None else ""
            lines.append(
                f"  {pname}: {prec_s} ({p.get('hits', 0)}/{p.get('signals', 0)}){lift_s} | {p.get('credibility', '—')}"
            )
    rec = result.get("recommendation")
    if rec:
        lines.append("")
        lines.append(f"Recommendation: {rec}")
    return "\n".join(lines)

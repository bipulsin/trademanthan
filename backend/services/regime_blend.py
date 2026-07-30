"""Live regime blend for warning_stack REGIME UNSTABLE (NIFTY + signed VWAP slope).

Replaces binary NIFTY-only REGIME UNSTABLE in the stack with:

    blend = 0.20 * nifty_regime + 0.80 * signed_stock_vwap_slope

``REGIME UNSTABLE`` counts toward warning_stack when ``blend < 0.40``.
Live default-on (no shadow flag). CHURN / DIR CONFLICT / distance / DI unchanged.
"""
from __future__ import annotations

import os
from typing import Any, Dict, List, Optional, Tuple

# Live defaults — no feature flag.
REGIME_BLEND_NIFTY_WEIGHT = 0.20
REGIME_BLEND_STOCK_WEIGHT = 0.80
REGIME_BLEND_THRESHOLD = 0.40
SLOPE_SCORE_NORM = 50.0  # production THRESHOLD_VWAP_SLOPE


def regime_blend_threshold() -> float:
    raw = os.environ.get("REGIME_BLEND_THRESHOLD", str(REGIME_BLEND_THRESHOLD))
    try:
        return float(raw)
    except (TypeError, ValueError):
        return REGIME_BLEND_THRESHOLD


def nifty_regime_component(market_regime: Optional[str]) -> float:
    return 1.0 if (market_regime or "").strip().upper() == "TREND" else 0.0


def signed_stock_vwap_slope_component(
    *,
    direction: Optional[str],
    slope_score: Optional[float],
    signed_slope_atr: Optional[float],
) -> Tuple[float, bool, Optional[float]]:
    """Direction-aligned stock component in [0, 1].

    Wrong-way or missing signed slope → 0. Aligned → min(1, score/50).
    """
    is_short = (direction or "LONG").upper() in ("SHORT", "BEAR", "BEARISH")
    try:
        signed = float(signed_slope_atr) if signed_slope_atr is not None else None
    except (TypeError, ValueError):
        signed = None
    if signed is None:
        return 0.0, False, None

    direction_ok = (signed < 0) if is_short else (signed > 0)
    aligned = (-signed) if is_short else signed
    if not direction_ok:
        return 0.0, False, aligned

    try:
        mag = max(0.0, min(1.0, float(slope_score or 0.0) / SLOPE_SCORE_NORM))
    except (TypeError, ValueError):
        mag = 0.0
    return mag, True, aligned


def compute_regime_blend(
    *,
    direction: Optional[str],
    market_regime: Optional[str],
    slope_score: Optional[float],
    signed_slope_atr: Optional[float],
    threshold: Optional[float] = None,
) -> Dict[str, Any]:
    thr = REGIME_BLEND_THRESHOLD if threshold is None else float(threshold)
    nifty_c = nifty_regime_component(market_regime)
    stock_c, direction_ok, aligned = signed_stock_vwap_slope_component(
        direction=direction,
        slope_score=slope_score,
        signed_slope_atr=signed_slope_atr,
    )
    blend = REGIME_BLEND_NIFTY_WEIGHT * nifty_c + REGIME_BLEND_STOCK_WEIGHT * stock_c
    unstable = blend < thr
    return {
        "regime_blend": round(blend, 4),
        "regime_blend_nifty_c": nifty_c,
        "regime_blend_stock_c": round(stock_c, 4),
        "regime_blend_threshold": thr,
        "regime_blend_direction_ok": direction_ok,
        "regime_blend_aligned_signed_atr": (
            round(aligned, 4) if aligned is not None else None
        ),
        "regime_unstable_for_stack": unstable,
    }


def _vq_fields(stock: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    vq = stock.get("vwap_quality") if isinstance(stock.get("vwap_quality"), dict) else {}
    slope = vq.get("slope_score")
    if slope is None:
        slope = stock.get("vwap_slope_score")
    signed = vq.get("signed_slope_atr")
    if signed is None:
        signed = stock.get("signed_slope_atr")
    try:
        slope_f = float(slope) if slope is not None else None
    except (TypeError, ValueError):
        slope_f = None
    try:
        signed_f = float(signed) if signed is not None else None
    except (TypeError, ValueError):
        signed_f = None
    return slope_f, signed_f


def apply_regime_blend_to_stocks(
    stocks: List[Dict[str, Any]],
    *,
    market_regime: Optional[str],
) -> Dict[str, int]:
    """Rewrite REGIME UNSTABLE badge from blend; stamp blend fields on each stock.

    Must run after ``annotate_regime_context`` (which may have added NIFTY-based
    REGIME UNSTABLE) and before ``apply_warning_stack_downgrades``.
    """
    thr = regime_blend_threshold()
    stats = {"evaluated": 0, "unstable": 0, "stable": 0, "badge_added": 0, "badge_removed": 0}
    for s in stocks:
        slope, signed = _vq_fields(s)
        # Prefer per-card regime_context if present; else session market_regime.
        rc = s.get("regime_context") if isinstance(s.get("regime_context"), dict) else {}
        reg = rc.get("market_regime") or market_regime or s.get("market_regime")
        info = compute_regime_blend(
            direction=s.get("direction"),
            market_regime=reg,
            slope_score=slope,
            signed_slope_atr=signed,
            threshold=thr,
        )
        s.update(info)
        stats["evaluated"] += 1
        if info["regime_unstable_for_stack"]:
            stats["unstable"] += 1
        else:
            stats["stable"] += 1

        badges = list(s.get("gate_badges") or [])
        had = any(
            str(b) == "REGIME UNSTABLE" or str(b).startswith("REGIME UNSTABLE")
            for b in badges
        )
        if info["regime_unstable_for_stack"]:
            if not had:
                badges.append("REGIME UNSTABLE")
                stats["badge_added"] += 1
            s["gate_badges"] = badges
        else:
            if had:
                badges = [
                    b
                    for b in badges
                    if not (
                        str(b) == "REGIME UNSTABLE" or str(b).startswith("REGIME UNSTABLE")
                    )
                ]
                stats["badge_removed"] += 1
            s["gate_badges"] = badges
            # Keep visibility of NIFTY TRANSITION via regime_context.flags, but
            # stack no longer sees REGIME UNSTABLE when blend clears.
    return stats

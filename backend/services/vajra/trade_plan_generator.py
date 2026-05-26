"""Conditional discretionary trade plans — assist only, never blind BUY/SELL."""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from backend.services.vajra.setup_classifier import (
    SETUP_BREAKOUT,
    SETUP_PULLBACK,
    SETUP_RECLAIM,
    classify_setup_type,
    quality_grade,
)
from backend.services.vajra.validation_engine import extension_risk_level


def _f(v: Any, default: float = 0.0) -> float:
    try:
        return float(v) if v is not None else default
    except (TypeError, ValueError):
        return default


def _round_px(p: float, tick: float = 0.05) -> float:
    if p <= 0:
        return p
    return round(round(p / tick) * tick, 2)


def _anchor_price(row: Dict[str, Any]) -> Optional[float]:
    for k in ("ltp", "last_price", "close", "entry_reference"):
        v = row.get(k)
        if v is not None:
            p = _f(v)
            if p > 0:
                return p
    return None


def generate_conditional_trade_plan(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Structured plan when setup is PREPARE or EXECUTABLE.
    Entry is conditional on trigger — not an unconditional call.
    """
    wf = str(row.get("execution_workflow_state") or "").upper()
    if wf not in ("PREPARE", "EXECUTABLE"):
        return None

    stock = str(row.get("stock") or row.get("security") or "").strip().upper()
    direction = str(row.get("execution_bias") or row.get("direction") or "LONG").upper()
    bull = direction != "SHORT"
    setup = classify_setup_type(row)
    grade = quality_grade(row)
    conv = _f(row.get("conviction_score")) or _f(row.get("confidence"))
    ext = _f(row.get("extension_risk_score"), 50.0)
    px = _anchor_price(row)
    if not px or px <= 0:
        skel_ctx: List[str] = []
        if row.get("sector_in_top_gainers_rank"):
            skel_ctx.append("Sector in top gainers")
        if row.get("sector_in_top_losers_rank"):
            skel_ctx.append("Sector in top losers")
        vwap_s = str(row.get("vwap_reclaim_status") or "")
        if vwap_s:
            skel_ctx.append(f"VWAP: {vwap_s}")
        return {
            "symbol": stock,
            "direction": direction,
            "setup_type": setup,
            "quality_grade": grade,
            "confidence_pct": round(conv, 1),
            "entry_condition": (
                "Enter only after 5m trigger confirmation on chart — "
                "set entry above reclaim/breakout close (long) or below breakdown close (short)."
            ),
            "preferred_entry": None,
            "stop_loss": None,
            "targets": None,
            "market_context": skel_ctx,
            "invalidation": [
                "Exit if VWAP is lost against your direction.",
                "Exit if sector alignment flips (S↔W badge change).",
            ],
            "disclaimer": "Discretionary assist only — confirm levels on chart before orders.",
            "extension_risk": extension_risk_level(ext),
            "levels_pending": True,
        }

    pb = _f(row.get("pullback_quality_score"), 50.0)
    risk_pct = 0.35 if ext < 55 else 0.45 if ext < 70 else 0.55

    if bull:
        trigger = _round_px(px * (1.001 if setup == SETUP_BREAKOUT else 1.0005))
        entry_lo = _round_px(trigger)
        entry_hi = _round_px(trigger * 1.0015)
        stop = _round_px(px * (1 - risk_pct / 100.0))
        t1 = _round_px(px * (1 + risk_pct * 1.2 / 100.0))
        t2 = _round_px(px * (1 + risk_pct * 2.0 / 100.0))
        t3_note = "Trail below 5m VWAP or last higher low after T2"
        entry_condition = (
            f"Enter only if 5m candle closes above {trigger:.2f} with acceptance "
            f"(no immediate rejection wick)."
        )
        invalidation = [
            f"Exit thesis if price closes below {stop:.2f} (stop zone).",
            "Exit if VWAP is lost with a lower high on 5m.",
            "Exit if sector rank flips to weak (W1–W3) while structure fails.",
        ]
    else:
        trigger = _round_px(px * (0.999 if setup == SETUP_BREAKOUT else 0.9995))
        entry_lo = _round_px(trigger * 0.9985)
        entry_hi = _round_px(trigger)
        stop = _round_px(px * (1 + risk_pct / 100.0))
        t1 = _round_px(px * (1 - risk_pct * 1.2 / 100.0))
        t2 = _round_px(px * (1 - risk_pct * 2.0 / 100.0))
        t3_note = "Trail above 5m VWAP or last lower high after T2"
        entry_condition = (
            f"Enter only if 5m candle closes below {trigger:.2f} with acceptance."
        )
        invalidation = [
            f"Exit thesis if price closes above {stop:.2f} (stop zone).",
            "Exit if VWAP reclaimed against short with higher low sequence.",
            "Exit if sector rank flips to strong (S1–S3) while structure fails.",
        ]

    context_bullets: List[str] = []
    if row.get("sector_trade_badge") == "SECTOR_ALIGNED" or row.get("sector_in_top_gainers_rank"):
        context_bullets.append("Sector aligned with direction")
    elif row.get("sector_in_top_losers_rank"):
        context_bullets.append("Sector weak — reduce size or wait")
    vwap = str(row.get("vwap_reclaim_status") or "")
    if vwap:
        context_bullets.append(f"VWAP: {vwap}")
    if _f(row.get("ess_score")) >= 60:
        context_bullets.append(f"Execution stability (ESS) {_f(row.get('ess_score')):.0f}")
    if pb >= 55:
        context_bullets.append(f"Pullback quality {pb:.0f}")
    mp = row.get("market_context") or row.get("market_phase")
    if mp:
        context_bullets.append(f"Market context: {mp}")

    return {
        "symbol": stock,
        "direction": direction,
        "setup_type": setup,
        "quality_grade": grade,
        "confidence_pct": round(conv, 1),
        "entry_condition": entry_condition,
        "preferred_entry": {"low": entry_lo, "high": entry_hi},
        "stop_loss": stop,
        "targets": {"t1": t1, "t2": t2, "t3_note": t3_note},
        "market_context": context_bullets,
        "invalidation": invalidation,
        "disclaimer": (
            "Discretionary assist only — confirm on chart before placing orders. "
            "Not an automated trade signal."
        ),
        "extension_risk": extension_risk_level(ext),
    }

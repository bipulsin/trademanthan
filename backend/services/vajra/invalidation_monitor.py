"""Thesis weakening signals — warnings only, no auto-exit."""
from __future__ import annotations

from typing import Any, Dict, List

from backend.services.vajra.validation_engine import extension_risk_level


def _f(row: Dict[str, Any], key: str, default: float = 0.0) -> float:
    v = row.get(key)
    if v is None:
        return default
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def collect_invalidation_signals(row: Dict[str, Any]) -> List[str]:
    signals: List[str] = []
    bull = str(row.get("execution_bias") or row.get("direction") or "LONG").upper() != "SHORT"

    vwap = str(row.get("vwap_reclaim_status") or "").upper()
    if bull and "BELOW" in vwap:
        signals.append("VWAP lost — bullish acceptance failing")
    if not bull and "ABOVE" in vwap and "BELOW" not in vwap:
        signals.append("VWAP reclaimed against short thesis")

    ema = str(row.get("ema_reclaim_status") or row.get("trend") or "").upper()
    if "FAIL" in ema:
        signals.append("EMA / trend structure failing")

    ext = _f(row, "extension_risk_score", 50.0)
    if extension_risk_level(ext) == "HIGH":
        signals.append("Extension risk HIGH — late entry / exhaustion risk")

    if row.get("sector_in_top_losers_rank") and bull:
        signals.append("Sector moved to top losers — flow contradiction")
    if row.get("sector_in_top_gainers_rank") and not bull:
        signals.append("Sector moved to top gainers — short thesis under pressure")

    ts = str(row.get("transition_state") or "").upper()
    if "EXHAUST" in ts or "FAIL" in ts:
        signals.append(f"Momentum state: {row.get('transition_state')}")

    if _f(row, "conviction_score") or _f(row, "confidence"):
        conv = _f(row, "conviction_score") or _f(row, "confidence")
        if conv < 45:
            signals.append("Conviction deteriorating below 45")

    return signals[:5]

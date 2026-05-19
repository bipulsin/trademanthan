"""Qualification layer — apply trade quality to signal rows."""
from __future__ import annotations

from typing import Any, Dict, Optional, Sequence

from backend.services.vajra.actions import resolve_enter_action
from backend.services.vajra.trade_quality import compute_trade_quality


def apply_trade_qualification(
    row: Dict[str, Any],
    *,
    candles_30m: Sequence[Dict[str, Any]],
    candles_5m: Optional[Sequence[Dict[str, Any]]] = None,
    candles_1hr: Optional[Sequence[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    bull_dir = "SHORT" not in str(row.get("trade_type") or "").upper()
    if "SHORT" in str(row.get("trade_type") or "").upper():
        bull_dir = False
    elif "LONG" in str(row.get("trade_type") or "").upper():
        bull_dir = True
    else:
        bull_dir = float(row.get("bull_score") or 0) >= float(row.get("bear_score") or 0)

    tq = compute_trade_quality(
        candles_30m=candles_30m,
        candles_5m=candles_5m,
        candles_1hr=candles_1hr,
        bull_dir=bull_dir,
        market_phase=str(row.get("market_phase") or ""),
        extension_risk=row.get("extension_risk_score"),
        pullback_quality=row.get("pullback_quality_score"),
        ees_score=row.get("ees_score"),
        execution_validated=bool(row.get("execution_validated")),
        structure_pass="PASS" in str(row.get("structure") or ""),
        momentum_pass="PASS" in str(row.get("momentum") or ""),
        trend_pass="PASS" in str(row.get("trend") or ""),
        volume_pass="PASS" in str(row.get("volume") or ""),
        tps_score=row.get("tps_score"),
    )
    if tq is None:
        return row

    row.update(tq.to_dict())
    row["confidence"] = tq.confidence
    action = resolve_enter_action(
        entry_state=tq.state,
        confidence=tq.confidence,
        reject_reasons=tq.reject_reasons,
    )
    row.update(action)
    return row

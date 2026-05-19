"""
Two-stage Vajra transition pipeline:
  Stage 1 — 30m TPS discovery on full universe (→ 5–15 shortlist)
  Stage 2 — 5m execution validation on shortlist only
  HTF — 1hr directional bias (optional, not full ECS weight)
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence

import pytz

from backend.services.vajra.engine import compute_ecs_rating, sort_vajra_rows
from backend.services.vajra.ees import compute_ees, enter_action_label
from backend.services.vajra.transition import (
    TPS_SHORTLIST_MAX,
    TPS_SHORTLIST_MIN,
    classify_early_transition,
    compute_tps,
    merge_trade_type,
    validate_execution_5m,
)

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

DISCOVERY_TF = "30m"
EXECUTION_TF = "5m"
HTF_BIAS_TF = "1hr"


def _build_row(
    *,
    stock: str,
    fut_sym: str,
    instrument_key: str = "",
    ecs_rating,
    tps,
    early_type: Optional[str],
    execution,
    computed_at: datetime,
    ees_result=None,
) -> Dict[str, Any]:
    ecs = ecs_rating.to_row_dict()
    trade_type = merge_trade_type(ecs["trade_type"], early_type)
    tps_d = tps.to_dict()
    exec_d = execution.to_dict() if execution else {
        "execution_validated": False,
        "execution_step": "—",
        "execution_steps_passed": 0,
    }
    ext_low = float(tps_d.get("extension_risk_score") or 100) < 50
    exec_ok = exec_d.get("execution_validated", False)
    alertable = (
        trade_type in ("EARLY LONG TRANSITION", "EARLY SHORT TRANSITION")
        and ext_low
        and (exec_ok or not execution)
    )

    ees_d: Dict[str, Any] = {}
    enter_action: Dict[str, Any] = {}
    if ees_result is not None:
        ees_d = ees_result.to_dict()
        enter_action = enter_action_label(
            tps_score=tps_d.get("tps_score"),
            ees_score=ees_d.get("ees_score"),
            entry_state=ees_d.get("entry_state"),
        )

    return {
        "security": fut_sym or stock,
        "stock": stock,
        "instrument_key": instrument_key,
        "trade_type": trade_type,
        "confidence": ecs["confidence"],
        "ecs_score": ecs.get("ecs_score", ecs["confidence"]),
        "tps_score": tps_d["tps_score"],
        **ees_d,
        "enter_action": enter_action.get("action"),
        "enter_enabled": enter_action.get("enabled", False),
        "enter_reason": enter_action.get("reason"),
        "structure": ecs["structure"],
        "momentum": ecs["momentum"],
        "trend": ecs["trend"],
        "volume": ecs["volume"],
        "obv": ecs["obv"],
        "market_phase": ecs["market_phase"],
        "reversal_risk": ecs["reversal_risk"],
        "transition_state": tps_d["transition_state"],
        "vwap_reclaim_status": tps_d["vwap_reclaim_status"],
        "ema_reclaim_status": tps_d["ema_reclaim_status"],
        "rsi_transition_status": tps_d["rsi_transition_status"],
        "pullback_quality_score": tps_d["pullback_quality_score"],
        "extension_risk_score": tps_d["extension_risk_score"],
        "bull_score": ecs["bull_score"],
        "bear_score": ecs["bear_score"],
        "structure_pass": ecs.get("structure_pass"),
        "momentum_pass": ecs.get("momentum_pass"),
        "trend_pass": ecs.get("trend_pass"),
        "volume_pass": ecs.get("volume_pass"),
        "alertable": alertable and ext_low,
        **exec_d,
        "computed_at": computed_at.isoformat(),
        "pipeline_stage": "validated" if execution else "discovery",
    }


def shortlist_by_tps(candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Keep top 5–15 by TPS; always include alertable early transitions."""
    if not candidates:
        return []
    ranked = sorted(candidates, key=lambda r: float(r.get("_tps_sort") or 0), reverse=True)
    alert_rows = [r for r in ranked if r.get("_early")]
    core = [r for r in ranked if not r.get("_early")]
    n_core = max(TPS_SHORTLIST_MIN, min(TPS_SHORTLIST_MAX, len(ranked))) - len(alert_rows)
    n_core = max(0, min(n_core, len(core)))
    picked = alert_rows + core[:n_core]
    seen = set()
    out: List[Dict[str, Any]] = []
    for r in picked:
        key = r.get("stock") or r.get("security")
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    if len(out) < TPS_SHORTLIST_MIN:
        for r in ranked:
            key = r.get("stock") or r.get("security")
            if key in seen:
                continue
            seen.add(key)
            out.append(r)
            if len(out) >= TPS_SHORTLIST_MIN:
                break
    return out[: TPS_SHORTLIST_MAX + len(alert_rows)]


def rate_symbol_transition(
    *,
    stock: str,
    fut_sym: str,
    instrument_key: str = "",
    candles_30m: Sequence[Dict[str, Any]],
    candles_1hr: Optional[Sequence[Dict[str, Any]]],
    candles_5m: Optional[Sequence[Dict[str, Any]]] = None,
    computed_at: Optional[datetime] = None,
    run_execution: bool = False,
) -> Optional[Dict[str, Any]]:
    ecs = compute_ecs_rating(candles_30m, candles_1hr)
    if ecs is None:
        return None
    tps = compute_tps(candles_30m, market_phase=ecs.market_phase)
    if tps is None:
        return None

    execution = None
    if run_execution and candles_5m:
        execution = validate_execution_5m(candles_5m, bull_dir=tps.bull_dir)

    early = classify_early_transition(
        tps,
        ecs_trade_type=ecs.trade_type,
        ecs_bull=ecs.bull_score,
        ecs_bear=ecs.bear_score,
        execution=execution,
        require_execution=False,
    )

    ts = computed_at or datetime.now(IST)
    return _build_row(
        stock=stock,
        fut_sym=fut_sym,
        instrument_key=instrument_key,
        ecs_rating=ecs,
        tps=tps,
        early_type=early,
        execution=execution,
        computed_at=ts,
    )


def run_transition_pipeline(
    universe: List[Dict[str, str]],
    fetch_candles,
    *,
    computed_at: Optional[datetime] = None,
) -> List[Dict[str, Any]]:
    """
    Full pipeline: TPS on 30m for all symbols → shortlist → 5m validation.

    fetch_candles(instrument_key, tf_id) -> list of candle dicts
    """
    ts = computed_at or datetime.now(IST)
    discovery: List[Dict[str, Any]] = []

    for item in universe:
        stock = item["stock"]
        fut_sym = item["future_symbol"]
        fut_key = item["instrument_key"]
        try:
            c30 = fetch_candles(fut_key, DISCOVERY_TF)
            c1h = fetch_candles(fut_key, HTF_BIAS_TF)
            c5 = fetch_candles(fut_key, EXECUTION_TF)
            row = rate_symbol_transition(
                stock=stock,
                fut_sym=fut_sym,
                instrument_key=fut_key,
                candles_30m=c30,
                candles_1hr=c1h,
                candles_5m=c5,
                computed_at=ts,
                run_execution=False,
            )
            if row is None:
                continue
            tps_val = float(row.get("tps_score") or 0)
            early = row.get("trade_type", "").startswith("EARLY")
            discovery.append(
                {
                    **row,
                    "_tps_sort": tps_val - float(row.get("extension_risk_score") or 0) * 0.2,
                    "_early": early,
                    "_instrument_key": fut_key,
                }
            )
        except Exception as e:
            logger.debug("vajra_pipeline discovery skip %s: %s", stock, e)

    shortlist = shortlist_by_tps(discovery)
    shortlist_keys = {r.get("_instrument_key") for r in shortlist}

    final_rows: List[Dict[str, Any]] = []

    for sl in shortlist:
        fut_key = sl.get("_instrument_key")
        item = next((u for u in universe if u["instrument_key"] == fut_key), None)
        if not item:
            continue
        stock = item["stock"]
        try:
            c5 = fetch_candles(fut_key, EXECUTION_TF)
            c30 = fetch_candles(fut_key, DISCOVERY_TF)
            c1h = fetch_candles(fut_key, HTF_BIAS_TF)
            row = rate_symbol_transition(
                stock=stock,
                fut_sym=item["future_symbol"],
                instrument_key=fut_key,
                candles_30m=c30,
                candles_1hr=c1h,
                candles_5m=c5,
                computed_at=ts,
                run_execution=True,
            )
            if row:
                final_rows.append(row)
                continue
        except Exception as e:
            logger.debug("vajra_pipeline exec skip %s: %s", stock, e)
        clean = {k: v for k, v in sl.items() if not k.startswith("_")}
        final_rows.append(clean)

    validated_keys = {r.get("stock") for r in final_rows}
    for d in discovery:
        clean = {k: v for k, v in d.items() if not k.startswith("_")}
        if clean.get("stock") in validated_keys:
            continue
        clean["pipeline_stage"] = "discovery"
        clean["execution_validated"] = False
        clean["execution_step"] = "—"
        final_rows.append(clean)

    return sort_vajra_rows(final_rows, discovery_first=True)

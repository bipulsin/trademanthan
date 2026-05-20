"""Qualification engine — raw staging, hysteresis, trigger tags, blockers."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from backend.services.vajra.qualification_config import (
    DEFAULT_QUALIFICATION_CONFIG,
    IgnitionArmedConfig,
    QualificationConfig,
    STATE_ARMED,
    STATE_DISCOVERY,
    STATE_EXECUTABLE,
    STATE_REJECT,
    TierThresholds,
)
from backend.services.vajra.breakout_phase import (
    PHASE_BREAKOUT_INITIATED,
    PHASE_BREAKOUT_VALIDATED,
    PHASE_EXPANSION,
)
from backend.services.vajra.score_layers import ScoreLayers, momentum_improving, structure_improving
from backend.services.vajra.setup_quality import is_ignition_context
from backend.services.vajra.state_persistence import PriorQualificationState, load_prior_state, save_prior_state

PHASE_COMPRESSION = "Compression"
PHASE_ROTATIONAL = "Rotational"


def _price_above_vwap(row: Dict[str, Any]) -> bool:
    v = str(row.get("vwap_reclaim_status") or "").upper()
    return v in ("RECLAIMED", "ABOVE VWAP") or ("ABOVE" in v and "BELOW" not in v)


def _price_below_vwap(row: Dict[str, Any]) -> bool:
    v = str(row.get("vwap_reclaim_status") or "").upper()
    return v == "BELOW VWAP" or ("BELOW" in v and "ABOVE" not in v)


def _f(v: Any) -> float:
    if v is None:
        return 0.0
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


BLOCKER_LABELS = {
    "waiting_breakout": "Awaiting breakout close",
    "needs_vwap_reclaim": "Waiting VWAP reclaim",
    "early_expansion": "Early expansion — confirmation pending",
    "compression_ready": "Compression ready — trigger pending",
    "momentum_confirmation_pending": "Momentum confirmation pending",
    "volume_expansion_pending": "Volume expansion pending",
    "breakout_confirmation_pending": "Breakout confirmation pending",
    "breakout_initiated": "Breakout initiated — expansion ignition",
    "expansion_ignition": "Institutional expansion ignition",
    "conviction_below_threshold": "Conviction below execution threshold",
    "execution_readiness_low": "Execution readiness insufficient",
}


@dataclass
class TradeQualificationResult:
    qualification_state: str
    raw_stage: str
    score_layers: ScoreLayers
    reason_tags: List[str] = field(default_factory=list)
    primary_blocker: Optional[str] = None
    blocker_label: Optional[str] = None
    nearest_trigger: Optional[Dict[str, Any]] = None
    reject_reasons: List[str] = field(default_factory=list)
    hysteresis_applied: bool = False

    def to_row_fields(self) -> Dict[str, Any]:
        d = self.score_layers.to_dict()
        d.update(
            {
                "qualification_state": self.qualification_state,
                "qualification_stage": self.qualification_state.lower(),
                "qualification": self.qualification_state,
                "entry_state": self.qualification_state,
                "raw_stage": self.raw_stage,
                "hysteresis_applied": self.hysteresis_applied,
                "reason_tags": self.reason_tags,
                "qualification_tags": self.reason_tags,
                "primary_blocker": self.primary_blocker,
                "blocker_label": self.blocker_label,
                "nearest_trigger": self.nearest_trigger,
                "reject_reasons": self.reject_reasons,
                "confidence": round(self.score_layers.conviction_score, 1),
                "trade_quality_score": round(self.score_layers.conviction_score, 1),
            }
        )
        return d


def _hard_reject(
    layers: ScoreLayers,
    row: Dict[str, Any],
    reject_reasons: List[str],
    *,
    market_phase: str,
    chop_penalty: float = 0.0,
) -> bool:
    if layers.structure_score < 38 or layers.momentum_score < 32:
        reject_reasons.append("weak_core_scores")
        return True
    if layers.extension_quality_score < 28 and not is_ignition_context(row, layers):
        reject_reasons.append("over_extended")
        return True
    phase = (market_phase or "").upper()
    if phase == PHASE_COMPRESSION and layers.momentum_score < 50 and layers.structure_score < 55:
        reject_reasons.append("compression_chop")
        return True
    if chop_penalty >= 28:
        reject_reasons.append("chop_penalty")
        return True
    tps = layers.tps_score
    if tps > 0 and tps < 52:
        reject_reasons.append("low_tps_discovery")
        if layers.structure_score < 55 or layers.momentum_score < 52:
            return True
    return False


def _evs(row: Dict[str, Any]) -> float:
    return _f(row.get("evs_score"))


def _breakout_phase(row: Dict[str, Any]) -> str:
    return str(row.get("breakout_phase") or row.get("breakout_lifecycle") or "").lower()


def _vwap_accepted(row: Dict[str, Any]) -> bool:
    return _price_above_vwap(row) or _price_below_vwap(row)


def is_ignition_armed(
    layers: ScoreLayers,
    row: Dict[str, Any],
    *,
    cfg: QualificationConfig = DEFAULT_QUALIFICATION_CONFIG,
) -> bool:
    """ARMED on expansion ignition — before mature breakout confirmation."""
    ign = cfg.ignition
    evs = _evs(row)
    phase = _breakout_phase(row)
    if evs < ign.evs_min:
        return False
    if not _vwap_accepted(row):
        return False
    if not row.get("compression_broken") and phase not in (
        PHASE_BREAKOUT_INITIATED,
        PHASE_EXPANSION,
        PHASE_BREAKOUT_VALIDATED,
    ):
        return False
    if layers.structure_score < ign.structure_min:
        return False
    if layers.momentum_score < ign.momentum_min:
        return False
    if layers.extension_quality_score < ign.extension_min:
        return False
    if layers.conviction_score < ign.conviction_min:
        return False
    if layers.execution_score < ign.execution_score_min:
        return False
    tps = layers.tps_score
    if tps > 0 and tps < ign.tps_min:
        return False
    if not (structure_improving(row) or momentum_improving(row) or row.get("adx_accelerating")):
        return False
    return phase in (
        PHASE_BREAKOUT_INITIATED,
        PHASE_EXPANSION,
        PHASE_BREAKOUT_VALIDATED,
    ) or (evs >= ign.evs_min + 5 and bool(row.get("compression_broken")))


def breakout_confirmation_present(
    row: Dict[str, Any],
    layers: ScoreLayers,
    *,
    phase: str,
    cfg: QualificationConfig,
) -> bool:
    prof = cfg.for_phase(phase)
    brk_min = 52.0 * prof.breakout_weight
    if layers.breakout_score < brk_min:
        return False
    if row.get("execution_validated"):
        return True
    phase = _breakout_phase(row)
    evs = _evs(row)
    if phase in (PHASE_BREAKOUT_VALIDATED, PHASE_EXPANSION) and evs >= 50:
        if layers.breakout_score >= brk_min:
            return True
    if phase == PHASE_BREAKOUT_INITIATED and evs >= cfg.ignition.evs_min + 8:
        if layers.breakout_score >= brk_min - 4 and _vwap_accepted(row):
            return True
    if _price_above_vwap(row) or _price_below_vwap(row):
        if layers.breakout_score >= brk_min + 4:
            return True
    return layers.breakout_score >= cfg.hysteresis.breakout_enter_min


def derive_trigger_tags(row: Dict[str, Any], layers: ScoreLayers, market_phase: str) -> List[str]:
    tags: List[str] = []
    brk = layers.breakout_score
    phase = _breakout_phase(row)
    evs = _evs(row)

    if phase == PHASE_BREAKOUT_INITIATED or (
        evs >= 55 and row.get("compression_broken") and _vwap_accepted(row)
    ):
        tags.append("breakout_initiated")
    elif brk < 52 and evs < 50:
        tags.append("waiting_breakout")
    elif brk < 58 and not row.get("execution_validated") and phase not in (
        PHASE_BREAKOUT_INITIATED,
        PHASE_EXPANSION,
    ):
        tags.append("breakout_confirmation_pending")

    if not _vwap_accepted(row):
        tags.append("needs_vwap_reclaim")

    mp = (market_phase or "").lower()
    if phase in (PHASE_BREAKOUT_INITIATED, PHASE_EXPANSION):
        tags.append("expansion_ignition")
    elif "early" in mp and "expansion" in mp:
        tags.append("early_expansion")
    elif market_phase == PHASE_COMPRESSION:
        tags.append("compression_ready")
    elif market_phase == PHASE_ROTATIONAL and "breakout_initiated" not in tags:
        tags.append("early_expansion")

    if layers.momentum_score < 58 and layers.momentum_score >= 50 and "breakout_initiated" not in tags:
        tags.append("momentum_confirmation_pending")
    if layers.volume_score < 60 and layers.tps_score >= 50 and phase != PHASE_BREAKOUT_INITIATED:
        tags.append("volume_expansion_pending")
    return list(dict.fromkeys(tags))


def _meets_tier(layers: ScoreLayers, th: TierThresholds, *, require_discovery: bool = False) -> bool:
    if require_discovery and layers.discovery_score < th.discovery_score_min:
        return False
    if th.structure_min and layers.structure_score < th.structure_min:
        return False
    if th.momentum_min and layers.momentum_score < th.momentum_min:
        return False
    if th.extension_min and layers.extension_quality_score < th.extension_min:
        return False
    if th.tps_min and layers.tps_score > 0 and layers.tps_score < th.tps_min:
        return False
    if th.execution_score_min and layers.execution_score < th.execution_score_min:
        return False
    if th.conviction_min and layers.conviction_score < th.conviction_min:
        return False
    return True


def classify_raw_stage(
    layers: ScoreLayers,
    row: Dict[str, Any],
    *,
    market_phase: str,
    reject_reasons: List[str],
    cfg: QualificationConfig = DEFAULT_QUALIFICATION_CONFIG,
) -> str:
    if _hard_reject(layers, row, reject_reasons, market_phase=market_phase):
        return STATE_REJECT

    exec_th = cfg.executable_thresholds(market_phase)
    if _meets_tier(layers, exec_th) and breakout_confirmation_present(row, layers, phase=market_phase, cfg=cfg):
        return STATE_EXECUTABLE

    if is_ignition_armed(layers, row, cfg=cfg):
        return STATE_ARMED

    armed_th = cfg.armed_thresholds(market_phase)
    tags = derive_trigger_tags(row, layers, market_phase)
    if _meets_tier(layers, armed_th) and tags:
        return STATE_ARMED

    disc_th = cfg.discovery
    tps_ok = layers.tps_score <= 0 or layers.tps_score >= disc_th.tps_min
    if (
        tps_ok
        and layers.discovery_score >= disc_th.discovery_score_min
        and (structure_improving(row) or momentum_improving(row))
    ):
        return STATE_DISCOVERY

    if layers.conviction_score < 42 or (layers.structure_score < 48 and layers.momentum_score < 45):
        return STATE_REJECT
    if layers.discovery_score >= 40:
        return STATE_DISCOVERY
    return STATE_REJECT


def _pick_primary_blocker(tags: List[str], layers: ScoreLayers, th: TierThresholds) -> Optional[str]:
    priority = [
        "waiting_breakout",
        "needs_vwap_reclaim",
        "breakout_initiated",
        "expansion_ignition",
        "breakout_confirmation_pending",
        "momentum_confirmation_pending",
        "volume_expansion_pending",
        "conviction_below_threshold",
        "execution_readiness_low",
        "early_expansion",
        "compression_ready",
    ]
    for p in priority:
        if p in tags:
            return p
    if layers.conviction_score < th.conviction_min:
        return "conviction_below_threshold"
    if layers.execution_score < th.execution_score_min:
        return "execution_readiness_low"
    return tags[0] if tags else None


def _nearest_trigger(row: Dict[str, Any], layers: ScoreLayers, blocker: Optional[str]) -> Optional[Dict[str, Any]]:
    if blocker == "waiting_breakout" or blocker == "breakout_confirmation_pending":
        gap = max(0.0, 58.0 - layers.breakout_score)
        return {
            "type": "breakout_close",
            "distance_pct": round(gap / 10.0, 2),
            "label": f"Breakout score {layers.breakout_score:.0f}/58",
        }
    if blocker == "needs_vwap_reclaim":
        return {"type": "vwap_reclaim", "distance_pct": None, "label": "VWAP reclaim pending"}
    if blocker == "momentum_confirmation_pending":
        gap = max(0.0, 58.0 - layers.momentum_score)
        return {
            "type": "momentum_confirm",
            "distance_pct": round(gap / 10.0, 2),
            "label": f"Momentum {layers.momentum_score:.0f}/58",
        }
    return None


def _holds_executable(layers: ScoreLayers, cfg: QualificationConfig) -> bool:
    h = cfg.hysteresis
    return (
        layers.conviction_score >= h.executable_exit_conviction
        and layers.execution_score >= h.executable_exit_execution
    )


def _holds_armed(layers: ScoreLayers, cfg: QualificationConfig) -> bool:
    h = cfg.hysteresis
    return (
        layers.execution_score >= h.armed_exit_execution
        and layers.conviction_score >= h.armed_exit_conviction
    )


def _holds_discovery(layers: ScoreLayers, cfg: QualificationConfig) -> bool:
    return layers.discovery_score >= cfg.hysteresis.discovery_exit_discovery


def _meets_executable_enter(layers: ScoreLayers, cfg: QualificationConfig, phase: str) -> bool:
    th = cfg.executable_thresholds(phase)
    return _meets_tier(layers, th)


def _meets_armed_enter(layers: ScoreLayers, cfg: QualificationConfig, phase: str) -> bool:
    th = cfg.armed_thresholds(phase)
    return _meets_tier(layers, th)


def apply_hysteresis(
    raw_stage: str,
    layers: ScoreLayers,
    row: Dict[str, Any],
    *,
    market_phase: str,
    prior: Optional[PriorQualificationState],
    cfg: QualificationConfig = DEFAULT_QUALIFICATION_CONFIG,
) -> tuple[str, bool]:
    prev = prior.qualification_state if prior else None
    applied = False

    if raw_stage == STATE_EXECUTABLE and _meets_executable_enter(layers, cfg, market_phase):
        if breakout_confirmation_present(row, layers, phase=market_phase, cfg=cfg):
            return STATE_EXECUTABLE, applied

    if prev == STATE_EXECUTABLE:
        brk_ok = layers.breakout_score >= cfg.hysteresis.breakout_exit_min
        if _holds_executable(layers, cfg) and brk_ok:
            return STATE_EXECUTABLE, True

    if raw_stage == STATE_ARMED or prev == STATE_ARMED:
        if prev == STATE_ARMED and _holds_armed(layers, cfg):
            return STATE_ARMED, True
        if is_ignition_armed(layers, row, cfg=cfg):
            return STATE_ARMED, applied
        if _meets_armed_enter(layers, cfg, market_phase):
            tags = derive_trigger_tags(row, layers, market_phase)
            if tags:
                return STATE_ARMED, applied

    if raw_stage == STATE_DISCOVERY or prev == STATE_DISCOVERY:
        if prev == STATE_DISCOVERY and _holds_discovery(layers, cfg):
            return STATE_DISCOVERY, True
        if raw_stage == STATE_DISCOVERY:
            return STATE_DISCOVERY, applied

    if raw_stage == STATE_REJECT:
        return STATE_REJECT, applied
    return raw_stage, applied


def qualify_trade(
    row: Dict[str, Any],
    layers: ScoreLayers,
    *,
    market_phase: str,
    reject_reasons: Optional[List[str]] = None,
    cfg: QualificationConfig = DEFAULT_QUALIFICATION_CONFIG,
    session_date=None,
) -> TradeQualificationResult:
    reasons = list(reject_reasons or [])
    raw = classify_raw_stage(layers, row, market_phase=market_phase, reject_reasons=reasons, cfg=cfg)
    prior = load_prior_state(row, session_date)
    final, hysteresis = apply_hysteresis(
        raw, layers, row, market_phase=market_phase, prior=prior, cfg=cfg
    )

    tags = derive_trigger_tags(row, layers, market_phase)
    exec_th = cfg.executable_thresholds(market_phase)
    blocker = None
    label = None
    trigger = None

    if final == STATE_ARMED or final == STATE_DISCOVERY:
        blocker = _pick_primary_blocker(tags, layers, exec_th)
        label = BLOCKER_LABELS.get(blocker or "", blocker or "")
        trigger = _nearest_trigger(row, layers, blocker)
    elif final == STATE_EXECUTABLE:
        tags = [t for t in tags if t not in ("waiting_breakout", "needs_vwap_reclaim")]
        if not tags:
            tags = ["execution_ready"]

    result = TradeQualificationResult(
        qualification_state=final,
        raw_stage=raw,
        score_layers=layers,
        reason_tags=tags[:3],
        primary_blocker=blocker,
        blocker_label=label,
        nearest_trigger=trigger,
        reject_reasons=reasons,
        hysteresis_applied=hysteresis,
    )
    save_prior_state({**row, **result.to_row_fields()}, session_date)
    return result

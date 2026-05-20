"""Central qualification thresholds — v2 discovery / armed / executable staging."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict

STATE_DISCOVERY = "DISCOVERY"
STATE_ARMED = "ARMED"
STATE_EXECUTABLE = "EXECUTABLE"
STATE_REJECT = "REJECT"
STATE_WATCHLIST = "WATCHLIST"  # legacy alias — maps ARMED + DISCOVERY

ALL_STATES = frozenset({STATE_DISCOVERY, STATE_ARMED, STATE_EXECUTABLE, STATE_REJECT})
STAGING_STATES = frozenset({STATE_DISCOVERY, STATE_ARMED})


@dataclass(frozen=True)
class TierThresholds:
    structure_min: float = 0.0
    momentum_min: float = 0.0
    extension_min: float = 0.0
    tps_min: float = 0.0
    execution_score_min: float = 0.0
    conviction_min: float = 0.0
    discovery_score_min: float = 0.0


@dataclass(frozen=True)
class HysteresisBands:
    executable_enter_conviction: float = 75.0
    executable_exit_conviction: float = 68.0
    executable_enter_execution: float = 72.0
    executable_exit_execution: float = 62.0
    armed_enter_execution: float = 60.0
    armed_exit_execution: float = 52.0
    armed_enter_conviction: float = 65.0
    armed_exit_conviction: float = 58.0
    discovery_enter_discovery: float = 50.0
    discovery_exit_discovery: float = 42.0
    breakout_enter_min: float = 58.0
    breakout_exit_min: float = 50.0


@dataclass(frozen=True)
class PhaseThresholdProfile:
    executable_conviction: float
    armed_conviction: float
    discovery_conviction: float = 50.0
    breakout_weight: float = 1.0
    executable_execution: float = 72.0
    armed_execution: float = 60.0


DEFAULT_PHASE_PROFILES: Dict[str, PhaseThresholdProfile] = {
    "Trend Continuation": PhaseThresholdProfile(75, 65, breakout_weight=1.0),
    "Rotational": PhaseThresholdProfile(68, 62, breakout_weight=0.85),
    "Compression": PhaseThresholdProfile(65, 60, breakout_weight=0.80),
    "Bull Expansion": PhaseThresholdProfile(78, 68, breakout_weight=1.1),
    "Bear Expansion": PhaseThresholdProfile(78, 68, breakout_weight=1.1),
    "Early Bull Expansion": PhaseThresholdProfile(72, 63, breakout_weight=1.0),
    "Early Bear Expansion": PhaseThresholdProfile(72, 63, breakout_weight=1.0),
    "Weakening": PhaseThresholdProfile(76, 66, breakout_weight=0.9),
}


@dataclass(frozen=True)
class QualificationConfig:
    discovery: TierThresholds = field(
        default_factory=lambda: TierThresholds(
            tps_min=45.0,
            discovery_score_min=50.0,
        )
    )
    armed: TierThresholds = field(
        default_factory=lambda: TierThresholds(
            structure_min=58.0,
            momentum_min=55.0,
            extension_min=45.0,
            tps_min=50.0,
            execution_score_min=60.0,
            conviction_min=65.0,
        )
    )
    executable: TierThresholds = field(
        default_factory=lambda: TierThresholds(
            structure_min=62.0,
            momentum_min=58.0,
            extension_min=45.0,
            tps_min=52.0,
            execution_score_min=72.0,
            conviction_min=75.0,
        )
    )
    hysteresis: HysteresisBands = field(default_factory=HysteresisBands)
    phase_profiles: Dict[str, PhaseThresholdProfile] = field(
        default_factory=lambda: dict(DEFAULT_PHASE_PROFILES)
    )
    section_limits: Dict[str, int] = field(
        default_factory=lambda: {
            STATE_EXECUTABLE: 8,
            STATE_ARMED: 8,
            STATE_DISCOVERY: 8,
        }
    )

    def for_phase(self, phase: str) -> PhaseThresholdProfile:
        return self.phase_profiles.get(phase, self.phase_profiles["Rotational"])

    def executable_thresholds(self, phase: str) -> TierThresholds:
        prof = self.for_phase(phase)
        base = self.executable
        return TierThresholds(
            structure_min=base.structure_min,
            momentum_min=base.momentum_min,
            extension_min=base.extension_min,
            tps_min=base.tps_min,
            execution_score_min=max(base.execution_score_min, prof.executable_execution),
            conviction_min=prof.executable_conviction,
        )

    def armed_thresholds(self, phase: str) -> TierThresholds:
        prof = self.for_phase(phase)
        base = self.armed
        return TierThresholds(
            structure_min=base.structure_min,
            momentum_min=base.momentum_min,
            extension_min=base.extension_min,
            tps_min=base.tps_min,
            execution_score_min=max(base.execution_score_min, prof.armed_execution),
            conviction_min=prof.armed_conviction,
        )


DEFAULT_QUALIFICATION_CONFIG = QualificationConfig()

"""Top screener selection — sectional EXECUTABLE / ARMED / DISCOVERY (no padding)."""
from __future__ import annotations

from typing import Any, Dict, List, Tuple

from backend.services.vajra.qualification_config import (
    STATE_ARMED,
    STATE_DISCOVERY,
    STATE_EXECUTABLE,
    STATE_REJECT,
    STATE_WATCHLIST,
)
from backend.services.vajra.screener_sections import build_screener_sections, select_screener_sections
from backend.services.vajra.trade_state import (
    PHASE_SCORES,
    _phase_bucket_for_top8,
    compute_execution_rank_score,
    resolve_market_phase,
)

__all__ = [
    "PHASE_SCORES",
    "STATE_EXECUTABLE",
    "STATE_ARMED",
    "STATE_DISCOVERY",
    "STATE_WATCHLIST",
    "STATE_REJECT",
    "select_top_picks",
    "select_screener_sections",
    "build_screener_sections",
    "enrich_execution_scores",
    "apply_phase_executable_cap",
]


def apply_phase_executable_cap(*args, **kwargs):
    from backend.services.vajra.trade_state import apply_phase_qualification_cap

    return apply_phase_qualification_cap(*args, **kwargs)


def _qual(row: Dict[str, Any]) -> str:
    return str(row.get("qualification_state") or row.get("qualification") or "").upper()


def select_top_picks(
    rows: List[Dict[str, Any]], n: int = 8
) -> Tuple[List[Dict[str, Any]], Dict[str, List[Dict[str, Any]]]]:
    """EXECUTABLE-only top_picks — ARMED/DISCOVERY in separate sections (no back-fill)."""
    return select_screener_sections(rows, n=n)


def enrich_execution_scores(row: Dict[str, Any]) -> Dict[str, Any]:
    if (
        row.get("execution_rank_score") is not None
        and row.get("market_phase")
        and row.get("top8_phase_bucket") is not None
        and row.get("discovery_score") is not None
    ):
        row["market_context"] = row.get("market_phase")
        return row
    mp = resolve_market_phase(row)
    row["market_phase"] = mp
    row["market_context"] = mp
    row["market_phase_score"] = PHASE_SCORES.get(mp, 0.0)
    row["top8_phase_bucket"] = _phase_bucket_for_top8(mp)

    def _fv(key: str) -> float:
        v = row.get(key)
        try:
            return float(v) if v is not None else 0.0
        except (TypeError, ValueError):
            return 0.0

    ext_q = _fv("extension_quality_score") or max(0.0, 100.0 - _fv("extension_risk_score"))
    qual = _qual(row) or STATE_DISCOVERY
    row["execution_rank_score"] = round(
        compute_execution_rank_score(
            qualification_state=qual,
            market_phase=mp,
            structure_score=_fv("structure_score"),
            momentum_score=_fv("momentum_score"),
            breakout_score=_fv("breakout_score"),
            trend_strength_score=_fv("trend_score"),
            volume_score=_fv("volume_score"),
            pullback_score=_fv("pullback_score"),
            htf_alignment_score=_fv("htf_alignment_score"),
            extension_quality_score=ext_q,
            execution_score=_fv("execution_score"),
            conviction_score=_fv("conviction_score") or _fv("confidence"),
            discovery_score=_fv("discovery_score"),
            risk_efficiency_score=_fv("risk_efficiency_score"),
        ),
        2,
    )
    from backend.services.vajra.trade_state import (
        compute_directional_scores,
        derive_structural_bias,
        directional_confidence_label,
        has_directional_conviction,
        resolve_execution_direction,
    )

    row["structural_bias"] = derive_structural_bias(row)
    row["execution_bias"] = resolve_execution_direction(row, mp, allow_neutral=False)
    row["direction"] = row["execution_bias"]
    ls, ss = compute_directional_scores(row, mp)
    row["directional_long_score"] = round(ls, 2)
    row["directional_short_score"] = round(ss, 2)
    row["directional_confidence"] = directional_confidence_label(row["execution_bias"], ls, ss)
    row["directional_conviction"] = has_directional_conviction(row, mp)
    if row.get("conviction_score") is None and row.get("confidence") is not None:
        row["conviction_score"] = row["confidence"]
    return row

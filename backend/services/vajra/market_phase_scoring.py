"""Top 8 selection — uses canonical phases from trade_state."""
from __future__ import annotations

from typing import Any, Dict, List, Tuple

from backend.services.vajra.trade_state import (
    PHASE_SCORES,
    STATE_EXECUTABLE,
    STATE_REJECT,
    STATE_WATCHLIST,
    TOP8_EXCLUDED_PHASES,
    _phase_bucket_for_top8,
    compute_execution_rank_score,
    resolve_market_phase,
)

__all__ = [
    "PHASE_SCORES",
    "STATE_EXECUTABLE",
    "STATE_WATCHLIST",
    "STATE_REJECT",
    "select_top_picks",
    "enrich_execution_scores",
    "apply_phase_executable_cap",
]


def apply_phase_executable_cap(*args, **kwargs):
    from backend.services.vajra.trade_state import apply_phase_qualification_cap

    return apply_phase_qualification_cap(*args, **kwargs)


def _qual(row: Dict[str, Any]) -> str:
    return str(row.get("qualification_state") or row.get("qualification") or "").upper()


def _rank(row: Dict[str, Any]) -> float:
    sc = row.get("execution_rank_score")
    return float(sc) if sc is not None else 0.0


def _top8_eligible(row: Dict[str, Any]) -> bool:
    if _qual(row) == STATE_REJECT:
        return False
    phase = str(row.get("market_phase") or row.get("market_context") or "")
    if phase in TOP8_EXCLUDED_PHASES:
        return False
    bias = str(row.get("execution_bias") or row.get("direction") or "").upper()
    if bias == "NEUTRAL":
        return False
    if row.get("directional_conviction") is False:
        return False
    from backend.services.vajra.trade_state import has_directional_conviction, resolve_market_phase

    if not row.get("directional_conviction"):
        if not has_directional_conviction(row, resolve_market_phase(row)):
            return False
    return True


def _sort_pool(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(
        rows,
        key=lambda r: (
            -_rank(r),
            -float(r.get("market_phase_score") or 0),
            str(r.get("symbol") or r.get("stock") or ""),
        ),
    )


def select_top_picks(
    rows: List[Dict[str, Any]], n: int = 8
) -> Tuple[List[Dict[str, Any]], Dict[str, List[Dict[str, Any]]]]:
    eligible = [r for r in rows if _top8_eligible(r)]
    executable = _sort_pool([r for r in eligible if _qual(r) == STATE_EXECUTABLE])
    watchlist = _sort_pool([r for r in eligible if _qual(r) == STATE_WATCHLIST])

    def _tier_sorted(pool: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        buckets: Dict[int, List[Dict[str, Any]]] = {1: [], 2: [], 3: []}
        for r in pool:
            b = int(r.get("top8_phase_bucket") or 99)
            if b in buckets:
                buckets[b].append(r)
        out: List[Dict[str, Any]] = []
        for b in (1, 2, 3):
            out.extend(_sort_pool(buckets[b]))
        return out

    ordered: List[Dict[str, Any]] = []
    for r in _tier_sorted(executable):
        if len(ordered) >= n:
            break
        ordered.append(r)
    if len(ordered) < n:
        for r in _tier_sorted(watchlist):
            if len(ordered) >= n:
                break
            ordered.append(r)

    exec_in_top = [r for r in ordered if _qual(r) == STATE_EXECUTABLE]
    watch_in_top = [r for r in ordered if _qual(r) == STATE_WATCHLIST]
    return ordered, {
        STATE_EXECUTABLE: exec_in_top,
        STATE_WATCHLIST: watch_in_top,
    }


def enrich_execution_scores(row: Dict[str, Any]) -> Dict[str, Any]:
    if (
        row.get("execution_rank_score") is not None
        and row.get("market_phase")
        and row.get("top8_phase_bucket") is not None
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
    qual = _qual(row) or STATE_WATCHLIST
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
    return row

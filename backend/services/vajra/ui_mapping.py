"""Screener UI mapping — single qualification state, no contradictory labels."""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from backend.services.vajra.trade_quality import (
    STATE_EXECUTABLE,
    STATE_REJECT,
    STATE_WATCHLIST,
)

_REASON_LABELS = {
    "low_tps_discovery": "Awaiting discovery confirm",
    "compression_chop": "Compression",
    "weak_core_scores": "Weak structure/momentum",
    "over_extended": "Over-extended",
    "awaiting_discovery_confirm": "Discovery unconfirmed",
    "failed_reclaim": "Failed reclaim",
    "weak_volume": "Weak volume",
}


def derive_direction(row: Dict[str, Any]) -> str:
    tt = str(row.get("trade_type") or "").upper()
    if "SHORT" in tt:
        return "SHORT"
    if "LONG" in tt:
        return "LONG"
    bull = float(row.get("bull_score") or 0)
    bear = float(row.get("bear_score") or 0)
    if bull > bear + 3:
        return "LONG"
    if bear > bull + 3:
        return "SHORT"
    return "NEUTRAL"


def normalize_qualification(entry_state: Optional[str]) -> str:
    s = (entry_state or STATE_WATCHLIST).strip().upper()
    if s == STATE_EXECUTABLE or "EXECUTABLE" in s:
        return STATE_EXECUTABLE
    if s == STATE_REJECT or "REJECT" in s or "AVOID" in s:
        return STATE_REJECT
    return STATE_WATCHLIST


def build_qualification_tags(
    qualification: str,
    *,
    reject_reasons: Optional[List[str]] = None,
    structure_score: Optional[float] = None,
    momentum_score: Optional[float] = None,
    breakout_score: Optional[float] = None,
    pullback_score: Optional[float] = None,
    market_phase: str = "",
    tps_score: Optional[float] = None,
) -> List[str]:
    reasons = list(reject_reasons or [])
    tags: List[str] = []
    struct = float(structure_score or 0)
    mom = float(momentum_score or 0)
    brk = float(breakout_score or 0)
    pb = float(pullback_score or 0)
    tps = float(tps_score) if tps_score is not None else 0.0
    phase = (market_phase or "").upper()

    if qualification == STATE_EXECUTABLE:
        if mom >= 58:
            tags.append("Momentum expanding")
        if pb >= 52:
            tags.append("Pullback healthy")
        if brk >= 58:
            tags.append("Breakout confirmed")
        if struct >= 62:
            tags.append("Structure aligned")
        if not tags:
            tags.append("Setup confirmed")
        return tags[:3]

    if qualification == STATE_WATCHLIST:
        if tps > 0 and tps < 52:
            tags.append("Awaiting discovery confirm")
        elif brk < 52:
            tags.append("Waiting breakout")
        if mom < 52 and mom > 0:
            tags.append("Momentum weakening")
        if struct < 55 and struct > 0:
            tags.append("Structure forming")
        if phase in ("ROTATIONAL", "COMPRESSION"):
            tags.append("Market rotational")
        for code in reasons:
            label = _REASON_LABELS.get(code)
            if label and label not in tags:
                tags.append(label)
        if not tags:
            tags.append("Setup forming")
        return tags[:3]

    for code in reasons:
        label = _REASON_LABELS.get(code)
        if label:
            tags.append(label)
    if phase == "COMPRESSION":
        tags.append("Compression")
    if struct < 45 and struct > 0:
        tags.append("Weak structure")
    if not tags:
        tags.append("Low quality setup")
    return tags[:3]


def finalize_screener_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Canonical fields for UI — qualification is the only primary state."""
    qual = normalize_qualification(row.get("entry_state") or row.get("qualification"))
    row["qualification"] = qual
    row["entry_state"] = qual
    row["trade_quality_state"] = qual

    row["direction"] = derive_direction(row)
    ees = row.get("ees_score")
    if ees is not None:
        row["setup_potential_score"] = ees

    tags = build_qualification_tags(
        qual,
        reject_reasons=row.get("reject_reasons"),
        structure_score=row.get("structure_score"),
        momentum_score=row.get("momentum_score"),
        breakout_score=row.get("breakout_score"),
        pullback_score=row.get("pullback_score"),
        market_phase=str(row.get("market_phase") or ""),
        tps_score=row.get("tps_score"),
    )
    row["qualification_tags"] = tags

    conf = row.get("confidence")
    row["market_context"] = str(row.get("market_phase") or "—").replace("_", " ").title() or "—"
    tq = row.get("trade_quality_score")
    row["setup_quality_score"] = tq if tq is not None else conf

    action = str(row.get("enter_action") or "").upper()
    if qual == STATE_REJECT:
        row["enter_action"] = ""
        row["enter_enabled"] = False
    elif qual == STATE_EXECUTABLE:
        row["enter_action"] = "ENTER"
        row["enter_enabled"] = True
    else:
        row["enter_action"] = "WATCH"
        row["enter_enabled"] = False

    if qual == STATE_EXECUTABLE and not row.get("enter_reason"):
        row["enter_reason"] = "Executable now — structure and momentum aligned"
    elif qual == STATE_WATCHLIST and not row.get("enter_reason"):
        row["enter_reason"] = "Watchlist — awaiting full confirmation"
    elif qual == STATE_REJECT and not row.get("enter_reason"):
        row["enter_reason"] = "Not qualified for execution"

    # Legacy transition label kept for advanced panel only (not main columns).
    row["lifecycle_hint"] = row.get("trade_type") or "—"
    if action == "REJECT" and qual != STATE_REJECT:
        pass  # action already normalized above
    return row


def finalize_screener_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [finalize_screener_row(dict(r)) for r in rows]

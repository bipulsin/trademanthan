"""Trade Ticket actions — delegates to Phase 3 lifecycle (PAPER only)."""
from __future__ import annotations

from typing import Any, Dict, Optional

from backend.services.tarang.lifecycle import dismiss_candidate, mark_taken_manual, take_trade_paper
from backend.services.tarang.screener import get_candidate


def build_ticket(candidate_id: int) -> Dict[str, Any]:
    cand = get_candidate(candidate_id)
    if not cand:
        return {"ok": False, "error": "candidate_not_found"}
    p = cand.get("payload") or {}
    legs = p.get("legs") or []
    return {
        "ok": True,
        "product": "Kosmic Tarang",
        "mode": "PAPER",
        "auto": False,
        "candidate_id": candidate_id,
        "profile_id": cand["profile_id"],
        "structure": cand.get("structure") or p.get("structure"),
        "status": p.get("status") or cand.get("status"),
        "legs": legs,
        "net_credit": p.get("net_credit"),
        "width": p.get("width"),
        "max_loss_per_unit_inr": p.get("max_loss_per_unit_inr"),
        "lots_or_contracts": p.get("lots_or_contracts"),
        "candidate_risk_inr": p.get("candidate_risk_inr"),
        "max_profit_approx_inr": p.get("max_profit_approx_inr"),
        "exit_levels": p.get("exit_levels"),
        "gates": p.get("gates"),
        "qualified_reasons": p.get("qualified_reasons"),
        "failed_gates": p.get("failed_gates"),
        "expiry": p.get("expiry"),
        "underlying": p.get("underlying"),
        "venue": p.get("venue"),
        "iv_percentile": p.get("iv_percentile"),
        "fees_frac_of_credit": p.get("fees_frac_of_credit"),
        "round_trip_fees_inr": p.get("round_trip_fees_inr"),
        "gross_credit_inr": p.get("gross_credit_inr"),
        "net_credit_per_contract_inr": p.get("net_credit_per_contract_inr"),
        "max_contracts_per_order": p.get("max_contracts_per_order"),
        "max_contracts_per_trade": p.get("max_contracts_per_trade"),
        "note": "Phase 3: Take trade simulates PAPER fills via PaperBroker — no live broker orders. Fees are round-trip estimates vs gross credit.",
    }


__all__ = ["build_ticket", "take_trade_paper", "dismiss_candidate", "mark_taken_manual"]

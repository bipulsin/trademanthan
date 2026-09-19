"""Min max-loss per lot/contract table for data-health UI."""
from __future__ import annotations

from typing import Any, Dict, List

from backend.services.tarang.config import crypto_budget, energy_budget, get_profiles


# From Phase 0 sizing spike (full contracts / Delta contract_value)
_STATIC_ROWS: List[Dict[str, Any]] = [
    {
        "underlying": "CRUDEOIL",
        "profile_id": "CL",
        "venue": "upstox_mcx",
        "lot_or_cv": 100,
        "strike_step": 50,
        "width_label": "1 step",
        "width_pts": 50,
        "max_loss_zero_credit": 5000,
        "currency": "INR",
    },
    {
        "underlying": "CRUDEOIL",
        "profile_id": "CL",
        "venue": "upstox_mcx",
        "lot_or_cv": 100,
        "strike_step": 50,
        "width_label": "profile min 5",
        "width_pts": 250,
        "max_loss_zero_credit": 25000,
        "currency": "INR",
    },
    {
        "underlying": "CRUDEOIL",
        "profile_id": "CL",
        "venue": "upstox_mcx",
        "lot_or_cv": 100,
        "strike_step": 50,
        "width_label": "profile max 8",
        "width_pts": 400,
        "max_loss_zero_credit": 40000,
        "currency": "INR",
    },
    {
        "underlying": "NATURALGAS",
        "profile_id": "NG",
        "venue": "upstox_mcx",
        "lot_or_cv": 1250,
        "strike_step": 5,
        "width_label": "1 step",
        "width_pts": 5,
        "max_loss_zero_credit": 6250,
        "currency": "INR",
    },
    {
        "underlying": "NATURALGAS",
        "profile_id": "NG",
        "venue": "upstox_mcx",
        "lot_or_cv": 1250,
        "strike_step": 5,
        "width_label": "profile min 3",
        "width_pts": 15,
        "max_loss_zero_credit": 18750,
        "currency": "INR",
    },
    {
        "underlying": "NATURALGAS",
        "profile_id": "NG",
        "venue": "upstox_mcx",
        "lot_or_cv": 1250,
        "strike_step": 5,
        "width_label": "profile max 5",
        "width_pts": 25,
        "max_loss_zero_credit": 31250,
        "currency": "INR",
    },
    {
        "underlying": "BTC",
        "profile_id": "BTC",
        "venue": "delta_india",
        "lot_or_cv": 0.001,
        "strike_step": 200,
        "width_label": "profile 2–4 steps",
        "width_pts": 800,
        "max_loss_zero_credit_usd": 0.80,
        "currency": "USD",
    },
    {
        "underlying": "ETH",
        "profile_id": "ETH",
        "venue": "delta_india",
        "lot_or_cv": 0.01,
        "strike_step": 10,
        "width_label": "profile 2–4 steps",
        "width_pts": 40,
        "max_loss_zero_credit_usd": 0.40,
        "currency": "USD",
    },
]


def min_max_loss_table() -> Dict[str, Any]:
    e = energy_budget()
    c = crypto_budget()
    energy_budget_inr = float(e.get("per_trade_budget_inr") or 0)
    crypto_budget_inr = float(c.get("per_trade_budget_inr") or 0)
    rows = []
    for r in _STATIC_ROWS:
        row = dict(r)
        if row["currency"] == "INR":
            ml = float(row["max_loss_zero_credit"])
            row["fits_energy_budget"] = ml <= energy_budget_inr
            row["budget_ref_inr"] = energy_budget_inr
        else:
            # rough USD→INR ~83 for fit flag vs crypto INR budget
            usd = float(row.get("max_loss_zero_credit_usd") or 0)
            row["fits_crypto_budget"] = (usd * 83) <= crypto_budget_inr
            row["budget_ref_inr"] = crypto_budget_inr
        rows.append(row)
    return {
        "energy_budget": e,
        "crypto_budget": c,
        "contract_family": (get_profiles().get("contractFamily") or "full"),
        "rows": rows,
        "source_doc": "docs/tarang-sizing-min-max-loss.md",
    }

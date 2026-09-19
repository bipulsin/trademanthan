"""Min max-loss per lot/contract table for data-health UI — mini and full."""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from backend.services.tarang.config import crypto_budget, energy_budget, get_profiles

# Fallbacks when the Upstox master is unavailable (unit tests / token down).
_FALLBACK_SPECS: Dict[str, Dict[str, Any]] = {
    "CRUDEOILM": {"lot": 10, "step": 50, "family": "mini", "profile_id": "CL", "widths": (("1 step", 1), ("profile min 5", 5), ("profile max 8", 8))},
    "CRUDEOIL": {"lot": 100, "step": 50, "family": "full", "profile_id": "CL", "widths": (("1 step", 1), ("profile min 5", 5), ("profile max 8", 8))},
    "NATGASMINI": {"lot": 250, "step": 5, "family": "mini", "profile_id": "NG", "widths": (("1 step", 1), ("profile min 3", 3), ("profile max 5", 5))},
    "NATURALGAS": {"lot": 1250, "step": 5, "family": "full", "profile_id": "NG", "widths": (("1 step", 1), ("profile min 3", 3), ("profile max 5", 5))},
}

_CRYPTO_ROWS: List[Dict[str, Any]] = [
    {
        "underlying": "BTC",
        "profile_id": "BTC",
        "venue": "delta_india",
        "contract_family": "n/a",
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
        "contract_family": "n/a",
        "lot_or_cv": 0.01,
        "strike_step": 10,
        "width_label": "profile 2–4 steps",
        "width_pts": 40,
        "max_loss_zero_credit_usd": 0.40,
        "currency": "USD",
    },
]


def _master_spec(underlying: str) -> Tuple[Optional[int], Optional[float], str]:
    import os

    if os.getenv("PYTEST_CURRENT_TEST"):
        fb = _FALLBACK_SPECS.get(underlying) or {}
        lot, step = fb.get("lot"), fb.get("step")
        return (int(lot) if lot else None, float(step) if step else None, "fallback")
    try:
        from backend.services.tarang.adapters.upstox_mcx import UpstoxMcxAdapter

        spec = UpstoxMcxAdapter().contract_spec(underlying)
        lot = spec.get("lot_size")
        step = spec.get("strike_step")
        if lot and step:
            return int(lot), float(step), "master"
    except Exception:
        pass
    fb = _FALLBACK_SPECS.get(underlying) or {}
    lot = fb.get("lot")
    step = fb.get("step")
    return (int(lot) if lot else None, float(step) if step else None, "fallback")


def min_max_loss_table() -> Dict[str, Any]:
    e = energy_budget()
    c = crypto_budget()
    energy_budget_inr = float(e.get("per_trade_budget_inr") or 0)
    energy_hard_inr = float(e.get("hard_cap_inr") or 0)
    crypto_budget_inr = float(c.get("per_trade_budget_inr") or 0)
    rows: List[Dict[str, Any]] = []
    cannot_fit: List[Dict[str, Any]] = []
    for us, fb in _FALLBACK_SPECS.items():
        lot, step, src = _master_spec(us)
        if not lot or not step:
            continue
        for label, steps in fb["widths"]:
            width_pts = steps * step
            ml = width_pts * lot
            fits_default = ml <= energy_budget_inr
            fits_hard = ml <= energy_hard_inr
            row = {
                "underlying": us,
                "profile_id": fb["profile_id"],
                "venue": "upstox_mcx",
                "contract_family": fb["family"],
                "lot_or_cv": lot,
                "strike_step": step,
                "width_label": label,
                "width_steps": steps,
                "width_pts": width_pts,
                "max_loss_zero_credit": ml,
                "currency": "INR",
                "lot_source": src,
                "fits_energy_budget": fits_default,
                "fits_hard_cap": fits_hard,
                "budget_ref_inr": energy_budget_inr,
                "hard_cap_inr": energy_hard_inr,
            }
            rows.append(row)
            if fb["family"] == "mini" and not fits_hard:
                cannot_fit.append(
                    {
                        "underlying": us,
                        "width_label": label,
                        "max_loss_zero_credit": ml,
                        "hard_cap_inr": energy_hard_inr,
                        "action": "do_not_raise_budget",
                    }
                )
    for r in _CRYPTO_ROWS:
        row = dict(r)
        usd = float(row.get("max_loss_zero_credit_usd") or 0)
        row["fits_crypto_budget"] = (usd * 83) <= crypto_budget_inr
        row["budget_ref_inr"] = crypto_budget_inr
        rows.append(row)
    return {
        "energy_budget": e,
        "crypto_budget": c,
        "contract_family": (get_profiles().get("contractFamily") or "mini"),
        "rows": rows,
        "mini_cannot_fit_hard_cap": cannot_fit,
        "source_doc": "docs/tarang-sizing-min-max-loss.md",
        "note": (
            "Mini lots from Upstox master when available. "
            "If a mini width cannot fit the ₹10,000 hard cap, report it — do not raise the budget."
        ),
    }

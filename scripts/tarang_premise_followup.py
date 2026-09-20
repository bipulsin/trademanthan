#!/usr/bin/env python3
"""Follow-up premise analysis. Live rules unchanged."""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.services.tarang.premise_followup import run_followup  # noqa: E402


def _slim(out: dict) -> dict:
    structs = []
    for s in out.get("structures") or []:
        one = s.get("one_entry_per_cycle") or {}
        all_d = s.get("all_days_mean_within_cycle") or {}
        structs.append(
            {
                "id": s.get("id"),
                "taken_days": s.get("taken_days"),
                "skip_budget_5000": s.get("skip_budget_5000"),
                "skip_hard_10000_zero_credit": s.get("skip_hard_10000_zero_credit"),
                "skip_live_floor_units": s.get("skip_live_floor_units"),
                "lots_used_mean": s.get("lots_used_mean"),
                "max_loss_per_lot_mean": s.get("max_loss_per_lot_mean"),
                "one_entry_gross": one.get("gross"),
                "one_entry_10pct_hold": (one.get("uniform") or {}).get("10pct", {}).get("hold"),
                "one_entry_calibrated_hold": (one.get("calibrated") or {}).get("hold"),
                "all_days_gross": all_d.get("gross"),
                "all_days_10pct_hold": (all_d.get("uniform") or {}).get("10pct", {}).get("hold"),
                "all_days_calibrated_hold": (all_d.get("calibrated") or {}).get("hold"),
            }
        )
    return {
        "followup_registered_sha": out.get("followup_registered_sha"),
        "expired_cycles_per_commodity": out.get("expired_cycles_per_commodity"),
        "calibrated_schedule": out.get("calibrated_schedule"),
        "cap_integrity": out.get("cap_integrity"),
        "structures": structs,
        "iv_rv_quartiles_exploratory": out.get("iv_rv_quartiles_exploratory"),
        "delta": {
            k: (out.get("delta") or {}).get(k)
            for k in (
                "expired_snapshot_cycles",
                "open_snapshot_cycles",
                "first_expected_expiry",
                "expired_forward_test_cycles",
                "open_expiries",
                "note",
            )
        },
        "delta_fees": (out.get("delta") or {}).get("fees"),
        "parity": out.get("parity"),
        "ng_and_2025_futcom": out.get("ng_and_2025_futcom"),
        "datewise": out.get("datewise"),
        "kill_criteria": out.get("kill_criteria"),
        "asof_ist": out.get("asof_ist"),
        "generated_at": out.get("generated_at"),
    }


def main() -> int:
    out = run_followup()
    print(json.dumps(_slim(out), indent=2, default=str))
    return 1 if out.get("error") else 0


if __name__ == "__main__":
    raise SystemExit(main())

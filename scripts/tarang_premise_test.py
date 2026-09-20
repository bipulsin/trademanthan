#!/usr/bin/env python3
"""Run the locked Tarang premise test. Does not change live rules."""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.services.tarang.premise_test import run_premise_test  # noqa: E402


def main() -> int:
    out = run_premise_test()
    slim = {
        "registered_sha": out.get("registered_sha"),
        "status": out.get("status"),
        "error": out.get("error"),
        "plain_language": out.get("plain_language"),
        "metric": out.get("metric"),
        "iv_vs_rv": out.get("iv_vs_rv"),
        "supported_ids": out.get("supported_ids"),
        "decision_inputs": out.get("decision_inputs"),
        "delta_premise": out.get("delta_premise"),
        "live_defaults_unchanged": out.get("live_defaults_unchanged"),
        "max_loss_specs": out.get("max_loss_specs"),
        "structures": out.get("structures") or [],
        "asof_ist": out.get("asof_ist"),
        "generated_at": out.get("generated_at"),
    }
    print(json.dumps(slim, indent=2, default=str))
    return 1 if out.get("error") else 0


if __name__ == "__main__":
    raise SystemExit(main())

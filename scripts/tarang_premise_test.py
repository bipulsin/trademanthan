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
    print(json.dumps(out, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
